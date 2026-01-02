//! Infrastructure adapter for extracting and streaming data from Oracle to CSV or Parquet.

use crate::domain::error_definitions::{ExportError, Result};
use crate::domain::export_models::{ExportTask, FileFormat, TaskResult};
use crate::infrastructure::oracle::oracle_metadata_adapter::{
    get_virtual_columns_map, SQL_GET_COLUMNS,
};
use crate::infrastructure::oracle::sql_utils::{build_hash_from_parts, get_hash_expr_from_str};
use crate::ports::data_streamer::DataStreamer;
use base64::{engine::general_purpose, Engine as _};
use csv::{QuoteStyle, WriterBuilder};
use flate2::write::GzEncoder;
use flate2::Compression as GzipCompression;
use oracle::{sql_type::OracleType, sql_type::Timestamp, Connection};
use std::fs::File;
use std::io::BufWriter;
use std::time::Instant;

use arrow_array::{ArrayRef, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression as ParquetCompression;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;

/// Concrete implementation of `DataStreamer` for Oracle databases.
///
/// This adapter handles the heavy lifting of executing SELECT statements,
/// formatting diverse Oracle types into strings (Base64 for BLOBs, WKT for Spatial),
/// and streaming the results into Gzip-compressed CSV files.
pub struct OracleExtractionAdapter {
    conn_str: String,
    user: String,
    pass: String,
    prefetch_rows: u32,
    delimiter: u8,
}

impl OracleExtractionAdapter {
    /// Creates a new OracleExtractionAdapter with connection and performance settings.
    pub fn new(
        conn_str: String,
        user: String,
        pass: String,
        prefetch_rows: u32,
        delimiter: u8,
    ) -> Self {
        Self {
            conn_str,
            user,
            pass,
            prefetch_rows,
            delimiter,
        }
    }

    /// Establishes a fresh connection to the Oracle database.
    fn get_conn(&self) -> Result<Connection> {
        Connection::connect(&self.user, &self.pass, &self.conn_str)
            .map_err(ExportError::OracleError)
    }
}

impl DataStreamer for OracleExtractionAdapter {
    fn export_task(&self, task: ExportTask) -> Result<TaskResult> {
        match task.file_format {
            FileFormat::Csv => self.export_csv(task),
            FileFormat::Parquet => self.export_parquet(task),
        }
    }
}

impl OracleExtractionAdapter {
    fn export_csv(&self, task: ExportTask) -> Result<TaskResult> {
        let start_time = Instant::now();
        let conn = self.get_conn()?;

        // 1. Prepare Query
        let (sql, _raw_col_names) = self.prepare_query(&conn, &task)?;

        // 2. Setup Statement
        let mut stmt = conn
            .statement(&sql)
            .prefetch_rows(self.prefetch_rows)
            .build()
            .map_err(ExportError::OracleError)?;

        let rows = stmt.query(&[]).map_err(ExportError::OracleError)?;
        let col_infos = rows.column_info();
        let col_types: Vec<OracleType> =
            col_infos.iter().map(|c| c.oracle_type().clone()).collect();
        let col_names: Vec<String> = col_infos.iter().map(|c| c.name().to_string()).collect();

        // 3. Open Output
        let file = File::create(&task.output_file).map_err(ExportError::IoError)?;
        let buf_writer = BufWriter::with_capacity(128 * 1024, file);
        let encoder = GzEncoder::new(buf_writer, GzipCompression::fast());

        let mut wtr = WriterBuilder::new()
            .delimiter(self.delimiter)
            .quote_style(QuoteStyle::NonNumeric)
            .from_writer(encoder);

        wtr.write_record(&col_names)
            .map_err(|e| ExportError::ArtifactError(e.to_string()))?;

        // 4. Stream Results
        let mut count = 0;
        let mut bytes: u64 = 0;

        for row_res in rows {
            let row = row_res.map_err(ExportError::OracleError)?;
            let mut record = Vec::with_capacity(col_names.len());

            for (i, otype) in col_types.iter().enumerate() {
                let val_str = self.format_value(&row, i, otype)?;
                bytes += val_str.len() as u64;
                record.push(val_str);
            }
            wtr.write_record(&record)
                .map_err(|e| ExportError::ArtifactError(e.to_string()))?;
            count += 1;
        }

        wtr.flush()
            .map_err(|e| ExportError::ArtifactError(e.to_string()))?;

        Ok(TaskResult::success(
            task.schema,
            task.table,
            count,
            bytes,
            start_time.elapsed().as_secs_f64(),
            task.chunk_id,
        ))
    }

    fn export_parquet(&self, task: ExportTask) -> Result<TaskResult> {
        let start_time = Instant::now();
        let conn = self.get_conn()?;

        // 1. Prepare Query
        let (sql, _raw_col_names) = self.prepare_query(&conn, &task)?;

        // 2. Setup Statement
        let mut stmt = conn
            .statement(&sql)
            .prefetch_rows(self.prefetch_rows)
            .build()
            .map_err(ExportError::OracleError)?;

        let rows = stmt.query(&[]).map_err(ExportError::OracleError)?;
        let col_infos = rows.column_info();
        let col_types: Vec<OracleType> =
            col_infos.iter().map(|c| c.oracle_type().clone()).collect();
        let col_names: Vec<String> = col_infos.iter().map(|c| c.name().to_string()).collect();

        // 3. Setup Arrow Schema
        let fields: Vec<Field> = col_names
            .iter()
            .map(|name| Field::new(name, DataType::Utf8, true))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        // 4. Setup Parquet Writer
        let file = File::create(&task.output_file).map_err(ExportError::IoError)?;
        let props = WriterProperties::builder()
            .set_compression(self.map_parquet_compression(&task.parquet_compression))
            .build();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
            .map_err(|e| ExportError::ArtifactError(e.to_string()))?;

        // 5. Stream & Batch Results
        let mut count = 0;
        let mut bytes: u64 = 0;
        let batch_size = 10000;
        let mut columns_data: Vec<Vec<Option<String>>> =
            vec![Vec::with_capacity(batch_size); col_names.len()];

        for row_res in rows {
            let row = row_res.map_err(ExportError::OracleError)?;

            for i in 0..col_names.len() {
                let val_str = self.format_value(&row, i, &col_types[i])?;
                bytes += val_str.len() as u64;
                columns_data[i].push(Some(val_str));
            }
            count += 1;

            if count % batch_size as u64 == 0 {
                self.write_batch(&mut writer, &schema, &mut columns_data)?;
            }
        }

        // Write final batch
        if !columns_data[0].is_empty() {
            self.write_batch(&mut writer, &schema, &mut columns_data)?;
        }

        writer
            .close()
            .map_err(|e| ExportError::ArtifactError(e.to_string()))?;

        Ok(TaskResult::success(
            task.schema,
            task.table,
            count,
            bytes,
            start_time.elapsed().as_secs_f64(),
            task.chunk_id,
        ))
    }

    fn write_batch(
        &self,
        writer: &mut ArrowWriter<File>,
        schema: &Arc<Schema>,
        columns_data: &mut Vec<Vec<Option<String>>>,
    ) -> Result<()> {
        let arrays: Vec<ArrayRef> = columns_data
            .iter()
            .map(|data| Arc::new(StringArray::from(data.clone())) as ArrayRef)
            .collect();

        let batch = arrow_array::RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| ExportError::ArtifactError(e.to_string()))?;

        writer
            .write(&batch)
            .map_err(|e| ExportError::ArtifactError(e.to_string()))?;

        // Clear buffers
        for col in columns_data {
            col.clear();
        }

        Ok(())
    }

    fn map_parquet_compression(&self, c: &Option<String>) -> ParquetCompression {
        match c.as_deref().unwrap_or("zstd").to_lowercase().as_str() {
            "snappy" => ParquetCompression::SNAPPY,
            "gzip" => ParquetCompression::GZIP(Default::default()),
            "lzo" => ParquetCompression::LZO,
            "brotli" => ParquetCompression::BROTLI(Default::default()),
            "lz4" => ParquetCompression::LZ4,
            "zstd" => ParquetCompression::ZSTD(Default::default()),
            "none" => ParquetCompression::UNCOMPRESSED,
            _ => ParquetCompression::ZSTD(Default::default()),
        }
    }
}

impl OracleExtractionAdapter {
    fn prepare_query(&self, conn: &Connection, task: &ExportTask) -> Result<(String, Vec<String>)> {
        let virtual_map = get_virtual_columns_map(conn, &task.schema, &task.table);

        let mut stmt_cols = conn
            .statement(SQL_GET_COLUMNS)
            .build()
            .map_err(ExportError::OracleError)?;
        let rows_cols = stmt_cols
            .query(&[&task.table.to_uppercase(), &task.schema.to_uppercase()])
            .map_err(ExportError::OracleError)?;

        let mut select_parts = Vec::new();
        let mut raw_names = Vec::new();
        let mut hash_parts = Vec::new();

        for row_res in rows_cols {
            let row = row_res.map_err(ExportError::OracleError)?;
            let name: String = row.get(0).map_err(ExportError::OracleError)?;
            let data_type: String = row.get(1).map_err(ExportError::OracleError)?;
            let is_virtual: String = row.get(3).map_err(ExportError::OracleError)?;
            let is_hidden: String = row.get(4).map_err(ExportError::OracleError)?;
            let is_user_gen: String = row.get(6).map_err(ExportError::OracleError)?;

            let name_up = name.to_uppercase();

            if is_hidden == "YES" && is_user_gen == "NO" {
                continue;
            }
            if is_virtual == "YES" && virtual_map.contains_key(&name_up) {
                continue;
            }

            raw_names.push(name.clone());

            let upper_type = data_type.to_uppercase();
            let raw_expr = if upper_type.contains("TIME ZONE") {
                format!(
                    "TO_CHAR(SYS_EXTRACT_UTC(\"{}\"), 'YYYY-MM-DD\"T\"HH24:MI:SS.FF6\"Z\"')",
                    name
                )
            } else if upper_type == "XMLTYPE" {
                format!(
                    "REPLACE(REPLACE(sys.XMLType.getStringVal(\"{}\"), CHR(10), ''), CHR(13), '')",
                    name
                )
            } else if upper_type == "JSON" {
                format!(
                    "REPLACE(REPLACE(JSON_SERIALIZE(\"{}\"), CHR(10), ''), CHR(13), '')",
                    name
                )
            } else if upper_type == "BOOLEAN" {
                format!("CASE WHEN \"{}\" THEN 'true' ELSE 'false' END", name)
            } else if upper_type.contains("SDO_GEOMETRY") {
                format!("SDO_UTIL.TO_WKTGEOMETRY(\"{}\")", name)
            } else if upper_type.contains("UROWID") || upper_type.contains("ROWID") {
                format!("ROWIDTOCHAR(\"{}\")", name)
            } else {
                format!("\"{}\"", name)
            };

            select_parts.push(format!("{} AS \"{}\"", raw_expr, name));

            if task.enable_row_hash && !task.use_client_hash {
                if let Some(h) = get_hash_expr_from_str(&name, &data_type) {
                    hash_parts.push(h);
                }
            }
        }

        if task.enable_row_hash && !task.use_client_hash && !hash_parts.is_empty() {
            let final_expr = build_hash_from_parts(&hash_parts);
            select_parts.push(format!("{} AS \"ROW_HASH\"", final_expr));
        }

        let mut sql = format!(
            "SELECT {} FROM \"{}\".\"{}\"",
            select_parts.join(", "),
            task.schema,
            task.table
        );
        if let Some(w) = &task.query_where {
            sql.push_str(" WHERE ");
            sql.push_str(w);
        } else {
            sql.push_str(" ORDER BY ROWID");
        }

        Ok((sql, raw_names))
    }

    fn format_value(&self, row: &oracle::Row, i: usize, otype: &OracleType) -> Result<String> {
        match otype {
            OracleType::Number(_, _)
            | OracleType::Int64
            | OracleType::Float(_)
            | OracleType::BinaryFloat
            | OracleType::BinaryDouble => {
                let v: Option<String> = row.get(i).map_err(ExportError::OracleError)?;
                Ok(v.unwrap_or_default())
            }
            OracleType::Date
            | OracleType::Timestamp(_)
            | OracleType::TimestampTZ(_)
            | OracleType::TimestampLTZ(_) => {
                let v: Option<Timestamp> = row.get(i).map_err(ExportError::OracleError)?;
                Ok(v.map(|ts| self.format_timestamp(&ts)).unwrap_or_default())
            }
            OracleType::Raw(_) | OracleType::BLOB => {
                let v: Option<Vec<u8>> = row.get(i).map_err(ExportError::OracleError)?;
                Ok(v.map(|b| general_purpose::STANDARD.encode(b))
                    .unwrap_or_default())
            }
            _ => {
                let v: Option<String> = row.get(i).map_err(ExportError::OracleError)?;
                Ok(v.unwrap_or_default())
            }
        }
    }

    fn format_timestamp(&self, ts: &Timestamp) -> String {
        format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
            ts.year(),
            ts.month(),
            ts.day(),
            ts.hour(),
            ts.minute(),
            ts.second(),
            ts.nanosecond() / 1000
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::basic::Compression as ParquetCompression;

    #[test]
    fn test_map_parquet_compression() {
        let adapter = OracleExtractionAdapter::new(
            "conn".into(),
            "user".into(),
            "pass".into(),
            5000,
            b'\x10',
        );

        assert_eq!(
            adapter.map_parquet_compression(&None),
            ParquetCompression::ZSTD(Default::default())
        );
        assert_eq!(
            adapter.map_parquet_compression(&Some("snappy".to_string())),
            ParquetCompression::SNAPPY
        );
        assert_eq!(
            adapter.map_parquet_compression(&Some("gzip".to_string())),
            ParquetCompression::GZIP(Default::default())
        );
        assert_eq!(
            adapter.map_parquet_compression(&Some("none".to_string())),
            ParquetCompression::UNCOMPRESSED
        );
        assert_eq!(
            adapter.map_parquet_compression(&Some("invalid".to_string())),
            ParquetCompression::ZSTD(Default::default())
        );
    }

    #[test]
    fn test_format_timestamp() {
        let adapter = OracleExtractionAdapter::new(
            "conn".into(),
            "user".into(),
            "pass".into(),
            5000,
            b'\x10',
        );

        let ts = Timestamp::new(2023, 10, 27, 14, 30, 45, 123456000).unwrap();
        assert_eq!(adapter.format_timestamp(&ts), "2023-10-27 14:30:45.123456");

        let ts2 = Timestamp::new(2023, 1, 1, 0, 0, 0, 0).unwrap();
        assert_eq!(adapter.format_timestamp(&ts2), "2023-01-01 00:00:00.000000");
    }
}
