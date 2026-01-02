//! Infrastructure adapter for extracting and streaming data from Oracle to CSV or Parquet.

use crate::domain::mapping;
use crate::domain::errors::{ExportError, Result};
use crate::domain::entities::{ExportTask, FileFormat, TaskResult};
use crate::infrastructure::oracle::metadata::{
    get_virtual_columns_map, SQL_GET_COLUMNS,
};
use crate::infrastructure::oracle::sql_utils::{build_hash_from_parts, get_hash_expr_from_str};
use crate::ports::extraction_port::ExtractionPort;
use base64::{engine::general_purpose, Engine as _};
use csv::{QuoteStyle, WriterBuilder};
use flate2::write::GzEncoder;
use flate2::Compression as GzipCompression;
use oracle::{sql_type::OracleType, sql_type::Timestamp, Connection};
use r2d2::Pool;
use crate::infrastructure::oracle::connection_manager::OracleConnectionManager;
use std::fs::File;
use std::io::BufWriter;
use std::time::Instant;

use arrow_array::ArrayRef;
use arrow_array::builder::*;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression as ParquetCompression;
use parquet::file::properties::WriterProperties;
use std::sync::Arc;

/// Concrete implementation of `ExtractionPort` for Oracle databases.
///
/// This adapter handles the heavy lifting of executing SELECT statements,
/// formatting diverse Oracle types into strings (Base64 for BLOBs, WKT for Spatial),
/// and streaming the results into Gzip-compressed CSV files.
/// Concrete implementation of `ExtractionPort` for Oracle databases.
///
/// This adapter handles the heavy lifting of executing SELECT statements,
/// formatting diverse Oracle types into strings (Base64 for BLOBs, WKT for Spatial),
/// and streaming the results into Gzip-compressed CSV or high-performance Parquet files.
pub struct Extractor {
    pool: Arc<Pool<OracleConnectionManager>>,
    prefetch_rows: u32,
    delimiter: u8,
}

/// Helper enum to manage different Arrow array builders for Parquet export.
enum BoxedBuilder {
    Int64(Int64Builder),
    Float64(Float64Builder),
    String(StringBuilder),
    Boolean(BooleanBuilder),
    Timestamp(TimestampMicrosecondBuilder),
    Binary(BinaryBuilder),
    Decimal128(Decimal128Builder),
}

impl BoxedBuilder {
    fn new(dt: &DataType, capacity: usize) -> Self {
        match dt {
            DataType::Int64 => BoxedBuilder::Int64(Int64Builder::with_capacity(capacity)),
            DataType::Float64 => BoxedBuilder::Float64(Float64Builder::with_capacity(capacity)),
            DataType::Boolean => BoxedBuilder::Boolean(BooleanBuilder::with_capacity(capacity)),
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                BoxedBuilder::Timestamp(TimestampMicrosecondBuilder::with_capacity(capacity))
            }
            DataType::Binary => BoxedBuilder::Binary(BinaryBuilder::with_capacity(capacity, capacity * 100)),
            DataType::Decimal128(p, s) => {
                BoxedBuilder::Decimal128(Decimal128Builder::with_capacity(capacity).with_precision_and_scale(*p, *s).unwrap())
            }
            _ => BoxedBuilder::String(StringBuilder::with_capacity(capacity, capacity * 20)),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            BoxedBuilder::Int64(b) => Arc::new(b.finish()) as ArrayRef,
            BoxedBuilder::Float64(b) => Arc::new(b.finish()) as ArrayRef,
            BoxedBuilder::String(b) => Arc::new(b.finish()) as ArrayRef,
            BoxedBuilder::Boolean(b) => Arc::new(b.finish()) as ArrayRef,
            BoxedBuilder::Timestamp(b) => Arc::new(b.finish()) as ArrayRef,
            BoxedBuilder::Binary(b) => Arc::new(b.finish()) as ArrayRef,
            BoxedBuilder::Decimal128(b) => Arc::new(b.finish()) as ArrayRef,
        }
    }
}

/// Manages a batch of Arrow builders for Parquet streaming.
struct ParquetRowBatch {
    builders: Vec<BoxedBuilder>,
    schema: Arc<Schema>,
    batch_size: usize,
    row_count: usize,
    total_bytes: u64,
}

impl ParquetRowBatch {
    fn new(schema: Arc<Schema>, batch_size: usize) -> Self {
        let builders = schema.fields().iter()
            .map(|f| BoxedBuilder::new(f.data_type(), batch_size))
            .collect();
        Self {
            builders,
            schema,
            batch_size,
            row_count: 0,
            total_bytes: 0,
        }
    }

    fn push_row(&mut self, row: &oracle::Row, col_types: &[OracleType], adapter: &Extractor) -> Result<()> {
        for (i, builder) in self.builders.iter_mut().enumerate() {
            match builder {
                BoxedBuilder::Int64(b) => {
                    let v: Option<i64> = row.get(i).map_err(|e| ExportError::OracleError(e.to_string()))?;
                    b.append_option(v);
                    self.total_bytes += 8;
                }
                BoxedBuilder::Float64(b) => {
                    let v: Option<f64> = row.get(i).map_err(|e| ExportError::OracleError(e.to_string()))?;
                    b.append_option(v);
                    self.total_bytes += 8;
                }
                BoxedBuilder::Boolean(b) => {
                    let v: Option<bool> = row.get(i).map_err(|e| ExportError::OracleError(e.to_string()))?;
                    b.append_option(v);
                    self.total_bytes += 1;
                }
                BoxedBuilder::Timestamp(b) => {
                    let v: Option<oracle::sql_type::Timestamp> = row.get(i).map_err(|e| ExportError::OracleError(e.to_string()))?;
                    if let Some(ts) = v {
                        // Better use chrono for conversion
                        let y = ts.year();
                        let m = ts.month();
                        let d = ts.day();
                        let h = ts.hour();
                        let min = ts.minute();
                        let s = ts.second();
                        let us = ts.nanosecond() / 1000;

                        if let Some(dt) = chrono::NaiveDate::from_ymd_opt(y, m, d)
                            .and_then(|nd| nd.and_hms_micro_opt(h, min, s, us)) {
                                b.append_value(dt.and_utc().timestamp_micros());
                        } else {
                            b.append_null();
                        }
                    } else {
                        b.append_null();
                    }
                    self.total_bytes += 8;
                }
                BoxedBuilder::Binary(b) => {
                    let v: Option<Vec<u8>> = row.get(i).map_err(|e| ExportError::OracleError(e.to_string()))?;
                    if let Some(bytes) = &v {
                        self.total_bytes += bytes.len() as u64;
                    }
                    b.append_option(v);
                }
                BoxedBuilder::Decimal128(b) => {
                    let v: Option<String> = row.get(i).map_err(|e| ExportError::OracleError(e.to_string()))?;
                    if let Some(s) = v {
                        // Very basic decimal parsing (removes dot)
                        let clean = s.replace(".", "");
                        if let Ok(val) = clean.parse::<i128>() {
                            b.append_value(val);
                        } else {
                            b.append_null();
                        }
                    } else {
                        b.append_null();
                    }
                    self.total_bytes += 16;
                }
                BoxedBuilder::String(b) => {
                    let val_str = adapter.format_value(row, i, &col_types[i])?;
                    self.total_bytes += val_str.len() as u64;
                    b.append_value(val_str);
                }
            }
        }
        self.row_count += 1;
        Ok(())
    }

    fn flush(&mut self, writer: &mut ArrowWriter<File>) -> Result<()> {
        if self.row_count == 0 {
            return Ok(());
        }

        let arrays: Vec<ArrayRef> = self.builders.iter_mut()
            .map(|b| b.finish())
            .collect();

        let batch = arrow_array::RecordBatch::try_new(self.schema.clone(), arrays)
            .map_err(|e| ExportError::ArtifactError(format!("Failed to create RecordBatch: {}", e)))?;

        writer.write(&batch)
            .map_err(|e| ExportError::ArtifactError(format!("Failed to write Parquet batch: {}", e)))?;

        // Re-initialize builders for next batch
        self.builders = self.schema.fields().iter()
            .map(|f| BoxedBuilder::new(f.data_type(), self.batch_size))
            .collect();
        self.row_count = 0;
        Ok(())
    }
}

impl Extractor {
    /// Creates a new Extractor with a shared connection pool and performance settings.
    pub fn new(
        pool: Arc<Pool<OracleConnectionManager>>,
        prefetch_rows: u32,
        delimiter: u8,
    ) -> Self {
        Self {
            pool,
            prefetch_rows,
            delimiter,
        }
    }

    /// Obtains a connection from the pool.
    fn get_conn(&self) -> Result<r2d2::PooledConnection<OracleConnectionManager>> {
        self.pool.get()
            .map_err(|e| ExportError::OracleError(format!("Failed to get connection from pool: {}", e)))
    }
}

impl ExtractionPort for Extractor {
    fn export_task(&self, task: ExportTask) -> Result<TaskResult> {
        match task.file_format {
            FileFormat::Csv => self.export_csv(task),
            FileFormat::Parquet => self.export_parquet(task),
        }
    }
}

impl Extractor {
    /// Extracts data from a table and formats it as a Gzip-compressed CSV file.
    fn export_csv(&self, task: ExportTask) -> Result<TaskResult> {
        let table_name = format!("{}.{}", task.schema, task.table);
        let chunk_ctx = task.chunk_id.map(|id| format!(" (chunk {})", id)).unwrap_or_default();
        
        match self.internal_export_csv(task) {
            Ok(res) => Ok(res),
            Err(e) => Err(ExportError::ExtractionError { 
                table: table_name, 
                reason: format!("CSV export failed{}: {}", chunk_ctx, e) 
            }),
        }
    }

    fn internal_export_csv(&self, task: ExportTask) -> Result<TaskResult> {
        let start_time = Instant::now();
        let conn = self.get_conn()?;

        // 1. Prepare Query
        let (sql, _raw_col_names) = self.prepare_query(&conn, &task)?;

        // 2. Setup Statement
        let mut stmt = conn
            .statement(&sql)
            .prefetch_rows(self.prefetch_rows)
            .build()
            .map_err(|e| ExportError::OracleError(e.to_string()))?;

        let rows = stmt.query(&[]).map_err(|e| ExportError::OracleError(e.to_string()))?;
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
            let row = row_res.map_err(|e| ExportError::OracleError(e.to_string()))?;
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

    /// Extracts data from a table and formats it as an Apache Parquet file.
    fn export_parquet(&self, task: ExportTask) -> Result<TaskResult> {
        let table_name = format!("{}.{}", task.schema, task.table);
        let chunk_ctx = task.chunk_id.map(|id| format!(" (chunk {})", id)).unwrap_or_default();

        match self.internal_export_parquet(task) {
            Ok(res) => Ok(res),
            Err(e) => Err(ExportError::ExtractionError { 
                table: table_name, 
                reason: format!("Parquet export failed{}: {}", chunk_ctx, e) 
            }),
        }
    }

    fn internal_export_parquet(&self, task: ExportTask) -> Result<TaskResult> {
        let start_time = Instant::now();
        let conn = self.get_conn()?;

        // 1. Prepare Query
        let (sql, _raw_col_names) = self.prepare_query(&conn, &task)?;

        // 2. Setup Statement
        let mut stmt = conn
            .statement(&sql)
            .prefetch_rows(self.prefetch_rows)
            .build()
            .map_err(|e| ExportError::OracleError(e.to_string()))?;

        let rows = stmt.query(&[]).map_err(|e| ExportError::OracleError(e.to_string()))?;
        let col_infos = rows.column_info();
        let col_types: Vec<OracleType> =
            col_infos.iter().map(|c| c.oracle_type().clone()).collect();
        let col_names: Vec<String> = col_infos.iter().map(|c| c.name().to_string()).collect();

        // 3. Setup Arrow Schema
        let arrow_fields: Vec<Field> = col_names
            .iter()
            .enumerate()
            .map(|(i, name)| {
                let dt = mapping::map_oracle_to_arrow(&col_types[i], Some(col_infos[i].oracle_type().to_string().as_str()));
                Field::new(name, dt, true)
            })
            .collect();
        let schema = Arc::new(Schema::new(arrow_fields));

        // 4. Setup Parquet Writer
        let file = File::create(&task.output_file).map_err(ExportError::IoError)?;
        let props = WriterProperties::builder()
            .set_compression(Self::map_parquet_compression(&task.parquet_compression))
            .build();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
            .map_err(|e| ExportError::ArtifactError(e.to_string()))?;

        // 5. Stream & Batch Results
        let batch_size = task.parquet_batch_size.unwrap_or(10000);
        let mut row_batch = ParquetRowBatch::new(schema, batch_size);

        let mut total_count = 0;

        for row_res in rows {
            let row = row_res.map_err(|e| ExportError::OracleError(e.to_string()))?;
            row_batch.push_row(&row, &col_types, self)?;
            total_count += 1;

            if total_count % batch_size as u64 == 0 {
                row_batch.flush(&mut writer)?;
            }
        }

        // Write final batch
        row_batch.flush(&mut writer)?;

        writer
            .close()
            .map_err(|e| ExportError::ArtifactError(e.to_string()))?;

        Ok(TaskResult::success(
            task.schema,
            task.table,
            total_count,
            row_batch.total_bytes,
            start_time.elapsed().as_secs_f64(),
            task.chunk_id,
        ))
    }

    /// Maps a compression string to the corresponding Parquet compression codec.
    fn map_parquet_compression(c: &Option<String>) -> ParquetCompression {
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

impl Extractor {
    /// Prepares the SQL SELECT statement and retrieves mapped column names.
    fn prepare_query(&self, conn: &Connection, task: &ExportTask) -> Result<(String, Vec<String>)> {
        let virtual_map = get_virtual_columns_map(conn, &task.schema, &task.table);

        let mut stmt_cols = conn
            .statement(SQL_GET_COLUMNS)
            .build()
            .map_err(|e| ExportError::OracleError(e.to_string()))?;
        let rows_cols = stmt_cols
            .query(&[&task.table.to_uppercase(), &task.schema.to_uppercase()])
            .map_err(|e| ExportError::OracleError(e.to_string()))?;

        let mut select_parts = Vec::new();
        let mut raw_names = Vec::new();
        let mut hash_parts = Vec::new();

        for row_res in rows_cols {
            let row = row_res.map_err(|e| ExportError::OracleError(e.to_string()))?;
            let name: String = row.get(0).map_err(|e| ExportError::OracleError(e.to_string()))?;
            let data_type: String = row.get(1).map_err(|e| ExportError::OracleError(e.to_string()))?;
            let is_virtual: String = row.get(3).map_err(|e| ExportError::OracleError(e.to_string()))?;
            let is_hidden: String = row.get(4).map_err(|e| ExportError::OracleError(e.to_string()))?;
            let is_user_gen: String = row.get(6).map_err(|e| ExportError::OracleError(e.to_string()))?;

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
            } else if upper_type.contains("INTERVAL") {
                format!("TO_CHAR(\"{}\")", name)
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

    /// Formats a single Oracle column value into a String representation.
    fn format_value(&self, row: &oracle::Row, i: usize, otype: &OracleType) -> Result<String> {
        match otype {
            OracleType::Number(_, _)
            | OracleType::Int64
            | OracleType::Float(_)
            | OracleType::BinaryFloat
            | OracleType::BinaryDouble => {
                let v: Option<String> = row.get(i).map_err(|e| ExportError::OracleError(e.to_string()))?;
                Ok(v.unwrap_or_default())
            }
            OracleType::Date
            | OracleType::Timestamp(_)
            | OracleType::TimestampTZ(_)
            | OracleType::TimestampLTZ(_) => {
                let v: Option<Timestamp> = row.get(i).map_err(|e| ExportError::OracleError(e.to_string()))?;
                Ok(v.map(|ts| Self::format_timestamp(&ts)).unwrap_or_default())
            }
            OracleType::Raw(_) | OracleType::BLOB => {
                let v: Option<Vec<u8>> = row.get(i).map_err(|e| ExportError::OracleError(e.to_string()))?;
                Ok(v.map(|b| general_purpose::STANDARD.encode(b))
                    .unwrap_or_default())
            }
            _ => {
                let v: Option<String> = row.get(i).map_err(|e| ExportError::OracleError(e.to_string()))?;
                Ok(v.unwrap_or_default())
            }
        }
    }

    /// Formats an Oracle `Timestamp` into a BigQuery-compatible TIMESTAMP string.
    fn format_timestamp(ts: &Timestamp) -> String {
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
        assert_eq!(
            Extractor::map_parquet_compression(&None),
            ParquetCompression::ZSTD(Default::default())
        );
        assert_eq!(
            Extractor::map_parquet_compression(&Some("snappy".to_string())),
            ParquetCompression::SNAPPY
        );
        assert_eq!(
            Extractor::map_parquet_compression(&Some("gzip".to_string())),
            ParquetCompression::GZIP(Default::default())
        );
        assert_eq!(
            Extractor::map_parquet_compression(&Some("none".to_string())),
            ParquetCompression::UNCOMPRESSED
        );
        assert_eq!(
            Extractor::map_parquet_compression(&Some("invalid".to_string())),
            ParquetCompression::ZSTD(Default::default())
        );
    }

    #[test]
    fn test_format_timestamp() {
        let ts = Timestamp::new(2023, 10, 27, 14, 30, 45, 123456000).unwrap();
        assert_eq!(Extractor::format_timestamp(&ts), "2023-10-27 14:30:45.123456");

        let ts2 = Timestamp::new(2023, 1, 1, 0, 0, 0, 0).unwrap();
        assert_eq!(Extractor::format_timestamp(&ts2), "2023-01-01 00:00:00.000000");
    }
}
