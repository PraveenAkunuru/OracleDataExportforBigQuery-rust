//! # Oracle High-Performance Extractor
//!
//! This is the "Engine" of the data migration. It performs several critical tasks:
//! 1. **Streaming**: It fetches rows from Oracle in batches (prefetching) to minimize network roundtrips.
//! 2. **Abstraction**: It uses the `RowWriter` trait to support both CSV and Parquet without duplicating logic.
//! 3. **Memory Management**: It processes rows one by one (or in small batches), so it can move Terabytes of data using only a few Megabytes of RAM.

use crate::domain::mapping;
use crate::domain::errors::{ExportError, Result};
use crate::domain::entities::{ExportTask, FileFormat, TaskResult, TableMetadata};
use crate::infrastructure::oracle::sql_utils::build_export_query;
use crate::ports::extraction_port::ExtractionPort;
use base64::{engine::general_purpose, Engine as _};
use csv::{QuoteStyle, WriterBuilder};
use flate2::write::GzEncoder;
use flate2::Compression as GzipCompression;
use oracle::{sql_type::OracleType, sql_type::Timestamp, Row, ColumnInfo};
use r2d2::Pool;
use crate::infrastructure::oracle::connection_manager::OracleConnectionManager;
use std::fs::File;
use std::io::BufWriter;
use std::time::Instant;
use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::builder::*;
use arrow_schema::Schema;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression as ParquetCompression;
use parquet::file::properties::WriterProperties;

/// `Extractor` implements the `ExtractionPort`.
pub struct Extractor {
    /// The database connection pool.
    pool: Arc<Pool<OracleConnectionManager>>,
    /// How many rows to fetch from Oracle in a single network call.
    prefetch_rows: u32,
    /// Which character to use to separate fields in CSV (e.g., `,` or ASCII 16).
    field_delimiter: u8,
}

impl Extractor {
    pub fn new(pool: Arc<Pool<OracleConnectionManager>>, prefetch_rows: u32, field_delimiter: u8) -> Self {
        Self { pool, prefetch_rows, field_delimiter }
    }

    /// Converts an Oracle value into a String.
    /// Used for CSV export and for mapping complex types.
    fn format_value(&self, row: &Row, i: usize, otype: &OracleType) -> Result<String> {
        match otype {
            OracleType::Number(_, _) | OracleType::Int64 | OracleType::Float(_) | OracleType::BinaryFloat | OracleType::BinaryDouble => {
                let v: Option<String> = row.get(i).map_err(|e| ExportError::OracleError(e.to_string()))?;
                Ok(v.unwrap_or_default())
            }
            OracleType::Date | OracleType::Timestamp(_) | OracleType::TimestampTZ(_) | OracleType::TimestampLTZ(_) => {
                let v: Option<Timestamp> = row.get(i).map_err(|e| ExportError::OracleError(e.to_string()))?;
                // BigQuery expects "YYYY-MM-DD HH:MM:SS"
                Ok(v.map(|ts| format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}", ts.year(), ts.month(), ts.day(), ts.hour(), ts.minute(), ts.second(), ts.nanosecond() / 1000)).unwrap_or_default())
            }
            OracleType::Raw(_) | OracleType::BLOB => {
                // Binary data can't go into a CSV normally, so we encode it as Base64.
                let v: Option<Vec<u8>> = row.get(i).map_err(|e| ExportError::OracleError(e.to_string()))?;
                Ok(v.map(|b| general_purpose::STANDARD.encode(b)).unwrap_or_default())
            }
            _ => {
                let v: Option<String> = row.get(i).map_err(|e| ExportError::OracleError(e.to_string()))?;
                Ok(v.unwrap_or_default())
            }
        }
    }
}

/// `RowWriter` is an internal trait that abstracts the file format.
///
/// This allows the `export_task` function to focus on "reading from Oracle" 
/// and simply call `.write_row()` without caring if it's writing to CSV or Parquet.
trait RowWriter {
    fn write_row(&mut self, row: &Row, extractor: &Extractor) -> Result<()>;
    fn finish(self: Box<Self>) -> Result<(u64, u64)>;
}

/// Implementation for writing Gzipped CSV files.
struct CsvRowWriter {
    writer: csv::Writer<GzEncoder<BufWriter<File>>>,
    col_types: Vec<OracleType>,
    bytes: u64,
    count: u64,
}

impl CsvRowWriter {
    fn new(path: &str, delimiter: u8, col_infos: &[ColumnInfo]) -> Result<Self> {
        let file = File::create(path).map_err(ExportError::IoError)?;
        // We use a multi-layered writer: 
        // File -> BufWriter (for speed) -> GzEncoder (for compression) -> csv::Writer (for formatting).
        let writer = WriterBuilder::new()
            .delimiter(delimiter)
            .quote_style(QuoteStyle::NonNumeric)
            .from_writer(GzEncoder::new(BufWriter::with_capacity(128 * 1024, file), GzipCompression::fast()));
        
        let mut s = Self { 
            writer, 
            col_types: col_infos.iter().map(|c| c.oracle_type().clone()).collect(), 
            bytes: 0, 
            count: 0 
        };
        
        // Write the header row.
        let names: Vec<_> = col_infos.iter().map(|c| c.name()).collect();
        s.writer.write_record(&names).map_err(|e| ExportError::ArtifactError(e.to_string()))?;
        Ok(s)
    }
}

impl RowWriter for CsvRowWriter {
    fn write_row(&mut self, row: &Row, extractor: &Extractor) -> Result<()> {
        let mut record = Vec::with_capacity(self.col_types.len());
        for (i, otype) in self.col_types.iter().enumerate() {
            let val = extractor.format_value(row, i, otype)?;
            self.bytes += val.len() as u64;
            record.push(val);
        }
        self.writer.write_record(&record).map_err(|e| ExportError::ArtifactError(e.to_string()))?;
        self.count += 1;
        Ok(())
    }
    fn finish(mut self: Box<Self>) -> Result<(u64, u64)> {
        self.writer.flush().map_err(|e| ExportError::ArtifactError(e.to_string()))?;
        Ok((self.count, self.bytes))
    }
}

/// Implementation for writing Apache Parquet files using Arrow.
struct ParquetRowWriter {
    writer: ArrowWriter<File>,
    builders: Vec<Box<dyn ArrowPush>>,
    col_types: Vec<OracleType>,
    schema: Arc<Schema>,
    batch_size: usize,
    count: u64,
    bytes: u64,
    current_batch: usize,
}

impl ParquetRowWriter {
    fn new(path: &str, compression: &Option<String>, batch_size: usize, col_infos: &[ColumnInfo]) -> Result<Self> {
        // Step 1: Create the Arrow Schema from Oracle column info.
        let arrow_fields: Vec<_> = col_infos.iter().map(|c| {
            let dt = mapping::map_oracle_to_arrow(c.oracle_type(), Some(c.oracle_type().to_string().as_str()));
            arrow_schema::Field::new(c.name(), dt, true)
        }).collect();
        let schema = Arc::new(Schema::new(arrow_fields));
        
        let file = File::create(path).map_err(ExportError::IoError)?;
        let props = WriterProperties::builder().set_compression(Self::map_compression(compression)).build();
        let writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).map_err(|e| ExportError::ArtifactError(e.to_string()))?;
        
        let col_types: Vec<_> = col_infos.iter().map(|c| c.oracle_type().clone()).collect();
        
        // Step 2: Initialize "Builders". Builders are memory-buffers that hold data
        // before it gets flushed into the Parquet file.
        let builders = schema.fields().iter().zip(col_types.iter())
            .map(|(f, ot)| create_push_builder(f.data_type(), ot, batch_size))
            .collect();
            
        Ok(Self { writer, builders, col_types, schema, batch_size, count: 0, bytes: 0, current_batch: 0 })
    }

    /// Maps simple string names to Parquet compression algorithms.
    fn map_compression(c: &Option<String>) -> ParquetCompression {
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

    /// Flushes the current batch of rows in memory to the file.
    fn flush(&mut self) -> Result<()> {
        if self.current_batch == 0 { return Ok(()); }
        let arrays: Vec<ArrayRef> = self.builders.iter_mut().map(|b| b.finish()).collect();
        let batch = arrow_array::RecordBatch::try_new(self.schema.clone(), arrays).map_err(|e| ExportError::ArtifactError(e.to_string()))?;
        self.writer.write(&batch).map_err(|e| ExportError::ArtifactError(e.to_string()))?;
        
        // Reset builders for the next batch.
        self.builders = self.schema.fields().iter().zip(self.col_types.iter())
            .map(|(f, ot)| create_push_builder(f.data_type(), ot, self.batch_size))
            .collect();
        self.current_batch = 0;
        Ok(())
    }
}

impl RowWriter for ParquetRowWriter {
    fn write_row(&mut self, row: &Row, extractor: &Extractor) -> Result<()> {
        for (i, builder) in self.builders.iter_mut().enumerate() {
            self.bytes += builder.push(row, i, extractor)?;
        }
        self.count += 1;
        self.current_batch += 1;
        if self.current_batch >= self.batch_size { 
            self.flush()?; 
        }
        Ok(())
    }
    fn finish(mut self: Box<Self>) -> Result<(u64, u64)> {
        self.flush()?;
        self.writer.close().map_err(|e| ExportError::ArtifactError(e.to_string()))?;
        Ok((self.count, self.bytes))
    }
}

impl ExtractionPort for Extractor {
    /// The main export loop.
    fn export_task(&self, task: ExportTask, metadata: &TableMetadata) -> Result<TaskResult> {
        let start_time = Instant::now();
        
        // 1. Get a connection from the pool.
        let conn = self.pool.get().map_err(|e| ExportError::OracleError(e.to_string()))?;
        
        // 2. Build the SQL query (handled by sql_utils).
        let sql = build_export_query(
            &metadata.schema, 
            &metadata.table_name, 
            &metadata.columns, 
            task.enable_row_hash, 
            task.query_where.as_deref()
        );
        
        // 3. Prepare and execute the statement.
        let mut stmt = conn.statement(&sql)
            .prefetch_rows(self.prefetch_rows)
            .build()
            .map_err(|e| ExportError::OracleError(e.to_string()))?;
        let rows = stmt.query(&[]).map_err(|e| ExportError::OracleError(e.to_string()))?;
        
        // 4. Create the appropriate writer based on format.
        // `Box<dyn RowWriter>` allows us to use either CsvRowWriter or ParquetRowWriter 
        // through the same interface.
        let mut writer: Box<dyn RowWriter> = match task.file_format {
            FileFormat::Csv => {
                Box::new(CsvRowWriter::new(&task.output_file, self.field_delimiter, rows.column_info())?)
            },
            FileFormat::Parquet => {
                Box::new(ParquetRowWriter::new(&task.output_file, &task.parquet_compression, task.parquet_batch_size.unwrap_or(10000), rows.column_info())?)
            },
        };

        // 5. STREAMING LOOP
        // We iterate through every row returned by Oracle.
        for row_res in rows {
            let row = row_res.map_err(|e| ExportError::OracleError(e.to_string()))?;
            writer.write_row(&row, self)?;
        }

        // 6. Finalize and report.
        let (count, bytes) = writer.finish()?;
        Ok(TaskResult::success(task.schema, task.table, count, bytes, start_time.elapsed().as_secs_f64(), task.chunk_id))
    }
}

/// Helper trait for "Pushing" Oracle row data into Arrow columnar buffers.
trait ArrowPush: Send {
    fn push(&mut self, row: &Row, i: usize, extractor: &Extractor) -> Result<u64>;
    fn finish(&mut self) -> ArrayRef;
}

// Below are specific implementations of ArrowPush for different types (Strings, Ints, etc).
// We use a macro to avoid repeating the same logic for simple types.

macro_rules! impl_push {
    ($name:ident, $builder:ident, $type:ty, $conv:expr) => {
        struct $name($builder);
        impl ArrowPush for $name {
            fn push(&mut self, row: &Row, i: usize, _ex: &Extractor) -> Result<u64> {
                let v: Option<$type> = row.get(i).map_err(|e| ExportError::OracleError(e.to_string()))?;
                let len = $conv(&v);
                self.0.append_option(v);
                Ok(len)
            }
            fn finish(&mut self) -> ArrayRef { Arc::new(self.0.finish()) }
        }
    };
}

impl_push!(Int64Push, Int64Builder, i64, |_| 8);
impl_push!(Float64Push, Float64Builder, f64, |_| 8);
impl_push!(BooleanPush, BooleanBuilder, bool, |_| 1);

struct StringPush(StringBuilder, OracleType);
impl ArrowPush for StringPush {
    fn push(&mut self, row: &Row, i: usize, ex: &Extractor) -> Result<u64> {
        let s = ex.format_value(row, i, &self.1)?;
        let len = s.len() as u64;
        self.0.append_value(s);
        Ok(len)
    }
    fn finish(&mut self) -> ArrayRef { Arc::new(self.0.finish()) }
}

struct TimestampPush(TimestampMicrosecondBuilder);
impl ArrowPush for TimestampPush {
    fn push(&mut self, row: &Row, i: usize, _ex: &Extractor) -> Result<u64> {
        let v: Option<Timestamp> = row.get(i).map_err(|e| ExportError::OracleError(e.to_string()))?;
        if let Some(ts) = v {
            if let Some(dt) = chrono::NaiveDate::from_ymd_opt(ts.year(), ts.month(), ts.day()).and_then(|d| d.and_hms_micro_opt(ts.hour(), ts.minute(), ts.second(), ts.nanosecond()/1000)) {
                self.0.append_value(dt.and_utc().timestamp_micros());
            } else { self.0.append_null(); }
        } else { self.0.append_null(); }
        Ok(8)
    }
    fn finish(&mut self) -> ArrayRef { Arc::new(self.0.finish()) }
}

struct BinaryPush(BinaryBuilder);
impl ArrowPush for BinaryPush {
    fn push(&mut self, row: &Row, i: usize, _ex: &Extractor) -> Result<u64> {
        let b: Option<Vec<u8>> = row.get(i).map_err(|e| ExportError::OracleError(e.to_string()))?;
        let len = b.as_ref().map(|x| x.len()).unwrap_or(0) as u64;
        self.0.append_option(b);
        Ok(len)
    }
    fn finish(&mut self) -> ArrayRef { Arc::new(self.0.finish()) }
}

struct Decimal128Push(Decimal128Builder);
impl ArrowPush for Decimal128Push {
    fn push(&mut self, row: &Row, i: usize, _ex: &Extractor) -> Result<u64> {
        let v: Option<String> = row.get(i).map_err(|e| ExportError::OracleError(e.to_string()))?;
        if let Some(s) = v {
            if let Ok(val) = s.replace(".", "").parse::<i128>() { 
                self.0.append_value(val); 
            } else { 
                self.0.append_null(); 
            }
        } else { self.0.append_null(); }
        Ok(16)
    }
    fn finish(&mut self) -> ArrayRef { Arc::new(self.0.finish()) }
}

/// Dynamic factory to create the right "Pusher" for a given Arrow type.
fn create_push_builder(dt: &arrow_schema::DataType, ot: &OracleType, cap: usize) -> Box<dyn ArrowPush> {
    use arrow_schema::DataType::*;
    match dt {
        Int64 => Box::new(Int64Push(Int64Builder::with_capacity(cap))),
        Float64 => Box::new(Float64Push(Float64Builder::with_capacity(cap))),
        Utf8 => Box::new(StringPush(StringBuilder::with_capacity(cap, cap * 32), ot.clone())),
        Boolean => Box::new(BooleanPush(BooleanBuilder::with_capacity(cap))),
        Timestamp(arrow_schema::TimeUnit::Microsecond, _) => Box::new(TimestampPush(TimestampMicrosecondBuilder::with_capacity(cap))),
        Binary => Box::new(BinaryPush(BinaryBuilder::with_capacity(cap, cap * 64))),
        Decimal128(p, s) => Box::new(Decimal128Push(Decimal128Builder::with_capacity(cap).with_precision_and_scale(*p, *s).unwrap())),
        _ => Box::new(StringPush(StringBuilder::with_capacity(cap, cap * 32), ot.clone())),
    }
}
