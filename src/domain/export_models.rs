//! Domain models representing the core entities of the export process.
//!
//! These models are used across application, ports, and infrastructure layers
//! to maintain a consistent data representation.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Supported file formats for data extraction.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FileFormat {
    /// Comma-separated values (CSV), compressed with Gzip.
    Csv,
    /// Apache Parquet format.
    Parquet,
}

impl fmt::Display for FileFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FileFormat::Csv => write!(f, "CSV"),
            FileFormat::Parquet => write!(f, "PARQUET"),
        }
    }
}

/// Represents metadata for a single Oracle column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMetadata {
    /// Column name in Oracle.
    pub name: String,
    /// Raw Oracle data type (e.g., VARCHAR2, NUMBER).
    pub raw_type: String,
    /// Targeted BigQuery data type (e.g., STRING, INT64).
    pub bq_type: String,
    /// Whether the column is virtual (calculated via expression).
    pub is_virtual: bool,
    /// Whether the column is hidden (internal Oracle col).
    pub is_hidden: bool,
    /// Whether the column is an identity column.
    pub is_identity: bool,
    /// Optional comment/description from Oracle metadata.
    pub comment: Option<String>,
}

/// Represents the structure and size of an Oracle table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    /// Owner/Schema of the table.
    pub schema: String,
    /// Name of the table.
    pub table_name: String,
    /// List of columns including their types and properties.
    pub columns: Vec<ColumnMetadata>,
    /// Estimated size of the table in Gigabytes.
    pub size_gb: f64,
    /// List of primary key column names.
    pub pk_cols: Vec<String>,
    /// List of partition column names.
    pub partition_cols: Vec<String>,
    /// List of indexed column names.
    pub index_cols: Vec<String>,
}

/// Represents an aggregation result for validation purposes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnAggregate {
    /// Name of the column being aggregated.
    pub column_name: String,
    /// Type of aggregation (e.g., SUM, MIN, MAX).
    pub agg_type: String,
    /// String representation of the resulting value.
    pub value: String,
}

/// Holds statistics used to validate the integrity of an export.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationStats {
    /// Table name being validated.
    pub table_name: String,
    /// Total row count in the source table.
    pub row_count: u64,
    /// Hash of all primary keys (optional).
    pub pk_hash: Option<String>,
    /// List of column aggregates (optional).
    pub aggregates: Option<Vec<ColumnAggregate>>,
}

/// Defines a specific unit of work for data extraction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportTask {
    /// Schema name.
    pub schema: String,
    /// Table name.
    pub table: String,
    /// Optional chunk identifier (for parallel chunked exports).
    pub chunk_id: Option<u32>,
    /// Optional WHERE clause to filter data (used for chunking).
    pub query_where: Option<String>,
    /// Absolute path to the destination CSV file.
    pub output_file: String,
    /// Whether to calculate a SHA256 hash for each row.
    pub enable_row_hash: bool,
    /// Whether to use client-side (Rust) or server-side (Oracle) hashing.
    pub use_client_hash: bool,
    /// The target file format (CSV or Parquet).
    pub file_format: FileFormat,
    /// Optional compression for Parquet (e.g., zstd, snappy).
    pub parquet_compression: Option<String>,
}

/// Captures the outcome of an individual export task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResult {
    /// Schema name.
    pub schema: String,
    /// Table name.
    pub table: String,
    /// Unique identifier for the chunk (if chunked).
    pub chunk_id: Option<u32>,
    /// Total rows exported in this task.
    pub rows: u64,
    /// Total bytes (compressed) written to disk.
    pub bytes: u64,
    /// Wall-clock time taken for execution in seconds.
    pub duration: f64,
    /// Outcome status: SUCCESS or FAILED.
    pub status: String,
    /// Detailed error message if the task failed.
    pub error: Option<String>,
}

impl TaskResult {
    /// Creates a successful TaskResult.
    pub fn success(
        schema: String,
        table: String,
        rows: u64,
        bytes: u64,
        duration: f64,
        chunk_id: Option<u32>,
    ) -> Self {
        Self {
            schema,
            table,
            chunk_id,
            rows,
            bytes,
            duration,
            status: "SUCCESS".to_string(),
            error: None,
        }
    }

    /// Creates a failed TaskResult with an error message.
    pub fn failure(schema: String, table: String, chunk_id: Option<u32>, error: String) -> Self {
        Self {
            schema,
            table,
            chunk_id,
            rows: 0,
            bytes: 0,
            duration: 0.0,
            status: "FAILED".to_string(),
            error: Some(error),
        }
    }
}
