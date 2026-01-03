//! # Domain Entities
//!
//! Entities are the "Nouns" of our application. They are simple data structures
//! (structs) that represent the things we are working with: Tables, Columns,
//! Tasks, and Results.
//!
//! We use the `serde` crate (Serialize/Deserialize) to allow these structs
//! to be easily converted to/from JSON or YAML.

use serde::{Deserialize, Serialize};
use std::fmt;

/// `FileFormat` defines how we save the data on disk.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FileFormat {
    /// Comma-separated values (CSV), compressed with Gzip to save space.
    Csv,
    /// Apache Parquet: A columnar format that is very efficient for BigQuery.
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

/// `ColumnMetadata` represents everything we need to know about a single database column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMetadata {
    /// The name of the column (e.g., "USER_ID").
    pub name: String,
    /// What Oracle calls the type (e.g., "NUMBER(10,0)").
    pub raw_type: String,
    /// What BigQuery calls the type (e.g., "INT64").
    pub bq_type: String,
    /// Oracle native "Virtual Columns" (defined by an expression in the database).
    /// These are NOT materialized on disk and only exist in the Logical View.
    pub is_virtual: bool,
    /// The original SQL expression for a virtual column (e.g., "COL1 + COL2").
    pub virtual_expr: Option<String>,
    /// Columns transformed during extraction (e.g., XMLType stringified, INTERVAL formatted).
    /// These ARE materialized in the output files and exist in the Physical Table.
    pub is_transformed: bool,
    pub is_hidden: bool,
    pub is_identity: bool,
    pub comment: Option<String>,
}

/// `TableMetadata` is the blueprint for a whole table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableMetadata {
    pub schema: String,
    pub table_name: String,
    /// The list of columns in the table.
    pub columns: Vec<ColumnMetadata>,
    /// How big the table is. We use this to decide if we should use parallel "chunking".
    pub size_gb: f64,
    pub pk_cols: Vec<String>,
    pub partition_cols: Vec<String>,
    pub index_cols: Vec<String>,
}

impl TableMetadata {
    /// Determines if a BigQuery view is necessary.
    /// A view is needed only if there are native Oracle virtual columns
    /// that are not already materialized (transformed) in the physical data.
    pub fn needs_view(&self) -> bool {
        self.columns
            .iter()
            .any(|c| c.is_virtual && !c.is_transformed)
    }
}

/// `ExportTask` is a "To-Do" item for the database extractor.
/// It contains all the instructions needed to export a piece of data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportTask {
    pub schema: String,
    pub table: String,
    /// If we are splitting a table, this is which piece (chunk) we are working on.
    pub chunk_id: Option<u32>,
    /// The SQL "WHERE" clause used to filter this specific chunk.
    pub query_where: Option<String>,
    /// Where to save the resulting file.
    pub output_file: String,
    pub enable_row_hash: bool,
    pub use_client_hash: bool,
    pub file_format: FileFormat,
    pub parquet_compression: Option<String>,
    pub parquet_batch_size: Option<usize>,
}

/// `TaskResult` is the "Report Card" for an `ExportTask`.
/// It tells the Orchestrator whether the task succeeded and how much data was moved.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskResult {
    pub schema: String,
    pub table: String,
    pub chunk_id: Option<u32>,
    /// How many rows were actually exported.
    pub rows: u64,
    /// How many bytes were written to the file.
    pub bytes: u64,
    /// How long it took (in seconds).
    pub duration: f64,
    /// Either "SUCCESS" or "FAILED".
    pub status: String,
    /// If it failed, this contains the reason why.
    pub error: Option<String>,
}

impl TaskResult {
    /// Helper to create a successful result.
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

    /// Helper to create a failure result.
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

/// `ColumnAggregate` stores a validation metric (like SUM) for a column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnAggregate {
    pub column_name: String,
    pub agg_type: String,
    pub value: String,
}

/// `ValidationStats` is used for data fidelity checks between Oracle and BigQuery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationStats {
    pub table_name: String,
    pub row_count: u64,
    pub pk_hash: Option<String>,
    pub aggregates: Option<Vec<ColumnAggregate>>,
}
