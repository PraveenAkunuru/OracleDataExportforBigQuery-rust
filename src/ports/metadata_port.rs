//! Port defining the interface for reading database schema and metadata.

use crate::domain::entities::{TableMetadata, ValidationStats};
use crate::domain::errors::Result;

/// A trait defining the capabilities required to discover and read database schemas.
///
/// Implementers of this trait are responsible for querying the source database
/// to retrieve lists of tables, column metadata, and chunking information.
pub trait MetadataPort: Send + Sync {
    /// Retrieves a list of all table names available in the specified schema.
    fn get_tables(&self, schema: &str) -> Result<Vec<String>>;

    /// Fetches comprehensive metadata for a specific table.
    fn get_table_metadata(&self, schema: &str, table: &str) -> Result<TableMetadata>;

    /// Gets the number of CPUs available on the database server.
    /// Used for adaptive parallelism calculation.
    fn get_db_cpu_count(&self) -> Result<usize>;

    /// Generates WHERE clauses for parallel chunked exports based on ROWID ranges.
    fn generate_table_chunks(
        &self,
        schema: &str,
        table: &str,
        chunk_count: usize,
    ) -> Result<Vec<String>>;

    /// Performs validation of the source table and returns statistics.
    fn validate_table(
        &self,
        schema: &str,
        table: &str,
        pk_cols: Option<&[String]>,
        agg_cols: Option<&[String]>,
    ) -> Result<ValidationStats>;
}
