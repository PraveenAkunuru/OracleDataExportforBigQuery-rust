//! # Artifact Port
//!
//! This Port defines the interface for creating "Sidecar" files.
//!
//! When we export data, we also need to generate the `CREATE TABLE` scripts 
//! for BigQuery so that the user can actually load the data.

use crate::domain::entities::{FileFormat, TableMetadata};
use crate::domain::errors::Result;

/// `ArtifactPort` handles the generation of BigQuery DDL, load scripts, and JSON schemas.
pub trait ArtifactPort: Send + Sync {
    /// Writes various configuration and setup files to the specified directory.
    fn write_artifacts(
        &self,
        metadata: &TableMetadata,
        output_config_dir: &str,
        enable_row_hash: bool,
        file_format: FileFormat,
    ) -> Result<()>;
}
