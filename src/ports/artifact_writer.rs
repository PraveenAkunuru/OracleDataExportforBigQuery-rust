use crate::domain::error_definitions::Result;
use crate::domain::export_models::{FileFormat, TableMetadata};

/// Port for generating and writing sidecar artifacts (DDL, Schema, etc.).
pub trait ArtifactWriter: Send + Sync {
    /// Writes all sidecar artifacts for a given table metadata.
    fn write_artifacts(
        &self,
        metadata: &TableMetadata,
        output_config_dir: &str,
        enable_row_hash: bool,
        file_format: FileFormat,
    ) -> Result<()>;
}
