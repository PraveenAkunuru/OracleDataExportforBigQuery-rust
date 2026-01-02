//! # Extraction Port
//!
//! This Port defines the contract for the "Data Mover".
//!
//! Anything that implements `ExtractionPort` must be able to take an 
//! `ExportTask` (instructions on what to move) and actually stream those 
//! rows to a file.

use crate::domain::entities::{ExportTask, TableMetadata, TaskResult};
use crate::domain::errors::Result;

/// `ExtractionPort` handles the heavy-lifting of the export.
pub trait ExtractionPort: Send + Sync {
    /// Executes a single export task.
    ///
    /// It takes a `task` (where to save, what to filter) and `metadata` 
    /// (column names and types). It returns a `TaskResult` with progress stats.
    fn export_task(&self, task: ExportTask, metadata: &TableMetadata) -> Result<TaskResult>;
}
