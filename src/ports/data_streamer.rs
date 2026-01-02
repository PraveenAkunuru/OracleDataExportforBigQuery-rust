//! Port defining the interface for streaming data from Oracle to compressed CSV.

use crate::domain::error_definitions::Result;
use crate::domain::export_models::{ExportTask, TaskResult};

/// Port for streaming data from a source table to a target destination.
pub trait DataStreamer: Send + Sync {
    /// Executes an export task (streaming rows to a local file).
    fn export_task(&self, task: ExportTask) -> Result<TaskResult>;
}
