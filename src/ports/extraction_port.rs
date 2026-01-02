//! Port defining the interface for streaming data from Oracle to compressed CSV.

use crate::domain::entities::{ExportTask, TaskResult};
use crate::domain::errors::Result;

/// Port for streaming data from a source table to a target destination.
pub trait ExtractionPort: Send + Sync {
    /// Executes an export task (streaming rows to a local file).
    fn export_task(&self, task: ExportTask) -> Result<TaskResult>;
}
