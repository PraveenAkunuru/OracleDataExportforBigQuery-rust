// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
