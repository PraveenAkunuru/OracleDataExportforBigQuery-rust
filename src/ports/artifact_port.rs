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
