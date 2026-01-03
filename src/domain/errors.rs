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

//! Core error definitions for the Oracle Data Exporter.
//!
//! This module provides a centralized `ExportError` enum and a `Result` type
//! used throughout the application to handle Oracle, I/O, and logic errors.

use thiserror::Error;

/// Error types encountered during the export process.
#[derive(Error, Debug)]
pub enum ExportError {
    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Metadata discovery failed: {0}")]
    MetadataError(String),

    #[error("Extraction failed for {table}: {reason}")]
    ExtractionError { table: String, reason: String },

    #[error("Artifact generation failed: {0}")]
    ArtifactError(String),

    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Oracle error: {0}")]
    OracleError(String),

    #[error("BigQuery error: {0}")]
    BigQueryError(String),

    #[error("Unknown error: {0}")]
    Unknown(String),
}

impl From<oracle::Error> for ExportError {
    fn from(e: oracle::Error) -> Self {
        ExportError::OracleError(e.to_string())
    }
}

/// A specialized Result type for the Oracle Data Exporter.
pub type Result<T> = std::result::Result<T, ExportError>;
