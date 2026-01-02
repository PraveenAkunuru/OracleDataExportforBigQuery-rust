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
