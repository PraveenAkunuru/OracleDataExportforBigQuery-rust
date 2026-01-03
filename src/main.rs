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

//! # Main Entry Point
//!
//! This is where the application starts. The `main` function coordinates:
//! 1. **Configuration**: Parsing CLI arguments and loading YAML files.
//! 2. **Infrastructure**: Initializing the Runtime (Connection Pools, Thread Pools).
//! 3. **Dependency Injection**: Wiring up the Hexagonal components (Ports & Adapters).
//! 4. **Execution**: Handing control to the `Orchestrator`.

pub mod application;
pub mod config;
pub mod domain;
pub mod infrastructure;
pub mod ports;

use crate::application::orchestrator::Orchestrator;
use crate::config::{AppConfig, CliArgs};
use crate::infrastructure::artifacts::artifact_adapter::ArtifactAdapter;
use crate::infrastructure::oracle::extractor::Extractor;
use crate::infrastructure::oracle::metadata::MetadataAdapter;
use clap::Parser;
use log::{debug, error, info};
use std::process;
use std::sync::Arc;

/// The application entry point.
///
/// We use `std::process::exit` to return non-zero exit codes on failure,
/// which is important for shell scripts or CI/CD pipelines.
fn main() {
    // env_logger is a common Rust logger that reads the RUST_LOG environment variable.
    env_logger::init();

    // Parse command-line arguments using the `clap` crate.
    let args = CliArgs::parse();

    // --- STEP 1: CONFIGURATION ---
    // We try to load config from a YAML file first, then fallback to defaults.
    let mut config = if let Some(config_path) = &args.config {
        AppConfig::from_file(config_path).unwrap_or_else(|e| {
            error!("Failed to load config: {}", e);
            process::exit(1);
        })
    } else {
        AppConfig::default_from_cli(&args)
    };

    // Merge CLI overrides (CLI arguments take precedence over YAML config).
    config.merge_cli(&args);

    // Validate that required fields (like DB credentials) are present.
    config.validate().unwrap_or_else(|e| {
        error!("Invalid configuration: {}", e);
        process::exit(1);
    });

    // --- STEP 2: RUNTIME INITIALIZATION ---
    // The RuntimeContext manages resources like the Oracle connection pool and Rayon thread pool.
    let runtime = crate::application::runtime::RuntimeContext::init(&config).unwrap_or_else(|e| {
        let err_msg = e.to_string();
        error!("Runtime initialization failed: {}", err_msg);

        // Resilience: Suggest wrapper script for common library path issues
        if err_msg.contains("DPI-1047") {
            eprintln!("\n\x1b[31mError: Oracle Client libraries not found (DPI-1047).\x1b[0m");
            eprintln!("To fix this, try running with the helper script:");
            eprintln!(
                "\n    \x1b[32m./scripts/run_with_env.sh cargo run --release -- ...\x1b[0m\n"
            );
        }

        std::process::exit(1);
    });

    // --- STEP 3: COMPONENT WIRING (ports & adapters) ---
    // In Hexagonal Architecture, we define "ports" (interfaces) and "adapters" (implementations).
    // We wrap everything in `Arc` (Atomic Reference Counter) so they can be safely shared across threads.

    // 1. Metadata Port: Handles querying Oracle for table list and column types.
    let metadata_port = Arc::new(MetadataAdapter::new(runtime.pool.clone()));

    // 2. Extraction Port: Handles the heavy lifting of streaming rows to disk.
    let prefetch = config.export.prefetch_rows.unwrap_or(5000);
    let delimiter_str = config
        .export
        .field_delimiter
        .clone()
        .unwrap_or_else(|| "\u{0010}".to_string());
    let field_delimiter = delimiter_str.as_bytes()[0];
    let extraction_port = Arc::new(Extractor::new(runtime.pool, prefetch, field_delimiter));

    // 3. Artifact Port: Generates BigQuery DDL and load scripts.
    let project_id = config
        .bigquery
        .as_ref()
        .map(|b| b.project.clone())
        .unwrap_or_else(|| "PRJ".to_string());
    let dataset_id = config
        .bigquery
        .as_ref()
        .map(|b| b.dataset.clone())
        .unwrap_or_else(|| "DS".to_string());
    let artifact_port = Arc::new(ArtifactAdapter::new(project_id, dataset_id));

    // --- STEP 4: ORCHESTRATION ---
    // The Orchestrator doesn't know *how* to talk to Oracle or *how* to write files;
    // it just knows the *process* of exporting data using the provided ports.
    let orchestrator = Orchestrator::new(metadata_port, extraction_port, artifact_port, config);

    info!("Starting Export process...");
    match orchestrator.run() {
        Ok(results) => {
            debug!("Orchestrator run finished. Results: {}", results.len());
            let success_count = results.iter().filter(|r| r.status == "SUCCESS").count();
            let total_count = results.len();
            info!(
                "Export finished. {}/{} tables successful.",
                success_count, total_count
            );
            if success_count < total_count {
                error!("Some tables failed to export.");
                process::exit(1);
            }
        }
        Err(e) => {
            error!("Orchestrator failed: {:?}", e);
            process::exit(1);
        }
    }
}
