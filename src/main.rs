//! # Oracle Data Exporter for BigQuery (Rust)
//!
//! A high-performance, multi-threaded utility designed to migrate large-scale
//! data from Oracle databases to BigQuery-ready compressed CSV files.
//!
//! This application follows the **Hexagonal Architecture** (Ports and Adapters)
//! to maintain a strict separation between business logic and infrastructure.

pub mod application;
pub mod config;
pub mod domain;
pub mod infrastructure;
pub mod ports;

use crate::domain::export_models::FileFormat;

use crate::application::export_orchestrator::ExportOrchestrator;
use crate::config::{AppConfig, CliArgs};
use crate::infrastructure::local_storage::local_artifact_adapter::LocalArtifactAdapter;
use crate::infrastructure::oracle::oracle_extraction_adapter::OracleExtractionAdapter;
use crate::infrastructure::oracle::oracle_metadata_adapter::OracleMetadataAdapter;
use clap::Parser;
use log::{error, info};
use std::process;
use std::sync::Arc;

fn main() {
    // 1. Initialize Logging
    env_logger::init();

    // 2. Parse Arguments
    let args = CliArgs::parse();

    // 3. Load Config
    let mut config = if let Some(config_path) = &args.config {
        match AppConfig::from_file(config_path) {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to load config: {}", e);
                process::exit(1);
            }
        }
    } else {
        // Construct default config from CLI if no config file
        AppConfig::default_from_cli(&args)
    };

    // Merge CLI overrides
    config.merge_cli(&args);

    if let Err(e) = config.validate() {
        error!("Invalid configuration: {}", e);
        process::exit(1);
    }

    // 3.5 Setup Parallelism
    let cpu_percent = config.export.cpu_percent.unwrap_or(50);
    let total_cpus = num_cpus::get();
    let num_threads = if let Some(p) = config.export.parallel {
        p
    } else {
        (total_cpus as f64 * (cpu_percent as f64 / 100.0)).ceil() as usize
    };
    let num_threads = std::cmp::max(1, num_threads);
    info!(
        "Initializing worker pool with {} threads (Target CPU: {}%)",
        num_threads, cpu_percent
    );

    rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build_global()
        .unwrap_or_else(|e| {
            info!("Global thread pool already initialized: {}", e);
        });

    // 4. Initialize Hexagonal Components
    let conn_str = config.database.get_connection_string();
    let password = config
        .database
        .password
        .clone()
        .or_else(|| std::env::var("ORACLE_PASSWORD").ok())
        .unwrap_or_default();

    let schema_reader = Arc::new(OracleMetadataAdapter::new(
        conn_str.clone(),
        config.database.username.clone(),
        password.clone(),
    ));

    let prefetch = config.export.prefetch_rows.unwrap_or(5000);
    let delimiter_str = config
        .export
        .field_delimiter
        .clone()
        .unwrap_or_else(|| "\u{0010}".to_string());
    let delimiter = delimiter_str.as_bytes()[0];

    let data_streamer = Arc::new(OracleExtractionAdapter::new(
        conn_str,
        config.database.username.clone(),
        password.clone(),
        prefetch,
        delimiter,
    ));

    // For local artifacts, we need project/dataset
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

    let artifact_writer = Arc::new(LocalArtifactAdapter::new(project_id, dataset_id));

    // 5. Run Orchestrator
    let orchestrator =
        ExportOrchestrator::new(schema_reader, data_streamer, artifact_writer, config);

    info!("Starting Export process...");
    match orchestrator.run() {
        Ok(results) => {
            let success_count = results.iter().filter(|r| r.status == "SUCCESS").count();
            info!(
                "Export finished. {}/{} tables successful.",
                success_count,
                results.len()
            );
        }
        Err(e) => {
            error!("Orchestrator failed: {:?}", e);
            process::exit(1);
        }
    }
}

// Add a helper to AppConfig to create a default from CLI
impl AppConfig {
    fn default_from_cli(args: &CliArgs) -> Self {
        Self {
            database: crate::config::DatabaseConfig {
                username: args.username.clone().unwrap_or_default(),
                password: args.password.clone(),
                host: args.host.clone().unwrap_or_default(),
                port: args.port.unwrap_or(1521),
                service: args.service.clone().unwrap_or_default(),
                connection_string: None,
            },
            export: crate::config::ExportConfig {
                output_dir: args.output.clone().unwrap_or_else(|| ".".to_string()),
                schema: None,
                table: args.table.clone(),
                parallel: args.parallel,
                prefetch_rows: Some(5000),
                exclude_tables: None,
                enable_row_hash: None,
                cpu_percent: args.cpu_percent,
                field_delimiter: None,
                schemas: None,
                schemas_file: None,
                tables: None,
                tables_file: None,
                load_to_bq: Some(args.load),
                use_client_hash: None,
                adaptive_parallelism: None,
                target_throughput_per_core: None,
                file_format: args
                    .format
                    .as_ref()
                    .map(|f| match f.to_lowercase().as_str() {
                        "csv" => FileFormat::Csv,
                        "parquet" => FileFormat::Parquet,
                        _ => FileFormat::Csv,
                    }),
                parquet_compression: args.compression.clone(),
            },
            bigquery: None,
            gcp: None,
        }
    }
}
