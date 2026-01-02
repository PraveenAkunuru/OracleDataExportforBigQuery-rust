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


use crate::application::orchestrator::Orchestrator;
use crate::config::{AppConfig, CliArgs};
use crate::infrastructure::artifacts::artifact_adapter::ArtifactAdapter;
use crate::infrastructure::oracle::extractor::Extractor;
use crate::infrastructure::oracle::metadata::MetadataAdapter;
use clap::Parser;
use log::{error, info};
use r2d2::Pool;
use crate::infrastructure::oracle::connection_manager::OracleConnectionManager;
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

    // 4. Initialize Connection Pool
    let conn_str = config.database.get_connection_string();
    let password = config
        .database
        .password
        .clone()
        .or_else(|| std::env::var("ORACLE_PASSWORD").ok())
        .unwrap_or_default();

    info!("Initializing connection pool for {}...", conn_str);
    let manager = OracleConnectionManager::new(
        &config.database.username,
        &password,
        &conn_str,
    );
    
    // Pool size = num_threads + 2 (buffer for metadata queries)
    let pool_size = (num_threads + 2) as u32;
    let pool = Arc::new(Pool::builder()
        .max_size(pool_size)
        .build(manager)
        .unwrap_or_else(|e| {
            error!("Failed to create connection pool: {}", e);
            process::exit(1);
        }));

    // 5. Initialize Hexagonal Components
    let metadata_port = Arc::new(MetadataAdapter::new(pool.clone()));

    let prefetch = config.export.prefetch_rows.unwrap_or(5000);
    let delimiter_str = config
        .export
        .field_delimiter
        .clone()
        .unwrap_or_else(|| "\u{0010}".to_string());
    let field_delimiter = delimiter_str.as_bytes()[0];

    let extraction_port = Arc::new(Extractor::new(
        pool,
        prefetch,
        field_delimiter,
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

    let artifact_port = Arc::new(ArtifactAdapter::new(project_id, dataset_id));

    // 5. Run Orchestrator
    let orchestrator =
        Orchestrator::new(metadata_port, extraction_port, artifact_port, config);

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
