//! # Oracle Data Exporter for BigQuery (Rust)
//!
//! This is the main entry point for the application.
//! It handles argument parsing, configuration loading, and mode selection (Coordinator vs Worker).
//!
//! ## Modes
//! - **Coordinator Mode**: Activated via `--config` or when an output directory is specified.
//!   Manages discovery, task planning, and worker thread orchestration.
//! - **Ad-hoc Worker Mode**: Activated via `--table` and `--output` (file path).
//!   Performs a direct single-threaded export of a specific table or chunk.
//!
//! ## Modules
//! - `config`: Configuration structs and validation logic.
//! - `coordinator`: Discovery and thread pool management.
//! - `exporter`: Core Oracle-to-CSV export logic.
//! - `metadata`: Database metadata queries (schema, size, chunks).
//! - `validation`: Data validation logic (row counts, checksums).
//! - `artifacts`: Generators for DDL, JSON schemas, and SQL scripts.
//! - `sql_utils`: Centralized SQL generation utilities (hashing, parity).
//! - `bigquery`: BigQuery type mapping and schema generation.

// MODULES
mod exporter;
mod config;
mod metadata;
mod coordinator;
mod bigquery;
mod validation;
mod artifacts;
mod sql_utils;

use clap::Parser;
use log::{info, error};
use std::process;
use crate::config::{AppConfig, CliArgs};
use crate::coordinator::Coordinator;
use crate::exporter::ExportParams;

fn main() {
    // 1. Initialize Logging
    env_logger::init();
    
    // 2. Parse Arguments
    let args = CliArgs::parse();

    // 3. Determine Mode
    //    If --config is passed, or we look like we want full coordination (output is dir, schema specified, etc.)
    //    If --output ends with .csv.gz, unlikely to be coordination dir.
    
    // Priority:
    // A. Config File -> Coordinator
    // B. Explicit Table + File Output -> Ad-hoc Worker (Old behavior)
    // C. Schema/Table + Dir Output -> Coordinator (Ad-hoc)

    if let Some(config_path) = &args.config {
        // COORDINATOR MODE (via Config File)
        info!("Running in Coordinator Mode with config: {}", config_path);
        let mut config = match AppConfig::from_file(config_path) {
            Ok(c) => c,
            Err(e) => {
                error!("Failed to load config: {}", e);
                process::exit(1);
            }
        };
        // Allow CLI override
        config.merge_cli(&args);
        
        if let Err(e) = config.validate() {
            error!("Invalid configuration: {}", e);
            process::exit(1);
        }
        
        let coord = Coordinator::new(config);
        if let Err(e) = coord.run() {
            error!("Coordinator failed: {}", e);
            process::exit(1);
        }
    } else {
        // NO Config File. Check CLI args.
        // If we have table + output file -> Worker Mode
        if let (Some(table), Some(output)) = (&args.table, &args.output) {
             if output.ends_with(".csv.gz") || output.ends_with(".csv") {
                 // WORKER MODE
                 info!("Running in Ad-hoc Worker Mode for table: {}", table);
                 
                 // We need creds
                 if args.username.is_none() || args.host.is_none() || args.service.is_none() {
                     error!("Missing required DB args (username, host, service) for ad-hoc mode");
                     process::exit(1);
                 }
                 
                 // Handle password safely
                 let password = args.password.clone().or_else(|| std::env::var("ORACLE_PASSWORD").ok()).unwrap_or_default();
                 if password.is_empty() {
                     // In real app, prompt. In Docker, fail.
                     error!("Password required (CLI or ORACLE_PASSWORD env)");
                     process::exit(1);
                 }
                 
                 let (schema, table_only) = if table.contains('.') {
                     let parts: Vec<&str> = table.split('.').collect();
                     (parts[0].to_uppercase(), parts[1].to_uppercase())
                 } else {
                     (args.username.as_ref().unwrap().to_uppercase(), table.to_uppercase())
                 };
                 
                 let params = ExportParams {
                     host: args.host.as_ref().unwrap().clone(),
                     port: args.port.unwrap_or(1521),
                     service: args.service.as_ref().unwrap().clone(),
                     username: args.username.as_ref().unwrap().clone(),
                     password,
                     output_file: output.clone(),
                     prefetch_rows: 5000,
                     schema,
                     table: table_only,
                     query_where: args.query_where.clone(),
                     enable_row_hash: false,
                     field_delimiter: "\u{0010}".to_string(),
                 };
                 
                 if let Err(e) = crate::exporter::export_table(params) {
                     error!("Export failed: {}", e);
                     process::exit(1);
                 }
                 
             } else {
                 // Output is likely a Directory? -> Ad-hoc Coordinator
                 // Implementation optional for now. Let's redirect to usage help or minimal coordinator.
                 info!("Output does not look like a file. Assuming Coordinator Mode.");
                 
                 // Construct minimal config
                 if args.username.is_none() || args.host.is_none() {
                     error!("Missing required DB args");
                     process::exit(1);
                 }
                 
                 let password = args.password.clone().or_else(|| std::env::var("ORACLE_PASSWORD").ok()).unwrap_or_default();
                 
                 let config = AppConfig {
                     database: crate::config::DatabaseConfig {
                         username: args.username.as_ref().unwrap().clone(),
                         password: Some(password),
                         host: args.host.as_ref().unwrap().clone(),
                         port: args.port.unwrap_or(1521),
                         service: args.service.as_ref().unwrap_or(&"FREEPDB1".to_string()).clone(),
                     },
                     export: crate::config::ExportConfig {
                         output_dir: output.clone(),
                         schema: None, // Will use DB user
                         table: Some(table.clone()),
                         parallel: None,
                         prefetch_rows: Some(5000),
                         exclude_tables: None,
                         enable_row_hash: None,
                         cpu_percent: None,
                         field_delimiter: None,
                         schemas: None,
                         schemas_file: None,
                         tables: None,
                         tables_file: None,
                     },
                     bigquery: None,
                 };
                 
                 let coord = Coordinator::new(config);
                 if let Err(e) = coord.run() {
                     error!("Coordinator failed: {}", e);
                     process::exit(1);
                 }
             }
        } else {
            error!("Invalid mode. Please provide --config OR --table and --output.");
            use clap::CommandFactory;
            let _ = CliArgs::command().print_help();
            process::exit(1);
        }
    }
}
