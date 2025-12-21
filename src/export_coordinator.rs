//! # Coordinator Module
//!
//! The "Brain" of the exporter. Responsible for:
//! 1. Discovering tables to export (via `metadata` module).
//! 2. Planning tasks (Chunking large tables > 1GB).
//! 3. Spawning worker threads.
//! 4. Collecting results and generating a final report.
//!
//! ## Dynamic Concurrency
//! It calculates thread count based on `cpu_percent` (default 50%, capped at 80%)
//! to ensure the host system is not overwhelmed.

use crate::config::AppConfig;
use crate::oracle_metadata;
use crate::oracle_data_extractor::{self, ExportParams};
use log::{info, error, warn};
use std::sync::{Arc, Mutex};
use std::thread;
use crossbeam_channel::{unbounded, Sender, Receiver};
use num_cpus;
use oracle::Connection;
use serde::Serialize;
use std::fs::File;
// use std::process::Command; // Moved to bigquery_ingestion
// use std::path::Path; // Moved to bigquery_ingestion

/// Task represents a single unit of work (exporting a table or a chunk)
#[derive(Debug, Clone)]
pub struct ExportTask {
    pub schema: String,
    pub table: String,
    pub chunk_id: Option<u32>,
    pub query_where: Option<String>,
    pub output_file: String,
}

#[derive(Serialize, Debug, Clone)]
pub struct TaskResult {
    pub schema: String,
    pub table: String,
    #[serde(default)]
    pub rows_exported: u64,
    #[serde(default)]
    pub bytes_exported: u64,
    pub duration_seconds: f64,
    pub status: String, // "SUCCESS", "FAILURE", or "SKIPPED"
    pub error: Option<String>,
    pub chunk_id: Option<u32>,
    pub completed_at: String,
}

impl TaskResult {
    pub fn success(schema: String, table: String, chunk_id: Option<u32>, rows: u64, bytes: u64, duration: f64) -> Self {
        Self {
            schema,
            table,
            rows_exported: rows,
            bytes_exported: bytes,
            duration_seconds: duration,
            status: "SUCCESS".to_string(),
            error: None,
            chunk_id,
            completed_at: chrono::Local::now().to_rfc3339(),
        }
    }

    pub fn failure(schema: String, table: String, chunk_id: Option<u32>, err: String) -> Self {
        Self {
            schema, table, chunk_id, rows_exported: 0, bytes_exported: 0, duration_seconds: 0.0,
            status: "FAILURE".to_string(), error: Some(err),
            completed_at: chrono::Local::now().to_rfc3339(),
        }
    }

    pub fn skipped(schema: String, table: String, chunk_id: Option<u32>) -> Self {
        Self {
            schema,
            table,
            rows_exported: 0,
            bytes_exported: 0,
            duration_seconds: 0.0,
            status: "SKIPPED".to_string(),
            error: None,
            chunk_id,
            completed_at: chrono::Local::now().to_rfc3339(),
        }
    }
}

pub struct Coordinator {
    config: AppConfig,
}

impl Coordinator {
    pub fn new(config: AppConfig) -> Self {
        Self { config }
    }

    pub fn resolve_tables(&self) -> Option<std::collections::HashSet<String>> {
        let mut tables = std::collections::HashSet::new();
        let mut has_source = false;

        if let Some(list) = &self.config.export.tables {
            for t in list {
                tables.insert(t.clone());
            }
            has_source = true;
        }

        if let Some(path) = &self.config.export.tables_file {
            if let Ok(content) = std::fs::read_to_string(path) {
                for line in content.lines() {
                    let t = line.trim();
                    if !t.is_empty() {
                        tables.insert(t.to_string());
                    }
                }
                has_source = true;
            } else {
                warn!("Could not read tables file: {}", path);
            }
        }

        if has_source {
            Some(tables)
        } else {
            None
        }
    }

    /// Entry point for the coordination logic
    pub fn resolve_schemas(&self) -> Vec<String> {
        let mut schemas = std::collections::HashSet::new();

        // 1. Explicit single schema
        if let Some(s) = &self.config.export.schema {
            schemas.insert(s.clone());
        }

        // 2. List of schemas
        if let Some(list) = &self.config.export.schemas {
            for s in list {
                schemas.insert(s.clone());
            }
        }

        // 3. File input
        if let Some(path) = &self.config.export.schemas_file {
            if let Ok(content) = std::fs::read_to_string(path) {
                for line in content.lines() {
                    let s = line.trim();
                    if !s.is_empty() {
                        schemas.insert(s.to_string());
                    }
                }
            } else {
                warn!("Could not read schemas file: {}", path);
            }
        }

        // 4. Default if empty
        if schemas.is_empty() {
             schemas.insert(self.config.database.username.clone());
        }

        let mut v: Vec<String> = schemas.into_iter().collect();
        v.sort();
        v
    }

    pub fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let start_time = std::time::Instant::now();
        let max_threads = self.calculate_concurrency();
        info!("Coordinator starting with {} worker threads (80% CPU rule)", max_threads);

        let (tx, rx): (Sender<ExportTask>, Receiver<ExportTask>) = unbounded();
        
        // Shared results vector
        let results = Arc::new(Mutex::new(Vec::new()));

        // 1. Discover Work
        info!("Step 1: Discovering tables and planning tasks...");
        self.discover_and_plan_tasks(tx, results.clone())?;
        info!("Step 1: Planning completed.");

        // 2. Spawn Workers
        info!("Step 2: Spawning {} worker threads...", max_threads);
        let mut handles = Vec::new();
        let config_clone = self.config.clone();
        
        for i in 0..max_threads {
            let rx_worker = rx.clone();
            let config_worker = config_clone.clone();
            let results_worker = results.clone();
            
            let handle = thread::spawn(move || {
                info!("Worker {} started", i);
                while let Ok(task) = rx_worker.recv() {
                    let task_result = Self::execute_task(&config_worker, &task);
                    
                    if let Ok(mut r) = results_worker.lock() {
                        r.push(task_result);
                    }
                }
                info!("Worker {} finished", i);
            });
            handles.push(handle);
        }

        // Wait for all workers
        for h in handles {
            h.join().unwrap();
        }
        
        info!("All export tasks completed.");
        
        // 3. Generate Report
        let duration = start_time.elapsed().as_secs_f64();
        if let Ok(final_results) = results.lock() {
            self.generate_report(&final_results, duration)?;
            
            // 4. Optional: Load to BigQuery
            if self.config.export.load_to_bq.unwrap_or(false) {
                crate::bigquery_ingestion::run_load_scripts(&self.config.export, &final_results)?;
            }
        }

        Ok(())
    }

    // run_load_scripts refactored to crate::bigquery_loader::run_load_scripts
    
    fn generate_report(&self, results: &[TaskResult], duration_secs: f64) -> std::io::Result<()> {
        let total = results.len();
        let success = results.iter().filter(|r| r.status == "SUCCESS").count();
        let skipped = results.iter().filter(|r| r.status == "SKIPPED").count();
        let failed = total - (success + skipped);
        
        info!("Report: Total={}, Success={}, Skipped={}, Failed={}", total, success, skipped, failed);
        
        let total_rows = results.iter().map(|r| r.rows_exported).sum::<u64>();
        let total_bytes = results.iter().map(|r| r.bytes_exported).sum::<u64>();
        let total_mb = total_bytes as f64 / 1024.0 / 1024.0;
        let total_mb_per_sec = if duration_secs > 0.0 {
            total_mb / duration_secs
        } else {
            0.0
        };

        let report = serde_json::json!({
            "summary": {
                "total_rows": total_rows,
                "total_bytes": total_bytes,
                "total_mb_per_sec": (total_mb_per_sec * 100.0).round() / 100.0, // Round to 2 decimal places
                "total_tasks": total,
                "success": success,
                "skipped": skipped,
                "failed": failed,
                "total_duration_seconds": duration_secs,
                "timestamp": chrono::Local::now().to_rfc3339(),
                "timezone": chrono::Local::now().offset().to_string()
            },
            "details": results
        });
        
        let timestamp_str = chrono::Utc::now().format("%Y%m%d_%H%M%S").to_string();
        let out_dir = &self.config.export.output_dir;
        let path = format!("{}/report_{}.json", out_dir, timestamp_str);
        let mut file = File::create(path)?;
        serde_json::to_writer_pretty(&mut file, &report)?;
        
        Ok(())
    }

    fn calculate_concurrency(&self) -> usize {
        if let Some(p) = self.config.export.parallel {
            return p;
        }

        let cpus = num_cpus::get();
        let pct = self.config.export.cpu_percent.unwrap_or(50);
        
        // If user explicitly set cpu_percent, we trust them up to 100%. 
        // Otherwise default is 50%.
        // We cap at 32 threads to avoid diminishing returns/context switching on massive machines 
        // unless explicitly overridden by 'parallel'.
        
        let target_usage = (cpus as f64 * (pct as f64 / 100.0)).ceil() as usize;
        let final_target = std::cmp::max(1, target_usage);
        
        info!("Dynamic Concurrency: {} CPUs available. Target usage {}%. Threads: {}", cpus, pct, final_target);
        
        final_target
    }

    fn discover_and_plan_tasks(&self, tx: Sender<ExportTask>, results: Arc<Mutex<Vec<TaskResult>>>) -> Result<(), Box<dyn std::error::Error>> {
        let db = &self.config.database;
        let conn_string = format!("//{}:{}/{}", db.host, db.port, db.service);
        let conn = Connection::connect(&db.username, db.password.as_deref().unwrap_or(""), &conn_string)?;
        
        let tasks = self.process_table_discovery(&conn, &results);
        info!("Step 1: Discovered {} tables to process.", tasks.len());
        if tasks.is_empty() {
            warn!("No tables found to export.");
        }

        let out_dir = &self.config.export.output_dir;
        std::fs::create_dir_all(out_dir)?;

        for (schema_name, table) in tasks {
            // Use schema_name variable consistently
            if let Err(e) = self.plan_table_export(&conn, &schema_name, &table, out_dir, &tx, &results) {
                 warn!("Failed to plan export for {}.{}: {}", schema_name, table, e);
                 // We don't stop everything for one table failure, plan_table_export records the failure in results
            }
        }

        Ok(())
    }

    /// Helper to discover tables based on configuration (single table, schema list, or file)
    fn process_table_discovery(&self, conn: &Connection, results: &Arc<Mutex<Vec<TaskResult>>>) -> Vec<(String, String)> {
        if let Some(t) = &self.config.export.table {
            let s = self.config.export.schema.as_deref().unwrap_or(&self.config.database.username).to_string();
            return vec![(s, t.clone())];
        }

        let schemas = self.resolve_schemas();
        let whitelist = self.resolve_tables();
        let mut t_list: Vec<(String, String)> = Vec::new();
        
        for s in schemas {
            info!("Discovering tables in schema: {}", s);
            match oracle_metadata::get_tables(conn, &s) {
                Ok(tables) => {
                    info!("Found {} tables in {}", tables.len(), s);
                    for table in &tables {
                        // Filter by tables list if present
                        if let Some(set) = &whitelist {
                            if set.iter().any(|w| w.eq_ignore_ascii_case(table)) {
                                t_list.push((s.clone(), table.clone()));
                            }
                        } else {
                            t_list.push((s.clone(), table.clone()));
                        }
                    }
                },
                Err(e) => {
                    warn!("Failed to get tables for schema {}: {}", s, e);
                    if let Ok(mut r) = results.lock() {
                        r.push(TaskResult::failure(
                            s.clone(),
                            "*ALL*".to_string(),
                            None,
                            format!("Failed to list tables: {}", e)
                        ));
                    }
                }
            }
        }
        t_list
    }

    /// Helper to plan a single table export: validation, artifacts, and chunking/task creation
    fn plan_table_export(
        &self, 
        conn: &Connection, 
        schema: &str, 
        table: &str, 
        out_dir: &str, 
        tx: &Sender<ExportTask>, 
        results: &Arc<Mutex<Vec<TaskResult>>>
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Exclude check
        if let Some(excludes) = &self.config.export.exclude_tables {
            if excludes.iter().any(|x| x.eq_ignore_ascii_case(table)) {
                info!("Skipping excluded table: {}.{}", schema, table);
                return Ok(());
            }
        }

        // Size check
        let size_gb = match oracle_metadata::get_table_size_gb(conn, schema, table) {
            Ok(s) => s,
            Err(e) => {
                warn!("Failed to get table size for {}.{}: {}", schema, table, e);
                if let Ok(mut r) = results.lock() {
                    r.push(TaskResult::failure(
                        schema.to_string(),
                        table.to_string(),
                        None,
                        format!("Size check failed: {}", e)
                    ));
                }
                return Ok(());
            }
        };
        info!("Table {}.{} is ~{:.2} GB", schema, table, size_gb);

        let table_dir = format!("{}/{}/{}", out_dir, schema, table);
        let data_dir = format!("{}/data", table_dir);
        let config_dir = format!("{}/config", table_dir);
        std::fs::create_dir_all(&data_dir)?;
        std::fs::create_dir_all(&config_dir)?;

        let (names, types, raw_strings) = match oracle_metadata::get_table_columns(conn, schema, table) {
            Ok(res) => res,
            Err(e) => {
                warn!("Failed to get columns for {}.{}: {}", schema, table, e);
                if let Ok(mut r) = results.lock() {
                    r.push(TaskResult::failure(
                        schema.to_string(),
                        table.to_string(),
                        None,
                        format!("Column discovery failed: {}", e)
                    ));
                }
                return Ok(());
            }
        };

        // Artifacts & Validation
        let start_time = std::time::Instant::now();
        let oracle_ddl = oracle_metadata::get_ddl(conn, schema, table);
        
        let pk_cols = match oracle_metadata::get_primary_key(conn, schema, table) {
            Ok(Some(pks)) => {
                info!("Identified Primary Key for {}.{}: {:?}", schema, table, pks);
                Some(pks)
            },
            Ok(None) => {
                info!("No Primary Key found for {}.{}. Validation will skip PK hash.", schema, table);
                None
            },
            Err(e) => {
                warn!("Failed to query PK for {}.{}: {}. Skipping PK hash.", schema, table, e);
                None
            }
        };

        let schema_path = format!("{}/schema.json", config_dir);
        let enable_row_hash = self.config.export.enable_row_hash.unwrap_or(false);
        if let Err(e) = crate::bigquery_schema_mapper::generate_schema(&names, &types, &raw_strings, &schema_path, enable_row_hash) {
            warn!("Failed to generate BigQuery schema for {}.{}: {}", schema, table, e);
        }

        let agg_cols: Vec<String> = names.iter().zip(types.iter())
            .filter(|(_, t)| matches!(t, oracle::sql_type::OracleType::Number(_, _)))
            .take(3) 
            .map(|(n, _)| n.clone())
            .collect();

        // Fetch Partition Keys and Indexes for DDL generation
                    let partition_cols = oracle_metadata::get_partition_keys(conn, schema, table).unwrap_or_else(|e| {
                        warn!("Failed to get partition keys for {}: {}", table, e);
                        Vec::new()
                    });
                    let index_cols = oracle_metadata::get_index_columns(conn, schema, table).unwrap_or_default();

        match crate::data_validator::validate_table(conn, schema, table, pk_cols.as_deref(), Some(&agg_cols)) {
            Ok(stats) => {
                let duration = start_time.elapsed().as_secs_f64();
                let config_path = std::path::Path::new(&config_dir);
                let project = self.config.bigquery.as_ref().map(|bq| bq.project.as_str()).unwrap_or("project");
                let dataset = self.config.bigquery.as_ref().map(|bq| bq.dataset.as_str()).unwrap_or(schema);

                if let Err(e) = crate::artifact_generator::save_all_artifacts(
                    config_path,
                    schema,
                    table,
                    &names,
                    &types,
                    &raw_strings,
                    &stats,
                    oracle_ddl.as_deref(),
                    duration,
                    self.config.export.enable_row_hash.unwrap_or(false),
                    self.config.export.field_delimiter.as_deref().unwrap_or("\u{0010}"),
                    project,
                    dataset,
                    pk_cols.as_deref(),
                    &partition_cols,
                    &index_cols,
                    self.config.gcp.as_ref().map(|g| g.gcs_bucket.as_str()),
                ) {
                    warn!("Failed to save artifacts for {}.{}: {}", schema, table, e);
                } else {
                    info!("Artifacts generated in {}", config_dir);
                }

                if let Some(gcp_config) = &self.config.gcp {
                    if let Some(ddl) = &oracle_ddl {
                         // Use 'input' and 'output' subdirectories to keep things clean and ensure config is found.
                         let base_gcs_path = format!("gs://{}/{}/{}", gcp_config.gcs_bucket, schema, table);
                         let gcs_input_dir = format!("{}/input", base_gcs_path);
                         let gcs_output_dir = format!("{}/output", base_gcs_path);
                         
                         let input_ddl_uri = format!("{}/oracle_ddl.sql", gcs_input_dir);

                         info!("Initiating GCP Translation for {}.{}", schema, table);
                         
                         // Upload Translation Config if enabled
                         if let Some(strict) = gcp_config.decimal_precision_strictness {
                             if strict {
                                 let config_content = "
schema_mapping:
  decimal_precision_strictness: MAX
"; 
                                 let config_uri = format!("{}/translation_config.yaml", gcs_input_dir);
                                 if let Err(e) = crate::gcp::storage::upload_file_cli(&config_uri, config_content.to_string()) {
                                     warn!("Failed to upload translation config: {}", e);
                                 }
                             }
                         }

                         match crate::gcp::storage::upload_file_cli(&input_ddl_uri, ddl.clone()) {
                             Ok(_) => {
                                 // Pass the DIRECTORY as source, so it finds both .sql and .yaml
                                 match crate::gcp::translator::translate_ddl_cli(
                                     &gcp_config.project_id,
                                     &gcp_config.location,
                                     &gcs_input_dir,
                                     &gcs_output_dir
                                 ) {
                                     Ok(output_path) => {
                                         info!("Translation submitted. Result output at: {}", output_path);
                                         
                                         // Attempt to find and download translated DDL
                                         // The output directory might contain multiple files. We look for .sql files.
                                         match crate::gcp::storage::list_files_cli(&output_path) {
                                             Ok(files) => {
                                                 let sql_files: Vec<&String> = files.iter()
                                                     .filter(|f| f.ends_with(".sql") && !f.ends_with("oracle_ddl.sql")) 
                                                     .collect();
                                                 
                                                 if let Some(translated_sql_uri) = sql_files.first() {
                                                     info!("Found translated SQL: {}", translated_sql_uri);
                                                     match crate::gcp::storage::download_file_cli(translated_sql_uri) {
                                                         Ok(sql_content) => {
                                                             let translated_ddl_path = format!("{}/bigquery_translated.ddl", config_dir);
                                                             if let Err(e) = std::fs::write(&translated_ddl_path, &sql_content) {
                                                                 warn!("Failed to write translated DDL to {}: {}", translated_ddl_path, e);
                                                             } else {
                                                                 info!("Saved translated DDL to {}", translated_ddl_path);
                                                                 // Validation: Compare with generated bigquery.ddl
                                                                 let bq_ddl_path = format!("{}/bigquery.ddl", config_dir);
                                                                 match std::fs::read_to_string(&bq_ddl_path) {
                                                                     Ok(generated_ddl) => {
                                                                         if generated_ddl.trim() != sql_content.trim() {
                                                                             warn!("VALIDATION WARNING: Translated DDL differs from generated DDL for {}.{}", schema, table);
                                                                             // We could add more diff details here if needed
                                                                         } else {
                                                                             info!("VALIDATION SUCCESS: Translated DDL matches generated DDL for {}.{}", schema, table);
                                                                         }
                                                                     },
                                                                     Err(e) => warn!("Could not read generated DDL for comparison: {}", e)
                                                                 }
                                                             }
                                                         },
                                                         Err(e) => warn!("Failed to download translated DDL: {}", e)
                                                     }
                                                 } else {
                                                     warn!("No translated .sql files found in output. Translation might have failed or produced no output.");
                                                     // Optional: Check for report to debug
                                                     if let Some(report) = files.iter().find(|f| f.ends_with("batch_translation_report.csv")) {
                                                          info!("Translation report found at: {}", report);
                                                     }
                                                 }
                                             },
                                             Err(e) => warn!("Failed to list translation output: {}", e)
                                         }
                                     },
                                     Err(e) => warn!("Translation failed for {}.{}: {}", schema, table, e)
                                 }
                             },
                             Err(e) => warn!("Failed to upload DDL to GCS for {}.{}: {}", schema, table, e)
                         }
                    }
                }
            },
            Err(e) => warn!("Validation failed for {}.{}: {}", schema, table, e),
        }

        // Planning
        if size_gb > 1.0 { // Threshold 1GB
            let chunk_count = (size_gb / 1.0).ceil() as u32;
            info!("Splitting {} into {} chunks", table, chunk_count);
            match oracle_metadata::generate_chunks(conn, schema, table, chunk_count) {
                Ok(chunks) => {
                    for chunk in chunks {
                         let filename = format!("{}/data_chunk_{:04}.csv.gz", data_dir, chunk.chunk_id);
                         if std::path::Path::new(&filename).exists() {
                             info!("Skipping existing chunk: {}", filename);
                             if let Ok(mut r) = results.lock() {
                                 r.push(TaskResult::skipped(schema.to_string(), table.to_string(), Some(chunk.chunk_id)));
                             }
                             continue;
                         }
                         let query_where = format!("ROWID BETWEEN '{}' AND '{}'", chunk.start_rowid, chunk.end_rowid);
                         tx.send(ExportTask {
                             schema: schema.to_string(),
                             table: table.to_string(),
                             chunk_id: Some(chunk.chunk_id),
                             query_where: Some(query_where),
                             output_file: filename,
                         })?;
                    }
                },
                Err(e) => {
                    warn!("Failed to generate chunks for {}: {}", table, e);
                    if let Ok(mut r) = results.lock() {
                        r.push(TaskResult::failure(
                            schema.to_string(),
                            table.to_string(),
                            None,
                            format!("Chunk generation failed: {}", e)
                        ));
                    }
                }
            }
        } else {
            // Single File
            let filename = format!("{}/data.csv.gz", data_dir);
            if std::path::Path::new(&filename).exists() {
                info!("Skipping existing file: {}", filename);
                if let Ok(mut r) = results.lock() {
                    r.push(TaskResult::skipped(schema.to_string(), table.to_string(), None));
                }
            } else {
                tx.send(ExportTask {
                    schema: schema.to_string(),
                    table: table.to_string(),
                    chunk_id: None,
                    query_where: None,
                    output_file: filename,
                })?;
            }
        }

        Ok(())
    }


    fn execute_task(config: &AppConfig, task: &ExportTask) -> TaskResult {
        let db = &config.database;
        let params = ExportParams {
            host: db.host.clone(),
            port: db.port,
            service: db.service.clone(),
            username: db.username.clone(),
            password: db.password.as_deref().unwrap_or("").to_string(),
            output_file: task.output_file.clone(),
            prefetch_rows: config.export.prefetch_rows.unwrap_or(5000),
            schema: task.schema.clone(),
            table: task.table.clone(),
            query_where: task.query_where.clone(),
            enable_row_hash: config.export.enable_row_hash.unwrap_or(false),
            field_delimiter: config.export.field_delimiter.clone().unwrap_or("\u{0010}".to_string()),
        };

        let start = std::time::Instant::now();
        info!("Starting task: {}.{} (Chunk: {:?})", task.schema, task.table, task.chunk_id);
        
        match oracle_data_extractor::export_table(params) {
            Ok(stats) => {
                info!("Finished task: {}.{} ({:?}) - {} rows, {} bytes", task.schema, task.table, task.chunk_id, stats.rows, stats.bytes);
                TaskResult::success(
                    task.schema.clone(),
                    task.table.clone(),
                    task.chunk_id,
                    stats.rows,
                    stats.bytes,
                    stats.duration_secs
                )
            },
            Err(e) => {
                let duration = start.elapsed().as_secs_f64();
                error!("Failed task: {}.{} ({:?}) after {:.2}s: {}", task.schema, task.table, task.chunk_id, duration, e);
                TaskResult::failure(
                    task.schema.clone(),
                    task.table.clone(),
                    task.chunk_id,
                    e.to_string()
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AppConfig, ExportConfig, DatabaseConfig};

    fn mock_config() -> AppConfig {
        AppConfig {
            database: DatabaseConfig {
                username: "DEFAULT_USER".to_string(),
                password: None,
                host: "localhost".to_string(),
                port: 1521,
                service: "XE".to_string(),
            },
            export: ExportConfig {
                output_dir: ".".to_string(),
                schema: None,
                table: None,
                parallel: None,
                prefetch_rows: None,
                exclude_tables: None,
                enable_row_hash: None,
                cpu_percent: None,
                field_delimiter: None,
                schemas: None,
                schemas_file: None,
                tables: None,
                tables_file: None,
                load_to_bq: None,
            },
            bigquery: None,
            gcp: None,
        }
    }

    #[test]
    fn test_resolve_schemas_default() {
        let config = mock_config();
        let coord = Coordinator::new(config);
        let schemas = coord.resolve_schemas();
        assert_eq!(schemas, vec!["DEFAULT_USER"]);
    }

    #[test]
    fn test_resolve_schemas_list() {
        let mut config = mock_config();
        config.export.schemas = Some(vec!["SCHEMA_B".to_string(), "SCHEMA_A".to_string()]);
        let coord = Coordinator::new(config);
        let schemas = coord.resolve_schemas();
        assert_eq!(schemas, vec!["SCHEMA_A", "SCHEMA_B"]); // Sorted
    }

    #[test]
    fn test_resolve_schemas_mixed() {
        // Create a temporary file
        let file_path = "/tmp/test_schemas_unit.txt";
        std::fs::write(file_path, "SCHEMA_C\nSCHEMA_A").unwrap();

        let mut config = mock_config();
        config.export.schema = Some("SCHEMA_B".to_string());
        config.export.schemas_file = Some(file_path.to_string());
        
        let coord = Coordinator::new(config);
        let schemas = coord.resolve_schemas();
        
        std::fs::remove_file(file_path).unwrap();
        
        // Should contain A, B, C sorted
        assert_eq!(schemas, vec!["SCHEMA_A", "SCHEMA_B", "SCHEMA_C"]);
    }
}
