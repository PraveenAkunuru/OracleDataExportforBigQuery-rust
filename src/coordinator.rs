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
use crate::metadata;
use crate::exporter::{self, ExportParams};
use log::{info, error, warn};
use std::sync::{Arc, Mutex};
use std::thread;
use crossbeam_channel::{unbounded, Sender, Receiver};
use num_cpus;
use oracle::Connection;
use serde::Serialize;
use std::fs::File;

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
    pub status: String, // "SUCCESS" or "FAILURE"
    pub error: Option<String>,
    pub chunk_id: Option<u32>,
}

pub struct Coordinator {
    config: AppConfig,
}

impl Coordinator {
    pub fn new(config: AppConfig) -> Self {
        Self { config }
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
        self.discover_and_plan_tasks(tx, results.clone())?;

        // 2. Spawn Workers
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
        }

        Ok(())
    }
    
    fn generate_report(&self, results: &[TaskResult], duration_secs: f64) -> std::io::Result<()> {
        let total = results.len();
        let success = results.iter().filter(|r| r.status == "SUCCESS").count();
        let skipped = results.iter().filter(|r| r.status == "SKIPPED").count();
        let failed = total - (success + skipped);
        
        info!("Report: Total={}, Success={}, Skipped={}, Failed={}", total, success, skipped, failed);
        
        let report = serde_json::json!({
            "summary": {
                "total_rows": results.iter().map(|r| r.rows_exported).sum::<u64>(),
                "total_bytes": results.iter().map(|r| r.bytes_exported).sum::<u64>(),
                "total_tasks": total,
                "success": success,
                "skipped": skipped,
                "failed": failed,
                "total_duration_seconds": duration_secs,
                "timestamp": chrono::Utc::now().to_rfc3339()
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
        let pct_clamped = std::cmp::min(pct, 80);
        
        let target = (cpus as f64 * (pct_clamped as f64 / 100.0)) as usize;
        let final_target = std::cmp::max(1, target);
        
        info!("Dynamic Concurrency: {} CPUs available. Target usage {}%. Threads: {}", cpus, pct_clamped, final_target);
        
        final_target
    }

    fn discover_and_plan_tasks(&self, tx: Sender<ExportTask>, results: Arc<Mutex<Vec<TaskResult>>>) -> Result<(), Box<dyn std::error::Error>> {
        let db = &self.config.database;
        let conn_string = format!("//{}:{}/{}", db.host, db.port, db.service);
        let conn = Connection::connect(&db.username, db.password.as_deref().unwrap_or(""), &conn_string)?;
        
        let tasks: Vec<(String, String)> = if let Some(t) = &self.config.export.table {
            let s = self.config.export.schema.as_deref().unwrap_or(&db.username).to_string();
            vec![(s, t.clone())]
        } else {
            let schemas = self.resolve_schemas();
            let mut t_list = Vec::new();
            
            for s in schemas {
                info!("Discovering tables in schema: {}", s);
                match metadata::get_tables(&conn, &s) {
                    Ok(tables) => {
                        info!("Found {} tables in {}", tables.len(), s);
                        for table in tables {
                            t_list.push((s.clone(), table));
                        }
                    },
                    Err(e) => {
                        warn!("Failed to get tables for schema {}: {}", s, e);
                        if let Ok(mut r) = results.lock() {
                            r.push(TaskResult {
                                schema: s.clone(),
                                table: "*ALL*".to_string(), 
                                chunk_id: None,
                                status: "FAILURE".to_string(),
                                error: Some(format!("Failed to list tables: {}", e)),
                                duration_seconds: 0.0,
                                rows_exported: 0,
                                bytes_exported: 0,
                            });
                        }
                    }
                }
            }
            t_list
        };

        if tasks.is_empty() {
            warn!("No tables found to export.");
        }

        let out_dir = &self.config.export.output_dir;
        std::fs::create_dir_all(out_dir)?;

        for (schema_name, table) in tasks {
            // Use schema_name variable consistently
            let schema = &schema_name;
            
            if let Some(excludes) = &self.config.export.exclude_tables {
                if excludes.iter().any(|x| x.eq_ignore_ascii_case(&table)) {
                    info!("Skipping excluded table: {}.{}", schema, table);
                    continue;
                }
            }

            let size_gb = match metadata::get_table_size_gb(&conn, schema, &table) {
                Ok(s) => s,
                Err(e) => {
                    warn!("Failed to get table size for {}.{}: {}", schema, table, e);
                    if let Ok(mut r) = results.lock() {
                        r.push(TaskResult {
                            schema: schema.to_string(),
                            table: table.clone(),
                            chunk_id: None,
                            status: "FAILURE".to_string(),
                            error: Some(format!("Size check failed: {}", e)),
                            duration_seconds: 0.0,
                            rows_exported: 0,
                            bytes_exported: 0,
                        });
                    }
                    continue;
                }
            };
            info!("Table {}.{} is ~{:.2} GB", schema, table, size_gb);

            let table_dir = format!("{}/{}/{}", out_dir, schema, table);
            let data_dir = format!("{}/data", table_dir);
            let config_dir = format!("{}/config", table_dir);
            std::fs::create_dir_all(&data_dir)?;
            std::fs::create_dir_all(&config_dir)?;

            match metadata::get_table_columns(&conn, schema, &table) {
                Ok((names, types)) => {
                    let start_time = std::time::Instant::now();
                    let oracle_ddl = metadata::get_ddl(&conn, schema, &table);
                    
                    // Identify PK
                    let pk_cols = match metadata::get_primary_key(&conn, schema, &table) {
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

                    let agg_cols: Vec<String> = names.iter().zip(types.iter())
                        .filter(|(_, t)| matches!(t, oracle::sql_type::OracleType::Number(_, _)))
                        .take(3) 
                        .map(|(n, _)| n.clone())
                        .collect();

                    // Validate
                    match crate::validation::validate_table(&conn, schema, &table, pk_cols.as_deref(), Some(&agg_cols)) {
                        Ok(stats) => {
                            let duration = start_time.elapsed().as_secs_f64();
                            let config_path = std::path::Path::new(&config_dir);
                            if let Err(e) = crate::artifacts::save_all_artifacts(
                                config_path,
                                schema,
                                &table,
                                &names,
                                &types,
                                &stats,
                                oracle_ddl.as_deref(),
                                duration,
                                self.config.export.enable_row_hash.unwrap_or(false),
                                self.config.export.field_delimiter.as_deref().unwrap_or("\u{0010}")
                            ) {
                                warn!("Failed to save artifacts for {}.{}: {}", schema, table, e);
                            } else {
                                info!("Artifacts generated in {}", config_dir);
                            }
                        },
                        Err(e) => warn!("Validation failed for {}.{}: {}", schema, table, e),
                    }
                },
                Err(e) => {
                    warn!("Failed to get columns for {}.{}: {}", schema, table, e);
                    if let Ok(mut r) = results.lock() {
                        r.push(TaskResult {
                            schema: schema.to_string(),
                            table: table.clone(),
                            chunk_id: None,
                            status: "FAILURE".to_string(),
                            error: Some(format!("Column discovery failed: {}", e)),
                            duration_seconds: 0.0,
                            rows_exported: 0,
                            bytes_exported: 0,
                        });
                    }
                    continue;
                }
            }

            // 2. EXPORT DATA
            if size_gb > 1.0 { // Threshold 1GB
                let chunk_count = (size_gb / 1.0).ceil() as u32;
                info!("Splitting {} into {} chunks", table, chunk_count);
                match metadata::generate_chunks(&conn, schema, &table, chunk_count) {
                    Ok(chunks) => {
                        for chunk in chunks {
                             let filename = format!("{}/data_chunk_{:04}.csv.gz", data_dir, chunk.chunk_id);
                             if std::path::Path::new(&filename).exists() {
                                 info!("Skipping existing chunk: {}", filename);
                                 if let Ok(mut r) = results.lock() {
                                     r.push(TaskResult {
                                         schema: schema.to_string(),
                                         table: table.clone(),
                                         chunk_id: Some(chunk.chunk_id),
                                         status: "SKIPPED".to_string(),
                                         error: None,
                                         duration_seconds: 0.0,
                                         rows_exported: 0,
                                         bytes_exported: 0,
                                     });
                                 }
                                 continue;
                             }
                             let query_where = format!("ROWID BETWEEN '{}' AND '{}'", chunk.start_rowid, chunk.end_rowid);
                             tx.send(ExportTask {
                                 schema: schema.to_string(),
                                 table: table.clone(),
                                 chunk_id: Some(chunk.chunk_id),
                                 query_where: Some(query_where),
                                 output_file: filename,
                             })?;
                        }
                    },
                    Err(e) => {
                        warn!("Failed to generate chunks for {}: {}", table, e);
                        if let Ok(mut r) = results.lock() {
                            r.push(TaskResult {
                                schema: schema.to_string(),
                                table: table.clone(),
                                chunk_id: None,
                                status: "FAILURE".to_string(),
                                error: Some(format!("Chunk generation failed: {}", e)),
                                duration_seconds: 0.0,
                                rows_exported: 0,
                                bytes_exported: 0,
                            });
                        }
                    }
                }
            } else {
                // Single File
                let filename = format!("{}/data.csv.gz", data_dir);
                if std::path::Path::new(&filename).exists() {
                    info!("Skipping existing file: {}", filename);
                    if let Ok(mut r) = results.lock() {
                        r.push(TaskResult {
                            schema: schema.to_string(),
                            table: table.clone(),
                            chunk_id: None,
                            status: "SKIPPED".to_string(),
                            error: None,
                            duration_seconds: 0.0,
                            rows_exported: 0,
                            bytes_exported: 0,
                        });
                    }
                } else {
                    tx.send(ExportTask {
                         schema: schema.to_string(),
                         table: table.clone(),
                         chunk_id: None,
                         query_where: None,
                         output_file: filename,
                    })?;
                }
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
        
        match exporter::export_table(params) {
            Ok(stats) => {
                info!("Finished task: {}.{} ({:?}) - {} rows, {} bytes", task.schema, task.table, task.chunk_id, stats.rows, stats.bytes);
                TaskResult {
                    schema: task.schema.clone(),
                    table: task.table.clone(),
                    chunk_id: task.chunk_id,
                    status: "SUCCESS".to_string(),
                    error: None,
                    duration_seconds: stats.duration_secs,
                    rows_exported: stats.rows,
                    bytes_exported: stats.bytes,
                }
            },
            Err(e) => {
                let duration = start.elapsed().as_secs_f64();
                error!("Failed task: {}.{} ({:?}): {}", task.schema, task.table, task.chunk_id, e);
                TaskResult {
                    schema: task.schema.clone(),
                    table: task.table.clone(),
                    chunk_id: task.chunk_id,
                    status: "FAILURE".to_string(),
                    error: Some(e.to_string()),
                    duration_seconds: duration,
                    rows_exported: 0,
                    bytes_exported: 0,
                }
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
            },
            bigquery: None,
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
