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

//! # Export Orchestrator
//!
//! The Orchestrator is the "Brain" of the application.
//!
//! In **Hexagonal Architecture**, the Orchestrator lives in the "Domain" or "Application"
//! layer. It knows the **business process** (how to export a table), but it
//! doesn't know the **technical details** (how to talk to Oracle or how to write
//! a Parquet file).
//!
//! It communicates with the outside world through **Ports** (interfaces):
//! - `MetadataPort`: To ask "what tables exist?" and "what columns does this table have?"
//! - `ExtractionPort`: To say "stream this data to this file."
//! - `ArtifactPort`: To say "generate the BigQuery schema for this table."

use crate::config::AppConfig as Config;
use crate::domain::entities::{ExportTask, FileFormat, TableMetadata, TaskResult};
use crate::domain::errors::Result;
use crate::ports::artifact_port::ArtifactPort;
use crate::ports::extraction_port::ExtractionPort;
use crate::ports::metadata_port::MetadataPort;
use rayon::prelude::*;
use serde_json::json;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info, instrument};

/// `Orchestrator` coordinates the end-to-end export process.
pub struct Orchestrator {
    // We use `Arc<dyn ...>` to hold our ports.
    // - `Arc`: safe to share across threads.
    // - `dyn`: stands for "Dynamic". It means we can swap the implementation
    //   (e.g., use a `Mock` port during testing).
    metadata_port: Arc<dyn MetadataPort>,
    extraction_port: Arc<dyn ExtractionPort>,
    artifact_port: Arc<dyn ArtifactPort>,
    config: Config,
}

impl Orchestrator {
    /// Creates a new Orchestrator. Note how we "inject" the dependencies here.
    pub fn new(
        metadata_port: Arc<dyn MetadataPort>,
        extraction_port: Arc<dyn ExtractionPort>,
        artifact_port: Arc<dyn ArtifactPort>,
        config: Config,
    ) -> Self {
        Self {
            metadata_port,
            extraction_port,
            artifact_port,
            config,
        }
    }

    /// The main entry point for starting the export.
    ///
    /// It performs target discovery (finding which tables to export),
    /// plans the tasks (fetching metadata, creating directories),
    /// and then executes the tasks in a flat parallel stream.
    #[instrument(skip(self))]
    pub fn run(&self) -> Result<Vec<TaskResult>> {
        let start_time = Instant::now();
        info!("Starting Export Orchestrator...");

        // --- STEP 1: DISCOVERY ---
        let target_tables = self.config.get_target_tables();
        let mut all_tables = Vec::new();
        for schema in self.config.get_schemas() {
            let tables = self.metadata_port.get_tables(&schema)?;
            for table in tables {
                all_tables.push((schema.clone(), table));
            }
        }

        // --- STEP 2: FILTERING ---
        let eligible_tables: Vec<(String, String)> = all_tables
            .into_iter()
            .filter(|(schema, table)| {
                let t_up = table.to_uppercase();
                if self.config.is_excluded(&t_up) {
                    info!("Skipping excluded table: {}.{}", schema, table);
                    return false;
                }
                if let Some(targets) = &target_tables {
                    if !targets.contains(&t_up) {
                        return false;
                    }
                }
                true
            })
            .collect();

        if eligible_tables.is_empty() {
            info!("No tables found to export.");
            return Ok(vec![]);
        }

        // --- STEP 3: PLANNING (Metadata & Artifacts) ---
        info!("Planning export for {} tables...", eligible_tables.len());
        let (plans, mut failures): (Vec<_>, Vec<_>) = eligible_tables
            .into_par_iter()
            .map(|(schema, table)| self.prepare_table(&schema, &table))
            .partition_map(|r| match r {
                Ok(val) => rayon::iter::Either::Left(val),
                Err(err_res) => rayon::iter::Either::Right(err_res),
            });

        // --- STEP 4: FLATTENING ---
        let mut flat_tasks = Vec::new();
        let mut table_meta_map = std::collections::HashMap::new();
        let mut validation_map = std::collections::HashMap::new();
        let mut skipped_tables = std::collections::HashSet::new();

        for (metadata, tasks, validation) in plans {
            let key = format!("{}.{}", metadata.schema, metadata.table_name);
            let meta_arc = Arc::new(metadata);

            table_meta_map.insert(key.clone(), meta_arc.clone());

            if tasks.is_empty() {
                skipped_tables.insert(key);
            } else {
                validation_map.insert(key, validation);
                for task in tasks {
                    flat_tasks.push((task, meta_arc.clone()));
                }
            }
        }

        // --- STEP 5: EXECUTION ---
        info!("Executing {} tasks in parallel...", flat_tasks.len());
        let execution_results: Vec<TaskResult> = flat_tasks
            .into_par_iter()
            .map(|(task, meta)| self.execute_task(task, &meta))
            .collect();

        // --- STEP 6: AGGREGATION ---
        let mut results_by_table: std::collections::HashMap<String, Vec<TaskResult>> =
            std::collections::HashMap::new();

        for key in table_meta_map.keys() {
            results_by_table.entry(key.clone()).or_default();
        }

        for res in execution_results {
            let key = format!("{}.{}", res.schema, res.table);
            results_by_table.entry(key).or_default().push(res);
        }

        let mut final_results: Vec<TaskResult> = Vec::new();
        final_results.append(&mut failures);

        for (key, chunk_results) in results_by_table {
            if let Some((schema, table)) = key.split_once('.') {
                let agg_result = self.aggregate_results(schema, table, chunk_results);

                // --- STEP 6.5: POST-SUCCESS ARTIFACTS (Metadata) ---
                if agg_result.status == "SUCCESS" && !skipped_tables.contains(&key) {
                    if let Some(meta) = table_meta_map.get(&key) {
                        let config_dir = self
                            .config
                            .get_table_config_dir(&meta.schema, &meta.table_name);
                        let validation = validation_map.get(&key).and_then(|v| v.as_ref());

                        if let Err(e) = self.artifact_port.write_metadata(
                            meta,
                            &config_dir.to_string_lossy(),
                            validation,
                        ) {
                            log::error!("Failed to write metadata.json for {}: {:?}", key, e);
                            // We don't fail the task, but we log strictly.
                        }
                    }
                } else if skipped_tables.contains(&key) {
                    // It was skipped. We still want to include it in final results,
                    // but we might want to flag it as "SKIPPED".
                    // Current aggregate_results return SUCCESS with 0 rows if empty.
                    // We can leave it as is.
                }

                final_results.push(agg_result);
            }
        }

        // Sort by duration desc for visibility
        final_results.sort_by(|a, b| {
            b.duration
                .partial_cmp(&a.duration)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // --- STEP 7: REPORTING ---
        self.generate_report(&final_results, start_time.elapsed().as_secs_f64())?;
        Ok(final_results)
    }

    /// Generates a final JSON report showing total rows, bytes, and throughput.
    fn generate_report(&self, results: &[TaskResult], duration_secs: f64) -> Result<()> {
        let success = results.iter().filter(|r| r.status == "SUCCESS").count();
        let failed = results.len() - success;
        let total_rows: u64 = results.iter().map(|r| r.rows).sum();
        let total_bytes: u64 = results.iter().map(|r| r.bytes).sum();

        let report = json!({
            "summary": {
                "total_tasks": results.len(),
                "success": success,
                "failed": failed,
                "total_rows": total_rows,
                "total_bytes": total_bytes,
                "total_duration_seconds": duration_secs,
                "total_mb_per_sec": if duration_secs > 0.0 { (total_bytes as f64 / 1024.0 / 1024.0) / duration_secs } else { 0.0 }
            },
            "details": results
        });

        let timestamp = chrono::Local::now().format("%Y%m%d_%H%M%S");
        let report_path = format!(
            "{}/report_{}.json",
            self.config.export.output_dir, timestamp
        );

        std::fs::create_dir_all(&self.config.export.output_dir)?;
        let file = std::fs::File::create(report_path)
            .map_err(crate::domain::errors::ExportError::IoError)?;
        serde_json::to_writer_pretty(file, &report)
            .map_err(|e| crate::domain::errors::ExportError::ArtifactError(e.to_string()))?;

        Ok(())
    }

    /// Prepares a table for export: fetches metadata, writes artifacts, and generates chunks.
    #[allow(clippy::result_large_err)]
    fn prepare_table(
        &self,
        schema: &str,
        table: &str,
    ) -> std::result::Result<
        (
            TableMetadata,
            Vec<ExportTask>,
            Option<crate::domain::entities::ValidationStats>,
        ),
        TaskResult,
    > {
        info!("Planning {}.{}", schema, table);

        // 1. Metadata
        let metadata = match self.metadata_port.get_table_metadata(schema, table) {
            Ok(m) => m,
            Err(e) => {
                return Err(TaskResult::failure(
                    schema.to_string(),
                    table.to_string(),
                    None,
                    format!("Metadata failed: {:?}", e),
                ));
            }
        };

        // 2. Setup Directories
        let config_dir = self.config.get_table_config_dir(schema, table);
        let data_dir = self.config.get_table_data_dir(schema, table);

        if let Err(e) =
            std::fs::create_dir_all(&config_dir).and_then(|_| std::fs::create_dir_all(&data_dir))
        {
            return Err(TaskResult::failure(
                schema.to_string(),
                table.to_string(),
                None,
                format!("Dir creation failed: {:?}", e),
            ));
        }

        // --- TABLE-LEVEL RESTART CHECK ---
        // If metadata.json exists, we assume the table is FULLY DONE.
        // We skip generating tasks.
        let meta_json_path = std::path::Path::new(&config_dir).join("metadata.json");
        if meta_json_path.exists() {
            info!(
                "Table {}.{} already completed (metadata.json exists). Skipping.",
                schema, table
            );
            // We return empty tasks.
            // Note: We don't have the previous run's stats unless we parse metadata.json.
            // For now, we return empty stats. The report will show 0 rows exported *in this run*.
            return Ok((metadata, vec![], None));
        }

        let enable_row_hash = self.config.export.enable_row_hash.unwrap_or(false);
        let file_format = self.config.export.file_format.unwrap_or(FileFormat::Csv);

        // 3. Validation
        // 1. Always validate Row Count.
        // 2. Always validate Aggregates for numeric columns (if any).
        // 3. Validate PK Hash only if enable_row_hash is true.
        let agg_cols: Vec<String> = metadata
            .columns
            .iter()
            .filter(|c| {
                matches!(
                    c.bq_type.as_str(),
                    "INT64" | "FLOAT64" | "BIGNUMERIC" | "NUMERIC"
                )
            })
            .map(|c| c.name.clone())
            .collect();

        let pk_ref = if enable_row_hash {
            Some(metadata.pk_cols.as_slice())
        } else {
            None
        };

        let agg_ref = if !agg_cols.is_empty() {
            Some(agg_cols.as_slice())
        } else {
            None
        };

        let validation_stats = self
            .metadata_port
            .validate_table(schema, table, pk_ref, agg_ref)
            .ok();

        if let Some(ref v) = validation_stats {
            info!("Validation Stats: Rows={}", v.row_count);
            if let Some(aggs) = &v.aggregates {
                info!("Validation Stats: Aggregates Checked={}", aggs.len());
            }
        }

        // 4. Artifacts (Setup only)
        if let Err(e) = self.artifact_port.prepare_artifacts(
            &metadata,
            &config_dir.to_string_lossy(),
            enable_row_hash,
            file_format,
        ) {
            return Err(TaskResult::failure(
                schema.to_string(),
                table.to_string(),
                None,
                format!("Artifacts failed: {:?}", e),
            ));
        }

        let extension = match file_format {
            FileFormat::Csv => "csv.gz",
            FileFormat::Parquet => "parquet",
        };

        // 5. Task Planning
        let parallel = self.config.export.parallel.unwrap_or(1);
        let chunks = if parallel > 1 && metadata.size_gb > 1.0 {
            self.metadata_port
                .generate_table_chunks(schema, table, parallel)
                .unwrap_or_default()
        } else {
            vec![]
        };

        let tasks: Vec<ExportTask> = if chunks.is_empty() {
            vec![ExportTask {
                schema: schema.to_string(),
                table: table.to_string(),
                chunk_id: None,
                query_where: self
                    .config
                    .export
                    .query_where
                    .clone()
                    .or_else(|| Some("1=1".to_string())),
                output_file: data_dir
                    .join(format!("data.{}", extension))
                    .to_string_lossy()
                    .to_string(),
                enable_row_hash,
                use_client_hash: self.config.export.use_client_hash.unwrap_or(false),
                file_format,
                parquet_compression: self.config.export.parquet_compression.clone(),
                parquet_batch_size: self.config.export.parquet_batch_size,
            }]
        } else {
            chunks
                .into_iter()
                .enumerate()
                .map(|(i, where_clause)| ExportTask {
                    schema: schema.to_string(),
                    table: table.to_string(),
                    chunk_id: Some(i as u32),
                    query_where: Some(format!("{} AND 1=1", where_clause)),
                    output_file: data_dir
                        .join(format!("data_chunk_{:04}.{}", i, extension))
                        .to_string_lossy()
                        .to_string(),
                    enable_row_hash,
                    use_client_hash: self.config.export.use_client_hash.unwrap_or(false),
                    file_format,
                    parquet_compression: self.config.export.parquet_compression.clone(),
                    parquet_batch_size: self.config.export.parquet_batch_size,
                })
                .collect()
        };

        Ok((metadata, tasks, validation_stats))
    }

    /// Executes a single export task.
    #[instrument(skip(self), fields(schema = %task.schema, table = %task.table, chunk = ?task.chunk_id))]
    fn execute_task(&self, task: ExportTask, metadata: &TableMetadata) -> TaskResult {
        // --- RESUME CAPABILITY ---
        // We now rely on Table-Level checking (metadata.json).
        // However, if we are here, it means we decided to run the task.
        // We should ensure we start clean.
        let data_path = std::path::Path::new(&task.output_file);
        if data_path.exists() {
            // Since we don't have chunk-level skipping anymore,
            // existence of data file means it's a leftover from a failed run (since metadata.json check didn't trigger).
            // We should delete it.
            let _ = std::fs::remove_file(data_path);
        }

        // Also remove legacy .meta files if they exist
        let meta_path = format!("{}.meta", task.output_file);
        let _ = std::fs::remove_file(meta_path);

        debug!("Starting chunk {:?}", task.chunk_id);
        self.extraction_port
            .export_task(task.clone(), metadata)
            .unwrap_or_else(|e| {
                TaskResult::failure(task.schema, task.table, task.chunk_id, format!("{:?}", e))
            })
    }

    /// Combines multiple chunk results into a single table result.
    fn aggregate_results(&self, schema: &str, table: &str, results: Vec<TaskResult>) -> TaskResult {
        if results.len() == 1 {
            return results.into_iter().next().unwrap();
        }

        let mut total_rows = 0;
        let mut total_bytes = 0;
        let mut max_duration: f64 = 0.0;
        let mut errors = Vec::new();

        for res in results {
            total_rows += res.rows;
            total_bytes += res.bytes;
            max_duration = max_duration.max(res.duration);
            if res.status == "FAILED" {
                errors.push(res.error.unwrap_or_default());
            }
        }

        if errors.is_empty() {
            TaskResult::success(
                schema.to_string(),
                table.to_string(),
                total_rows,
                total_bytes,
                max_duration,
                None,
            )
        } else {
            TaskResult::failure(
                schema.to_string(),
                table.to_string(),
                None,
                errors
                    .iter()
                    .filter(|e| !e.is_empty())
                    .cloned()
                    .collect::<Vec<_>>()
                    .join("; "),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::entities::{
        ColumnMetadata, ExportTask, FileFormat, TableMetadata, TaskResult,
    };
    use crate::ports::artifact_port::ArtifactPort;
    use crate::ports::extraction_port::ExtractionPort;
    use crate::ports::metadata_port::MetadataPort;
    use std::sync::Arc;

    struct MockMetadataPort;
    impl MetadataPort for MockMetadataPort {
        fn get_tables(&self, _schema: &str) -> Result<Vec<String>> {
            Ok(vec!["TABLE1".to_string()])
        }
        fn get_table_metadata(&self, schema: &str, table: &str) -> Result<TableMetadata> {
            Ok(TableMetadata {
                schema: schema.to_string(),
                table_name: table.to_string(),
                columns: vec![ColumnMetadata {
                    name: "COL1".to_string(),
                    raw_type: "VARCHAR2".to_string(),
                    bq_type: "STRING".to_string(),
                    is_virtual: false,
                    virtual_expr: None,
                    is_transformed: false,
                    is_hidden: false,
                    is_identity: false,
                    comment: None,
                }],
                size_gb: 2.0,
                pk_cols: vec!["COL1".to_string()],
                partition_cols: vec![],
                index_cols: vec![],
            })
        }
        fn get_db_cpu_count(&self) -> Result<usize> {
            Ok(4)
        }
        fn generate_table_chunks(
            &self,
            _schema: &str,
            _table: &str,
            _count: usize,
        ) -> Result<Vec<String>> {
            Ok(vec!["chunk1".to_string(), "chunk2".to_string()])
        }
        fn validate_table(
            &self,
            _schema: &str,
            table: &str,
            _pk: Option<&[String]>,
            _agg: Option<&[String]>,
        ) -> Result<crate::domain::entities::ValidationStats> {
            Ok(crate::domain::entities::ValidationStats {
                table_name: table.to_string(),
                row_count: 100,
                pk_hash: None,
                aggregates: None,
            })
        }
    }

    struct MockExtractionPort;
    impl ExtractionPort for MockExtractionPort {
        fn export_task(&self, task: ExportTask, _metadata: &TableMetadata) -> Result<TaskResult> {
            Ok(TaskResult::success(
                task.schema,
                task.table,
                50,
                512,
                0.5,
                task.chunk_id,
            ))
        }
    }

    struct MockArtifactPort;
    impl ArtifactPort for MockArtifactPort {
        fn prepare_artifacts(
            &self,
            metadata: &TableMetadata,
            _output_config_dir: &str,
            _enable_row_hash: bool,
            _file_format: FileFormat,
        ) -> Result<()> {
            Ok(())
        }
        fn write_metadata(
            &self,
            metadata: &TableMetadata,
            _output_config_dir: &str,
            _validation_stats: Option<&crate::domain::entities::ValidationStats>,
        ) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_orchestrator_run() {
        let temp_dir = tempfile::tempdir().unwrap();
        let out_dir = temp_dir.path().to_str().unwrap().to_string();

        let config = Config {
            database: crate::config::DatabaseConfig {
                username: "TEST".to_string(),
                password: None,
                host: "localhost".to_string(),
                port: 1521,
                service: "XE".to_string(),
                connection_string: None,
            },
            export: crate::config::ExportConfig {
                output_dir: out_dir.clone(),
                query_where: None,
                schema: Some("TEST".to_string()),
                table: None,
                parallel: Some(2),
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
                use_client_hash: None,
                adaptive_parallelism: None,
                target_throughput_per_core: None,
                file_format: None,
                parquet_compression: None,
                parquet_batch_size: None,
            },
            bigquery: None,
            gcp: None,
        };

        let artifact_port = Arc::new(MockArtifactPort);
        let orchestrator = Orchestrator::new(
            Arc::new(MockMetadataPort),
            Arc::new(MockExtractionPort),
            artifact_port,
            config,
        );

        let results = orchestrator.run().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].table, "TABLE1");
        assert_eq!(results[0].status, "SUCCESS");
        assert_eq!(results[0].rows, 100);

        let reports = std::fs::read_dir(&out_dir).unwrap();
        let mut report_found = false;
        for entry in reports {
            let name = entry.unwrap().file_name().into_string().unwrap();
            if name.starts_with("report_") && name.ends_with(".json") {
                report_found = true;
            }
        }
        assert!(report_found);
    }

    #[test]
    fn test_orchestrator_failure() {
        struct FailingMetadataPort;
        impl MetadataPort for FailingMetadataPort {
            fn get_tables(&self, _schema: &str) -> Result<Vec<String>> {
                Err(crate::domain::errors::ExportError::ArtifactError(
                    "DB Down".to_string(),
                ))
            }
            fn get_table_metadata(&self, _schema: &str, _table: &str) -> Result<TableMetadata> {
                unreachable!()
            }
            fn get_db_cpu_count(&self) -> Result<usize> {
                Ok(4)
            }
            fn generate_table_chunks(
                &self,
                _schema: &str,
                _table: &str,
                _count: usize,
            ) -> Result<Vec<String>> {
                Ok(vec![])
            }
            fn validate_table(
                &self,
                _schema: &str,
                _table: &str,
                _pk: Option<&[String]>,
                _agg: Option<&[String]>,
            ) -> Result<crate::domain::entities::ValidationStats> {
                unreachable!()
            }
        }

        let temp_dir = tempfile::tempdir().unwrap();
        let out_dir = temp_dir.path().to_str().unwrap().to_string();

        let config = Config {
            database: crate::config::DatabaseConfig {
                username: "TEST".to_string(),
                password: None,
                host: "localhost".to_string(),
                port: 1521,
                service: "XE".to_string(),
                connection_string: None,
            },
            export: crate::config::ExportConfig {
                output_dir: out_dir.clone(),
                query_where: None,
                schema: Some("TEST".to_string()),
                table: None,
                parallel: Some(1),
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
                use_client_hash: None,
                adaptive_parallelism: None,
                target_throughput_per_core: None,
                file_format: None,
                parquet_compression: None,
                parquet_batch_size: None,
            },
            bigquery: None,
            gcp: None,
        };

        let artifact_port = Arc::new(MockArtifactPort);
        let orchestrator = Orchestrator::new(
            Arc::new(FailingMetadataPort),
            Arc::new(MockExtractionPort),
            artifact_port,
            config,
        );

        let results = orchestrator.run();
        assert!(results.is_err());
    }

    #[test]
    fn test_orchestrator_run_parquet() {
        let temp_dir = tempfile::tempdir().unwrap();
        let out_dir = temp_dir.path().to_str().unwrap().to_string();

        let config = Config {
            database: crate::config::DatabaseConfig {
                username: "TEST".to_string(),
                password: None,
                host: "localhost".to_string(),
                port: 1521,
                service: "XE".to_string(),
                connection_string: None,
            },
            export: crate::config::ExportConfig {
                output_dir: out_dir.clone(),
                query_where: None,
                schema: Some("TEST".to_string()),
                table: None,
                parallel: Some(2),
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
                use_client_hash: None,
                adaptive_parallelism: None,
                target_throughput_per_core: None,
                file_format: Some(FileFormat::Parquet),
                parquet_compression: Some("zstd".into()),
                parquet_batch_size: None,
            },
            bigquery: None,
            gcp: None,
        };

        let artifact_port = Arc::new(MockArtifactPort);
        let orchestrator = Orchestrator::new(
            Arc::new(MockMetadataPort),
            Arc::new(MockExtractionPort),
            artifact_port,
            config,
        );

        let results = orchestrator.run().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].table, "TABLE1");
        assert_eq!(results[0].status, "SUCCESS");

        // Verify that Parquet files were "generated" (orchestrated)
        // In our mock, ExtractionPort doesn't actually create files,
        // but Orchestrator would have created the directories.
        let data_dir = format!("{}/TEST/TABLE1/data", out_dir);
        assert!(std::path::Path::new(&data_dir).exists());
    }
}
