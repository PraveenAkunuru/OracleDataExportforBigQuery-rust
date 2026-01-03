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
use crate::domain::entities::{ExportTask, FileFormat, TaskResult};
use crate::domain::errors::Result;
use crate::ports::artifact_port::ArtifactPort;
use crate::ports::extraction_port::ExtractionPort;
use crate::ports::metadata_port::MetadataPort;
use log::debug;
use log::info;
use rayon::prelude::*;
use serde_json::json;
use std::sync::Arc;
use std::time::Instant;

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
    /// It performs target discovery (finding which tables to export) and then
    /// runs them in parallel using Rayon.
    pub fn run(&self) -> Result<Vec<TaskResult>> {
        let start_time = Instant::now();
        info!("Starting Export Orchestrator...");

        // --- STEP 1: DISCOVERY ---
        // We look at the configurations to see which schemas and tables the user wants.
        let target_tables = self.config.get_target_tables();
        let mut all_tables = Vec::new();
        for schema in self.config.get_schemas() {
            // We use the `metadata_port` to ask Oracle what tables are in this schema.
            let tables = self.metadata_port.get_tables(&schema)?;
            for table in tables {
                all_tables.push((schema.clone(), table));
            }
        }

        // --- STEP 2: FILTERING ---
        // We filter out tables that are explicitly excluded or not in the target list.
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

        // --- STEP 3: PARALLEL EXECUTION ---
        // `.into_par_iter()` comes from the `rayon` crate. It automatically
        // distributes the table exports across all available CPU threads.
        let results: Vec<TaskResult> = eligible_tables
            .into_par_iter()
            .map(|(schema, table)| self.process_table(&schema, &table))
            .collect();

        // --- STEP 4: REPORTING ---
        // We generate a JSON report with summary statistics at the end.
        self.generate_report(&results, start_time.elapsed().as_secs_f64())?;
        Ok(results)
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

    /// Orchestrates the export of a single table.
    ///
    /// The process flow is:
    /// 1. Fetch Metadata (Column names, types, size).
    /// 2. Setup Directories (Where the CSVs and DDL will go).
    /// 3. Write Artifacts (BigQuery DDL, schema.json).
    /// 4. Plan Tasks (Single file or multiple chunks).
    /// 5. Execute Tasks (using the ExtractionPort).
    /// 6. Aggregate Results.
    fn process_table(&self, schema: &str, table: &str) -> TaskResult {
        info!("Processing {}.{}", schema, table);

        // --- STEP 1: METADATA ---
        let metadata = match self.metadata_port.get_table_metadata(schema, table) {
            Ok(m) => m,
            Err(e) => {
                return TaskResult::failure(
                    schema.to_string(),
                    table.to_string(),
                    None,
                    format!("Metadata failed: {:?}", e),
                )
            }
        };

        // --- STEP 2: SETUP ---
        let out_dir = format!("{}/{}/{}", self.config.export.output_dir, schema, table);
        let config_dir = format!("{}/config", out_dir);
        let data_dir = format!("{}/data", out_dir);
        if let Err(e) =
            std::fs::create_dir_all(&config_dir).and_then(|_| std::fs::create_dir_all(&data_dir))
        {
            return TaskResult::failure(
                schema.to_string(),
                table.to_string(),
                None,
                format!("Dir creation failed: {:?}", e),
            );
        }

        let enable_row_hash = self.config.export.enable_row_hash.unwrap_or(false);
        let file_format = self.config.export.file_format.unwrap_or(FileFormat::Csv);

        // --- VALIDATION (Pre-flight) ---
        // 1. Always validate Row Count.
        // 2. Always validate Aggregates for numeric columns (if any).
        // 3. Validate PK Hash only if enable_row_hash is true.
        let agg_cols: Vec<String> = metadata
            .columns
            .iter()
            .filter(|c| matches!(c.bq_type.as_str(), "INT64" | "FLOAT64" | "BIGNUMERIC" | "NUMERIC"))
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

        let validation_stats = self.metadata_port
            .validate_table(schema, table, pk_ref, agg_ref)
            .ok();

        if let Some(ref v) = validation_stats {
            info!("Validation Stats: Rows={}", v.row_count);
            if let Some(aggs) = &v.aggregates {
                info!("Validation Stats: Aggregates Checked={}", aggs.len());
            }
        }

        // --- STEP 3: ARTIFACTS ---
        if let Err(e) = self.artifact_port.write_artifacts(
            &metadata,
            &config_dir,
            enable_row_hash,
            file_format,
            validation_stats.as_ref(),
        ) {
            return TaskResult::failure(
                schema.to_string(),
                table.to_string(),
                None,
                format!("Artifacts failed: {:?}", e),
            );
        }

        let extension = match file_format {
            FileFormat::Csv => "csv.gz",
            FileFormat::Parquet => "parquet",
        };

        // --- STEP 4: TASK PLANNING ---
        // If the table is large (> 1GB) and we have parallel threads available,
        // we split the table into "chunks" using Oracle ROWID ranges.
        let parallel = self.config.export.parallel.unwrap_or(1);
        let chunks = if parallel > 1 && metadata.size_gb > 1.0 {
            self.metadata_port
                .generate_table_chunks(schema, table, parallel)
                .unwrap_or_default()
        } else {
            vec![]
        };

        // We building a list of `ExportTask` objects to execute.
        let tasks: Vec<ExportTask> = if chunks.is_empty() {
            // SINGLE FILE: No where clause needed.
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
                output_file: format!("{}/data.{}", data_dir, extension),
                enable_row_hash,
                use_client_hash: self.config.export.use_client_hash.unwrap_or(false),
                file_format,
                parquet_compression: self.config.export.parquet_compression.clone(),
                parquet_batch_size: self.config.export.parquet_batch_size,
            }]
        } else {
            // MULTI-CHUNK: Each chunk has a unique ROWID range in the WHERE clause.
            chunks
                .into_iter()
                .enumerate()
                .map(|(i, where_clause)| ExportTask {
                    schema: schema.to_string(),
                    table: table.to_string(),
                    chunk_id: Some(i as u32),
                    query_where: Some(format!("{} AND 1=1", where_clause)),
                    output_file: format!("{}/data_chunk_{:04}.{}", data_dir, i, extension),
                    enable_row_hash,
                    use_client_hash: self.config.export.use_client_hash.unwrap_or(false),
                    file_format,
                    parquet_compression: self.config.export.parquet_compression.clone(),
                    parquet_batch_size: self.config.export.parquet_batch_size,
                })
                .collect()
        };

        if tasks.len() > 1 {
            info!("Exporting {}.{} in {} chunks", schema, table, tasks.len());
        }

        // --- STEP 5: EXECUTION ---
        // We use rayons `into_par_iter()` again here!
        // This is "Nested Parallelism": multiple tables in parallel, and chunks
        // within a table in parallel.
        let results: Vec<TaskResult> = tasks
            .into_par_iter()
            .map(|task| {
                debug!("Starting chunk {:?}", task.chunk_id);
                self.extraction_port
                    .export_task(task, &metadata)
                    .unwrap_or_else(|e| {
                        TaskResult::failure(
                            schema.to_string(),
                            table.to_string(),
                            None,
                            format!("{:?}", e),
                        )
                    })
            })
            .collect();

        // --- STEP 6: AGGREGATION ---
        self.aggregate_results(schema, table, results)
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
        fn write_artifacts(
            &self,
            metadata: &TableMetadata,
            _output_config_dir: &str,
            _enable_row_hash: bool,
            _file_format: FileFormat,
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
