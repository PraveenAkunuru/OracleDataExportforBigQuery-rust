//! The core application logic that orchestrates the overall export process.
//!
//! This module coordinates between the schema reader, data streamer, and artifact
//! writer to discover tables, plan chunks, and aggregate results.

use crate::config::AppConfig as Config;
use crate::domain::errors::Result;
use crate::domain::entities::{ExportTask, FileFormat, TaskResult};
use crate::ports::artifact_port::ArtifactPort;
use crate::ports::extraction_port::ExtractionPort;
use crate::ports::metadata_port::MetadataPort;
use log::{error, info};
use rayon::prelude::*;
use serde_json::json;
use std::sync::Arc;
use std::time::Instant;

/// Orchestrates the end-to-end export of multiple Oracle schemas and tables.
pub struct Orchestrator {
    metadata_port: Arc<dyn MetadataPort>,
    extraction_port: Arc<dyn ExtractionPort>,
    artifact_port: Arc<dyn ArtifactPort>,
    config: Config,
}

impl Orchestrator {
    /// Creates a new Orchestrator with the provided components.
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

    /// Entry point for running the full export process.
    ///
    /// This method performs table discovery, applies filters/exclusions,
    /// and processes all eligible tables in parallel.
    pub fn run(&self) -> Result<Vec<TaskResult>> {
        let start_time = Instant::now();
        info!("Starting Export Orchestrator...");

        let schemas = self.config.get_schemas();
        let target_tables = self.config.get_target_tables();
        let mut all_tables = Vec::new();

        for schema in schemas {
            let db_tables = self.metadata_port.get_tables(&schema)?;
            for t in db_tables {
                let t_up = t.to_uppercase();
                if self.config.is_excluded(&t_up) {
                    info!("Skipping excluded table: {}.{}", schema, t);
                    continue;
                }
                if let Some(targets) = &target_tables {
                    if !targets.contains(&t_up) {
                        continue;
                    }
                }
                all_tables.push((schema.clone(), t));
            }
        }

        if all_tables.is_empty() {
            info!("No tables found to export.");
            return Ok(vec![]);
        }

        let results: Vec<TaskResult> = all_tables
            .into_par_iter()
            .map(
                |(schema, table)| match self.process_table(&schema, &table) {
                    Ok(res) => res,
                    Err(e) => {
                        error!("Table {}.{} failed: {:?}", schema, table, e);
                        TaskResult::failure(schema, table, None, format!("{:?}", e))
                    }
                },
            )
            .collect();

        self.generate_report(&results, start_time.elapsed().as_secs_f64())?;

        Ok(results)
    }

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
        serde_json::to_writer_pretty(file, &report).map_err(|e| {
            crate::domain::errors::ExportError::ArtifactError(e.to_string())
        })?;

        Ok(())
    }

    /// Orchestrates the export of a single table.
    ///
    /// This involves metadata discovery, artifact generation (DDL/Schema),
    /// and either single-file extraction or parallel chunked extraction.
    fn process_table(&self, schema: &str, table: &str) -> Result<TaskResult> {
        info!("Processing {}.{}", schema, table);

        let metadata = self.metadata_port.get_table_metadata(schema, table)?;

        let out_dir = format!("{}/{}/{}", self.config.export.output_dir, schema, table);
        let config_dir = format!("{}/config", out_dir);
        std::fs::create_dir_all(&config_dir)?;

        let enable_row_hash = self.config.export.enable_row_hash.unwrap_or(false);
        let use_client_hash = self.config.export.use_client_hash.unwrap_or(false);
        let file_format = self.config.export.file_format.unwrap_or(FileFormat::Csv);
        let parquet_compression = self.config.export.parquet_compression.clone();

        let extension = match file_format {
            FileFormat::Csv => "csv.gz",
            FileFormat::Parquet => "parquet",
        };

        self.artifact_port.write_artifacts(
            &metadata,
            &config_dir,
            enable_row_hash,
            file_format,
        )?;

        let data_dir = format!("{}/data", out_dir);
        std::fs::create_dir_all(&data_dir)?;

        let parallel = self.config.export.parallel.unwrap_or(1);
        if parallel > 1 && metadata.size_gb > 1.0 {
            let chunks = self
                .metadata_port
                .generate_table_chunks(schema, table, parallel)?;
            if !chunks.is_empty() {
                info!("Exporting {}.{} in {} chunks", schema, table, chunks.len());
                let results: Vec<TaskResult> = chunks
                    .into_par_iter()
                    .enumerate()
                    .map(|(i, where_clause)| {
                        let chunk_id = i as u32;
                        let task = ExportTask {
                            schema: schema.to_string(),
                            table: table.to_string(),
                            chunk_id: Some(chunk_id),
                            query_where: Some(where_clause),
                            output_file: format!("{}/data_chunk_{:04}.{}", data_dir, i, extension),
                            enable_row_hash,
                            use_client_hash,
                            file_format,
                            parquet_compression: parquet_compression.clone(),
                            parquet_batch_size: self.config.export.parquet_batch_size,
                        };
                        match self.extraction_port.export_task(task) {
                            Ok(r) => r,
                            Err(e) => TaskResult::failure(
                                schema.to_string(),
                                table.to_string(),
                                Some(chunk_id),
                                format!("{:?}", e),
                            ),
                        }
                    })
                    .collect();

                let mut total_rows = 0;
                let mut total_bytes = 0;
                let mut max_duration = 0.0;
                let mut errors = Vec::new();

                for res in results {
                    total_rows += res.rows;
                    total_bytes += res.bytes;
                    if res.duration > max_duration {
                        max_duration = res.duration;
                    }
                    if res.status == "FAILED" {
                        errors.push(res.error.unwrap_or_default());
                    }
                }

                return if errors.is_empty() {
                    Ok(TaskResult::success(
                        schema.to_string(),
                        table.to_string(),
                        total_rows,
                        total_bytes,
                        max_duration,
                        None,
                    ))
                } else {
                    Ok(TaskResult::failure(
                        schema.to_string(),
                        table.to_string(),
                        None,
                        errors.join("; "),
                    ))
                };
            }
        }

        let task = ExportTask {
            schema: schema.to_string(),
            table: table.to_string(),
            chunk_id: None,
            query_where: None,
            output_file: format!("{}/data.{}", data_dir, extension),
            enable_row_hash,
            use_client_hash,
            file_format,
            parquet_compression,
            parquet_batch_size: self.config.export.parquet_batch_size,
        };

        self.extraction_port.export_task(task)
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
        fn export_task(&self, task: ExportTask) -> Result<TaskResult> {
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
            _meta: &TableMetadata,
            _dir: &str,
            _hash: bool,
            _format: FileFormat,
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
                Err(
                    crate::domain::errors::ExportError::ArtifactError(
                        "DB Down".to_string(),
                    ),
                )
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
