//! Infrastructure adapter for writing BigQuery and metadata artifacts to local storage.

use crate::domain::error_definitions::{ExportError, Result};
use crate::domain::export_models::{FileFormat, TableMetadata};
use crate::ports::artifact_writer::ArtifactWriter;
// use log::{info, warn};
use serde_json::json;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Concrete implementation of `ArtifactWriter` for local filesystem storage.
///
/// This adapter generates all sidecar files required for BigQuery ingestion
/// and operational validation, including DDL, JSON schemas, and load scripts.
pub struct LocalArtifactAdapter {
    project_id: String,
    dataset_id: String,
}

impl LocalArtifactAdapter {
    /// Creates a new LocalArtifactAdapter with BigQuery destination context.
    pub fn new(project_id: String, dataset_id: String) -> Self {
        Self {
            project_id,
            dataset_id,
        }
    }

    /// Helper to write string content to a file at the specified path.
    fn write_file(&self, path: PathBuf, content: &str) -> std::io::Result<()> {
        let mut f = File::create(path)?;
        f.write_all(content.as_bytes())?;
        Ok(())
    }
}

impl ArtifactWriter for LocalArtifactAdapter {
    fn write_artifacts(
        &self,
        metadata: &TableMetadata,
        output_config_dir: &str,
        enable_row_hash: bool,
        file_format: FileFormat,
    ) -> Result<()> {
        let config_path = Path::new(output_config_dir);

        // 1. BigQuery DDL
        let bq_ddl = self.generate_bq_ddl(metadata, enable_row_hash);
        self.write_file(config_path.join("bigquery.ddl"), &bq_ddl)
            .map_err(ExportError::IoError)?;

        // 2. Schema JSON
        let schema_json = self.generate_schema_json(metadata, enable_row_hash);
        self.write_file(
            config_path.join("schema.json"),
            &serde_json::to_string_pretty(&schema_json).unwrap(),
        )
        .map_err(ExportError::IoError)?;

        // 3. Metadata JSON
        let meta_json = json!({
            "table_name": metadata.table_name,
            "schema_name": metadata.schema,
            "full_name": format!("{}.{}", metadata.schema, metadata.table_name),
            "export_timestamp": chrono::Utc::now().to_rfc3339(),
            "columns": metadata.columns,
            "pk": metadata.pk_cols,
            "partition_keys": metadata.partition_cols
        });
        self.write_file(
            config_path.join("metadata.json"),
            &serde_json::to_string_pretty(&meta_json).unwrap(),
        )
        .map_err(ExportError::IoError)?;

        // 4. Load Command
        let load_cmd = self.generate_load_command(metadata, file_format);
        let load_cmd_path = config_path.join("load_command.sh");
        self.write_file(load_cmd_path.clone(), &load_cmd)
            .map_err(ExportError::IoError)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Ok(f) = File::open(&load_cmd_path) {
                let _ = f.set_permissions(std::fs::Permissions::from_mode(0o755));
            }
        }

        // 5. Export SQL (Reference)
        let export_sql = self.generate_export_sql(metadata, enable_row_hash);
        self.write_file(config_path.join("export.sql"), &export_sql)
            .map_err(ExportError::IoError)?;

        Ok(())
    }
}

impl LocalArtifactAdapter {
    /// Generates BigQuery DDL (Physical Table + Logical View).
    fn generate_bq_ddl(&self, metadata: &TableMetadata, enable_row_hash: bool) -> String {
        let physical_name = format!("{}_PHYSICAL", metadata.table_name);
        let mut ddl = format!(
            "-- 1. Physical Table\nCREATE OR REPLACE TABLE `{}.{}.{}` (\n",
            self.project_id, self.dataset_id, physical_name
        );

        let mut stored_cols = Vec::new();
        for col in &metadata.columns {
            if !col.is_virtual {
                let col_ddl = format!("  {} {}", col.name, col.bq_type);
                let options = if let Some(c) = &col.comment {
                    format!(" OPTIONS(description=\"{}\")", c.replace("\"", "\\\""))
                } else {
                    String::new()
                };
                stored_cols.push(format!("{}{}", col_ddl, options));
            }
        }

        ddl.push_str(&stored_cols.join(",\n"));
        if enable_row_hash {
            ddl.push_str(",\n  ROW_HASH STRING NOT NULL OPTIONS(description=\"SHA256 hash for duplicate detection\")");
        }
        if !metadata.pk_cols.is_empty() {
            ddl.push_str(&format!(
                ",\n  PRIMARY KEY ({}) NOT ENFORCED",
                metadata.pk_cols.join(", ")
            ));
        }
        ddl.push_str("\n)");

        // Partitioning
        if !metadata.partition_cols.is_empty() {
            // Find first date/timestamp col
            if let Some(col) = metadata.columns.iter().find(|c| {
                metadata.partition_cols.contains(&c.name)
                    && (c.bq_type.contains("DATE") || c.bq_type.contains("TIMESTAMP"))
            }) {
                ddl.push_str(&format!("\nPARTITION BY {}", col.name));
            }
        }

        // Clustering
        let mut cluster_cols = Vec::new();
        for pk in &metadata.pk_cols {
            cluster_cols.push(pk.clone());
        }
        for idx in &metadata.index_cols {
            if !cluster_cols.contains(idx) {
                cluster_cols.push(idx.clone());
            }
        }
        if !cluster_cols.is_empty() {
            let count = std::cmp::min(4, cluster_cols.len());
            ddl.push_str(&format!(
                "\nCLUSTER BY {}",
                cluster_cols[0..count].join(", ")
            ));
        }

        ddl.push_str(";\n\n-- 2. Logical View\n");
        ddl.push_str(&format!(
            "CREATE OR REPLACE VIEW `{}.{}.{}` AS SELECT\n",
            self.project_id, self.dataset_id, metadata.table_name
        ));
        let mut view_cols = Vec::new();
        for col in &metadata.columns {
            if col.is_virtual {
                view_cols.push(format!(
                    "  /* Virtual */ CAST(`{}` AS STRING) AS {}",
                    col.name, col.name
                ));
            } else {
                view_cols.push(format!("  {}", col.name));
            }
        }
        if enable_row_hash {
            view_cols.push("  ROW_HASH".to_string());
        }
        ddl.push_str(&view_cols.join(",\n"));
        ddl.push_str(&format!(
            "\nFROM `{}.{}.{}`;",
            self.project_id, self.dataset_id, physical_name
        ));

        ddl
    }

    /// Generates BigQuery Schema JSON for the `bq load` command.
    fn generate_schema_json(
        &self,
        metadata: &TableMetadata,
        enable_row_hash: bool,
    ) -> serde_json::Value {
        let mut fields = Vec::new();
        for col in &metadata.columns {
            if !col.is_virtual {
                fields.push(json!({
                    "name": col.name,
                    "type": col.bq_type,
                    "mode": "NULLABLE",
                    "description": col.comment
                }));
            }
        }
        if enable_row_hash {
            fields.push(json!({
                "name": "ROW_HASH",
                "type": "STRING",
                "mode": "REQUIRED",
                "description": "SHA256 hash for duplicate detection"
            }));
        }
        json!(fields)
    }

    /// Generates a bash script with the `bq load` command.
    fn generate_load_command(&self, metadata: &TableMetadata, file_format: FileFormat) -> String {
        match file_format {
            FileFormat::Csv => format!(
                r#"#!/bin/bash
# BigQuery Load Command (CSV)
set -e
bq load --source_format=CSV --field_delimiter=$'\x10' --skip_leading_rows=1 --null_marker='' --replace --schema=schema.json {}:{}.{}_PHYSICAL "../data/*.csv.gz"
"#,
                self.project_id, self.dataset_id, metadata.table_name
            ),
            FileFormat::Parquet => format!(
                r#"#!/bin/bash
# BigQuery Load Command (PARQUET)
set -e
bq load --source_format=PARQUET --replace --schema=schema.json {}:{}.{}_PHYSICAL "../data/*.parquet"
"#,
                self.project_id, self.dataset_id, metadata.table_name
            ),
        }
    }

    /// Generates a reference Oracle SELECT statement for the table.
    fn generate_export_sql(&self, metadata: &TableMetadata, enable_row_hash: bool) -> String {
        let mut cols = Vec::new();
        for col in &metadata.columns {
            cols.push(format!("\"{}\"", col.name));
        }
        if enable_row_hash {
            cols.push("ROW_HASH (computed)".to_string());
        }
        format!(
            "-- Reference SQL\nSELECT {} FROM \"{}\".\"{}\";",
            cols.join(", "),
            metadata.schema,
            metadata.table_name
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::export_models::ColumnMetadata;

    #[test]
    fn test_generate_schema_json() {
        let adapter = LocalArtifactAdapter::new("p".into(), "d".into());
        let meta = TableMetadata {
            schema: "S".into(),
            table_name: "T".into(),
            columns: vec![
                ColumnMetadata {
                    name: "C1".into(),
                    raw_type: "VARCHAR2".into(),
                    bq_type: "STRING".into(),
                    is_virtual: false,
                    is_hidden: false,
                    is_identity: false,
                    comment: Some("C1 comment".into()),
                },
                ColumnMetadata {
                    name: "V1".into(),
                    raw_type: "NUMBER".into(),
                    bq_type: "INTEGER".into(),
                    is_virtual: true,
                    is_hidden: false,
                    is_identity: false,
                    comment: None,
                },
            ],
            size_gb: 0.0,
            pk_cols: vec!["C1".into()],
            partition_cols: vec![],
            index_cols: vec![],
        };

        let schema = adapter.generate_schema_json(&meta, true);
        let fields = schema.as_array().unwrap();

        // Should have C1 and ROW_HASH, but NOT V1
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0]["name"], "C1");
        assert_eq!(fields[1]["name"], "ROW_HASH");
    }

    #[test]
    fn test_generate_bq_ddl() {
        let adapter = LocalArtifactAdapter::new("my-project".into(), "my-dataset".into());
        let meta = TableMetadata {
            schema: "TEST".into(),
            table_name: "MY_TABLE".into(),
            columns: vec![
                ColumnMetadata {
                    name: "ID".into(),
                    raw_type: "NUMBER".into(),
                    bq_type: "INT64".into(),
                    is_virtual: false,
                    is_hidden: false,
                    is_identity: false,
                    comment: None,
                },
                ColumnMetadata {
                    name: "COMPUTED".into(),
                    raw_type: "VARCHAR2".into(),
                    bq_type: "STRING".into(),
                    is_virtual: true,
                    is_hidden: false,
                    is_identity: false,
                    comment: None,
                },
            ],
            size_gb: 0.1,
            pk_cols: vec!["ID".into()],
            partition_cols: vec![],
            index_cols: vec![],
        };

        let ddl = adapter.generate_bq_ddl(&meta, false);

        assert!(ddl.contains("CREATE OR REPLACE TABLE `my-project.my-dataset.MY_TABLE_PHYSICAL`"));
        assert!(ddl.contains("ID INT64"));
        assert!(!ddl.contains("COMPUTED STRING")); // Virtual should be in view, not table
        assert!(ddl.contains("CREATE OR REPLACE VIEW `my-project.my-dataset.MY_TABLE`"));
        assert!(ddl.contains("CAST(`COMPUTED` AS STRING) AS COMPUTED"));
    }

    #[test]
    fn test_generate_load_command() {
        let adapter = LocalArtifactAdapter::new("p".into(), "d".into());
        let meta = TableMetadata {
            schema: "S".into(),
            table_name: "T".into(),
            columns: vec![],
            size_gb: 0.0,
            pk_cols: vec![],
            partition_cols: vec![],
            index_cols: vec![],
        };

        let csv_cmd = adapter.generate_load_command(&meta, FileFormat::Csv);
        assert!(csv_cmd.contains("--source_format=CSV"));
        assert!(csv_cmd.contains("../data/*.csv.gz"));

        let parquet_cmd = adapter.generate_load_command(&meta, FileFormat::Parquet);
        assert!(parquet_cmd.contains("--source_format=PARQUET"));
        assert!(parquet_cmd.contains("../data/*.parquet"));
    }
}
