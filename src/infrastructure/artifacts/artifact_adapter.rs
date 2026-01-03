//! # BigQuery Artifact Adapter
//!
//! This adapter is responsible for generating the "Instruction Manual" for BigQuery.
//!
//! It creates 5 main files for every table:
//! 1. `bigquery.ddl`: SQL to create the table and view in BigQuery.
//! 2. `schema.json`: The JSON schema file used by the `bq load` command.
//! 3. `metadata.json`: A record of what settings were used during this export.
//! 4. `load_command.sh`: A helper bash script to load the data into Google Cloud.
//! 5. `export.sql`: The original query used to fetch data (for debugging).

use crate::domain::entities::{FileFormat, TableMetadata};
use crate::domain::errors::{ExportError, Result};
use crate::ports::artifact_port::ArtifactPort;
use serde_json::json;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

/// `ArtifactAdapter` implements the `ArtifactPort`.
pub struct ArtifactAdapter {
    project_id: String,
    dataset_id: String,
}

impl ArtifactAdapter {
    pub fn new(project_id: String, dataset_id: String) -> Self {
        Self {
            project_id,
            dataset_id,
        }
    }

    /// Helper to write text to a file.
    fn write_file(&self, path: PathBuf, content: &str) -> std::io::Result<()> {
        let mut f = File::create(path)?;
        f.write_all(content.as_bytes())?;
        Ok(())
    }
}

impl ArtifactPort for ArtifactAdapter {
    /// Generates and writes all sidecar files.
    fn write_artifacts(
        &self,
        metadata: &TableMetadata,
        output_config_dir: &str,
        enable_row_hash: bool,
        file_format: FileFormat,
    ) -> Result<()> {
        let config_path = Path::new(output_config_dir);

        // --- 1. BIGQUERY DDL ---
        // We create a "Physical" table to hold raw data and a "Logical" view for users.
        let bq_ddl = self.generate_bq_ddl(metadata, enable_row_hash);
        self.write_file(config_path.join("bigquery.ddl"), &bq_ddl)
            .map_err(ExportError::IoError)?;

        // --- 2. SCHEMA JSON ---
        // BigQuery needs a JSON map of columns and types to load raw files.
        let schema_json = self.generate_schema_json(metadata, enable_row_hash);
        self.write_file(
            config_path.join("schema.json"),
            &serde_json::to_string_pretty(&schema_json).unwrap(),
        )
        .map_err(ExportError::IoError)?;

        // --- 3. METADATA JSON ---
        // Useful for audit logs and verification tools.
        let meta_json = json!({
            "table_name": metadata.table_name,
            "schema_name": metadata.schema,
            "export_timestamp": chrono::Utc::now().to_rfc3339(),
            "columns": metadata.columns,
            "pk": metadata.pk_cols,
        });
        self.write_file(
            config_path.join("metadata.json"),
            &serde_json::to_string_pretty(&meta_json).unwrap(),
        )
        .map_err(ExportError::IoError)?;

        // --- 4. LOAD COMMAND ---
        // We generate the exact `bq load ...` CLI command to save the user time.
        let load_cmd = self.generate_load_command(metadata, file_format);
        let load_cmd_path = config_path.join("load_command.sh");
        self.write_file(load_cmd_path.clone(), &load_cmd)
            .map_err(ExportError::IoError)?;

        // Make the script executable on Linux/Mac.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Ok(f) = File::open(&load_cmd_path) {
                let _ = f.set_permissions(std::fs::Permissions::from_mode(0o755));
            }
        }

        Ok(())
    }
}

const DDL_TEMPLATE: &str = r#"-- 1. Physical Table: This holds the raw data.
CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.{physical_name}` (
{column_definitions}{hash_column}{primary_key}
)
{partitioning}
{clustering};

-- 2. Logical View: This is what analysts should query.
CREATE OR REPLACE VIEW `{project_id}.{dataset_id}.{table_name}` AS SELECT
{view_columns}{view_hash}
FROM `{project_id}.{dataset_id}.{physical_name}`;
"#;

impl ArtifactAdapter {
    /// Generates optimized BigQuery DDL.
    fn generate_bq_ddl(&self, metadata: &TableMetadata, enable_row_hash: bool) -> String {
        let physical_name = format!("{}_PHYSICAL", metadata.table_name);

        // Definitions for columns that actually exist on disk (no virtual columns).
        let column_definitions = metadata
            .columns
            .iter()
            .filter(|c| !c.is_virtual)
            .map(|c| {
                let desc = c
                    .comment
                    .as_ref()
                    .map(|cc| format!(" OPTIONS(description=\"{}\")", cc.replace("\"", "\\\"")))
                    .unwrap_or_default();
                format!("  {} {}{}", c.name, c.bq_type, desc)
            })
            .collect::<Vec<_>>()
            .join(",\n");

        let hash_column = if enable_row_hash {
            ",\n  ROW_HASH STRING NOT NULL OPTIONS(description=\"SHA256 hash for duplicate detection\")"
        } else {
            ""
        };
        let primary_key = if !metadata.pk_cols.is_empty() {
            format!(
                ",\n  PRIMARY KEY ({}) NOT ENFORCED",
                metadata.pk_cols.join(", ")
            )
        } else {
            "".to_string()
        };

        // Handle Partitioning (BigQuery's way of splitting data by day/month).
        let partitioning = metadata
            .columns
            .iter()
            .find(|c| {
                metadata.partition_cols.contains(&c.name)
                    && (c.bq_type.contains("DATE") || c.bq_type.contains("TIMESTAMP"))
            })
            .map(|c| format!("PARTITION BY {}", c.name))
            .unwrap_or_default();

        // Handle Clustering (BigQuery's way of sorting data for faster searches).
        let mut cluster_cols = metadata.pk_cols.clone();
        for idx in &metadata.index_cols {
            if !cluster_cols.contains(idx) {
                cluster_cols.push(idx.clone());
            }
        }
        let clustering = if !cluster_cols.is_empty() {
            format!(
                "CLUSTER BY {}",
                cluster_cols[0..std::cmp::min(4, cluster_cols.len())].join(", ")
            )
        } else {
            "".to_string()
        };

        // The View includes all columns + any virtual column logic.
        let view_columns = metadata
            .columns
            .iter()
            .map(|c| {
                if c.is_virtual {
                    format!("  CAST(`{}` AS STRING) AS {}", c.name, c.name)
                } else {
                    format!("  {}", c.name)
                }
            })
            .collect::<Vec<_>>()
            .join(",\n");
        let view_hash = if enable_row_hash { ",\n  ROW_HASH" } else { "" };

        DDL_TEMPLATE
            .replace("{project_id}", &self.project_id)
            .replace("{dataset_id}", &self.dataset_id)
            .replace("{physical_name}", &physical_name)
            .replace("{table_name}", &metadata.table_name)
            .replace("{column_definitions}", &column_definitions)
            .replace("{hash_column}", hash_column)
            .replace("{primary_key}", &primary_key)
            .replace("{partitioning}", &partitioning)
            .replace("{clustering}", &clustering)
            .replace("{view_columns}", &view_columns)
            .replace("{view_hash}", view_hash)
    }

    /// Generates BigQuery Schema JSON.
    fn generate_schema_json(
        &self,
        metadata: &TableMetadata,
        enable_row_hash: bool,
    ) -> serde_json::Value {
        let mut fields: Vec<_> = metadata.columns.iter()
            .filter(|c| !c.is_virtual)
            .map(|c| json!({"name": c.name, "type": c.bq_type, "mode": "NULLABLE", "description": c.comment}))
            .collect();
        if enable_row_hash {
            fields.push(json!({"name": "ROW_HASH", "type": "STRING", "mode": "REQUIRED", "description": "SHA256 hash for duplicate detection"}));
        }
        json!(fields)
    }

    /// Generates a bash script with the `bq load` command.
    fn generate_load_command(&self, metadata: &TableMetadata, file_format: FileFormat) -> String {
        let (fmt, args, ext) = match file_format {
            FileFormat::Csv => (
                "CSV",
                "--field_delimiter=$'\\x10' --skip_leading_rows=1 --null_marker=''",
                "*.csv.gz",
            ),
            FileFormat::Parquet => ("PARQUET", "", "*.parquet"),
        };
        format!(
            "#!/bin/bash\n# BigQuery Load Command ({})\n# Run this from inside the config/ directory.\nset -e\nbq load --source_format={} {} --replace --schema=schema.json {}:{}.{}_PHYSICAL \"../data/{}\"\n",
            fmt, fmt, args, self.project_id, self.dataset_id, metadata.table_name, ext
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::entities::ColumnMetadata;

    #[test]
    fn test_generate_schema_json() {
        let adapter = ArtifactAdapter::new("p".into(), "d".into());
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
        let adapter = ArtifactAdapter::new("my-project".into(), "my-dataset".into());
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
        let adapter = ArtifactAdapter::new("p".into(), "d".into());
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
