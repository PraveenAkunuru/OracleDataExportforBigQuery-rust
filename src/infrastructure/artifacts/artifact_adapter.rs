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

impl ArtifactAdapter {
    /// Generates optimized BigQuery DDL.
    fn generate_bq_ddl(&self, metadata: &TableMetadata, enable_row_hash: bool) -> String {
        let needs_view = metadata.needs_view();
        let physical_name = if needs_view {
            format!("{}_PHYSICAL", metadata.table_name)
        } else {
            metadata.table_name.clone()
        };

        // 1. Physical Table Definitions (Data on disk)
        let column_definitions = metadata
            .columns
            .iter()
            .filter(|c| !c.is_virtual || c.is_transformed)
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

        let partitioning = metadata
            .columns
            .iter()
            .find(|c| {
                metadata.partition_cols.contains(&c.name)
                    && (c.bq_type.contains("DATE") || c.bq_type.contains("TIMESTAMP"))
            })
            .map(|c| {
                if c.bq_type == "DATETIME" {
                    format!("PARTITION BY DATETIME_TRUNC({}, DAY)", c.name)
                } else if c.bq_type == "TIMESTAMP" {
                    format!("PARTITION BY TIMESTAMP_TRUNC({}, DAY)", c.name)
                } else {
                    format!("PARTITION BY {}", c.name)
                }
            })
            .unwrap_or_default();

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

        let table_ddl = format!(
            "CREATE OR REPLACE TABLE `{}.{}.{}` (\n{}{}{}\n)\n{}\n{};",
            self.project_id,
            self.dataset_id,
            physical_name,
            column_definitions,
            hash_column,
            primary_key,
            partitioning,
            clustering
        );

        if !needs_view {
            return format!("-- Physical Table: Holds the raw data.\n{}", table_ddl);
        }

        // 2. Logical View (Only if virtual columns exist)
        let view_columns = metadata
            .columns
            .iter()
            .map(|c| {
                if c.is_virtual && !c.is_transformed {
                    let mut expr = c
                        .virtual_expr
                        .as_deref()
                        .unwrap_or("NULL")
                        .replace('"', "`");

                    // Basic translation for common Oracle functions
                    if expr.contains("TO_CHAR(") {
                        expr = expr.replace("TO_CHAR(", "CAST(");
                        // Find the first closing paren after the CAST we just added
                        if let Some(cast_start) = expr.find("CAST(") {
                            if let Some(paren_offset) = expr[cast_start..].find(')') {
                                expr.insert_str(cast_start + paren_offset, " AS STRING");
                            }
                        }
                    }

                    format!("  {} AS {}", expr, c.name)
                } else {
                    // Regular or transformed column already exists in physical table.
                    format!("  {}", c.name)
                }
            })
            .collect::<Vec<_>>()
            .join(",\n");

        let view_hash = if enable_row_hash { ",\n  ROW_HASH" } else { "" };
        let view_ddl = format!(
            "CREATE OR REPLACE VIEW `{}.{}.{}` AS SELECT\n{}{}\nFROM `{}.{}.{}`;",
            self.project_id,
            self.dataset_id,
            metadata.table_name,
            view_columns,
            view_hash,
            self.project_id,
            self.dataset_id,
            physical_name
        );

        format!(
            "-- 1. Physical Table\n{}\n\n-- 2. Logical View\n{}",
            table_ddl, view_ddl
        )
    }

    /// Generates BigQuery Schema JSON.
    fn generate_schema_json(
        &self,
        metadata: &TableMetadata,
        enable_row_hash: bool,
    ) -> serde_json::Value {
        let mut fields: Vec<_> = metadata.columns.iter()
            .filter(|c| !c.is_virtual || c.is_transformed)
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
        let table_name = if metadata.needs_view() {
            format!("{}_PHYSICAL", metadata.table_name)
        } else {
            metadata.table_name.clone()
        };
        format!(
            "#!/bin/bash\n# BigQuery Load Command ({})\n# Run this from inside the config/ directory.\nset -e\nbq load --source_format={} {} --replace --schema=schema.json {}:{}.{} ../data/{}\n",
            fmt, fmt, args, self.project_id, self.dataset_id, table_name, ext
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
                    virtual_expr: None,
                    is_transformed: false,
                    is_hidden: false,
                    is_identity: false,
                    comment: Some("C1 comment".into()),
                },
                ColumnMetadata {
                    name: "V1".into(),
                    raw_type: "NUMBER".into(),
                    bq_type: "INTEGER".into(),
                    is_virtual: true,
                    virtual_expr: None,
                    is_transformed: false,
                    is_hidden: false,
                    is_identity: false,
                    comment: None,
                },
                ColumnMetadata {
                    name: "T1".into(),
                    raw_type: "XMLTYPE".into(),
                    bq_type: "STRING".into(),
                    is_virtual: true,
                    virtual_expr: None,
                    is_transformed: true,
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

        // Should have C1, T1, and ROW_HASH (V1 is native virtual, so excluded. T1 is virtual+transformed, so included)
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0]["name"], "C1");
        assert_eq!(fields[1]["name"], "T1");
        assert_eq!(fields[2]["name"], "ROW_HASH");
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
                    virtual_expr: None,
                    is_transformed: false,
                    is_hidden: false,
                    is_identity: false,
                    comment: None,
                },
                ColumnMetadata {
                    name: "NATIVE_V".into(),
                    raw_type: "VARCHAR2".into(),
                    bq_type: "STRING".into(),
                    is_virtual: true,
                    virtual_expr: Some("COL_A + COL_B".into()),
                    is_transformed: false,
                    is_hidden: false,
                    is_identity: false,
                    comment: None,
                },
                ColumnMetadata {
                    name: "TRANSFORMED_V".into(),
                    raw_type: "XMLTYPE".into(),
                    bq_type: "STRING".into(),
                    is_virtual: true,
                    virtual_expr: None,
                    is_transformed: true,
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
        assert!(ddl.contains("TRANSFORMED_V STRING")); // Included because it is transformed
        assert!(!ddl.contains("NATIVE_V STRING")); // Excluded because it is ONLY virtual
        assert!(ddl.contains("CREATE OR REPLACE VIEW `my-project.my-dataset.MY_TABLE`"));
        assert!(ddl.contains("COL_A + COL_B AS NATIVE_V"));
    }

    #[test]
    fn test_generate_bq_ddl_no_view() {
        let adapter = ArtifactAdapter::new("my-project".into(), "my-dataset".into());
        let meta = TableMetadata {
            schema: "TEST".into(),
            table_name: "SIMPLE_TABLE".into(),
            columns: vec![ColumnMetadata {
                name: "ID".into(),
                raw_type: "NUMBER".into(),
                bq_type: "INT64".into(),
                is_virtual: false,
                virtual_expr: None,
                is_transformed: false,
                is_hidden: false,
                is_identity: false,
                comment: None,
            }],
            size_gb: 0.1,
            pk_cols: vec!["ID".into()],
            partition_cols: vec![],
            index_cols: vec![],
        };

        let ddl = adapter.generate_bq_ddl(&meta, false);

        assert!(ddl.contains("CREATE OR REPLACE TABLE `my-project.my-dataset.SIMPLE_TABLE`"));
        assert!(!ddl.contains("VIEW"));
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
