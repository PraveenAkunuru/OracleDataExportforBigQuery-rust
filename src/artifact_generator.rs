//! # Artifacts Module
//!
//! Responsible for generating all "sidecar" files and documentation for an exported table.
//!
//! ## Generated Files
//! - `bigquery.ddl`: `CREATE TABLE` statement for BQ.
//! - `schema.json`: JSON Schema for BQ Load jobs.
//! - `metadata.json`: Comprehensive structured metadata (stats, columns, timings).
//! - `oracle.ddl`: Original Oracle DDL (for reference).
//! - `load_command.sh`: Ready-to-run `bq load` shell command.
//! - `export.sql`: SQLcl-compatible script to reproduce the export manually.
//! - `validation.sql`: SQL script to verify data integrity in BigQuery after load.

use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use log::{info, warn};
use oracle::sql_type::OracleType;

use serde_json::json;

use crate::data_validator::ValidationStats;
use crate::bigquery_schema_mapper;

/// Generates and saves all auxiliary artifacts to the `config` directory.
/// Parameters for artifact generation
pub struct ArtifactParams<'a> {
    pub config_dir: &'a Path,
    pub schema: &'a str,
    pub table: &'a str,
    pub columns: &'a [String],
    pub col_types: &'a [OracleType],
    pub raw_types: &'a [String],
    pub col_comments: &'a [Option<String>],
    pub virtual_cols_map: &'a std::collections::HashMap<String, String>,
    pub stats: &'a ValidationStats,
    pub oracle_ddl: Option<&'a str>,
    pub duration_secs: f64,
    pub enable_row_hash: bool,
    pub field_delimiter: &'a str,
    pub project: &'a str,
    pub dataset: &'a str,
    pub pk_cols: Option<&'a [String]>,
    pub partition_cols: &'a [String],
    pub index_cols: &'a [String],
    pub gcs_bucket: Option<&'a str>,
}

/// Generates and saves all auxiliary artifacts to the `config` directory.
pub fn save_all_artifacts(params: ArtifactParams) -> std::io::Result<()> {
    // 1. Generate BigQuery DDL
    let bq_ddl = generate_bigquery_ddl(&params);
    write_file(params.config_dir.join("bigquery.ddl"), &bq_ddl)?;

    // 1b. Generate BigQuery Schema JSON
    let schema_path = params.config_dir.join("schema.json");
    if let Err(e) = crate::bigquery_schema_mapper::generate_schema(
        params.columns, 
        params.col_types, 
        params.raw_types, 
        params.col_comments,
        schema_path.to_str().unwrap(), 
        params.enable_row_hash
    ) {
        warn!("Failed to generate schema.json: {}", e);
    }
    
    // 1c. Generate Column Mapping Doc (Parity)
    let mapping_doc = generate_column_mapping_doc(
        params.schema, 
        params.table, 
        params.columns, 
        params.col_types, 
        params.raw_types, 
        params.enable_row_hash
    );
    write_file(params.config_dir.join("column_mapping.md"), &mapping_doc)?;

    // 2. Save Oracle DDL
    let oracle_ddl_content = params.oracle_ddl.unwrap_or("-- No Oracle DDL available");
    write_file(params.config_dir.join("oracle.ddl"), oracle_ddl_content)?;

    // 3. Generate Load Command (shell script)
    // If Virtual Columns OR Spatial Columns exist, the CSV data matches the PHYSICAL table.
    let has_spatial = params.raw_types.iter().any(|t| t.to_uppercase().contains("SDO_GEOMETRY"));
    let load_table_name = if !params.virtual_cols_map.is_empty() || has_spatial {
        format!("{}_PHYSICAL", params.table)
    } else {
        params.table.to_string()
    };

    let load_cmd = generate_load_command(
        params.project, 
        params.dataset, 
        &load_table_name, 
        params.field_delimiter, 
        params.gcs_bucket
    );
    write_file(params.config_dir.join("load_command.sh"), &load_cmd)?;
    // Make executable
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let p = params.config_dir.join("load_command.sh");
        if let Ok(f) = File::open(&p) {
             let _ = f.set_permissions(std::fs::Permissions::from_mode(0o755));
        }
    }

    // 4. Generate Validation SQL
    let val_sql = generate_validation_sql(params.project, params.dataset, params.table, params.stats);
    write_file(params.config_dir.join("validation.sql"), &val_sql)?;

    // 5. Generate Reference Export SQL
    let export_sql = generate_export_sql(
        params.schema, 
        params.table, 
        params.columns, 
        params.col_types, 
        params.raw_types, 
        params.enable_row_hash, 
        params.field_delimiter
    );
    write_file(params.config_dir.join("export.sql"), &export_sql)?;

    // 6. Generate Metadata JSON (Expanded)
    let metadata = json!({
        "table_name": params.table,
        "schema_name": params.schema,
        "full_name": format!("{}.{}", params.schema, params.table),
        "export_timestamp": chrono::Utc::now().to_rfc3339(),
        "validation": params.stats,
        "export_duration_seconds": params.duration_secs,
        "columns": generate_column_metadata(params.columns, params.col_types, params.raw_types, params.col_comments, params.enable_row_hash),
        "oracle_ddl_file": "oracle.ddl",
        "bigquery_ddl_file": "bigquery.ddl"
    });
    let metadata_path = params.config_dir.join("metadata.json");
    let f = File::create(metadata_path)?;
    serde_json::to_writer_pretty(f, &metadata)?;

    info!("Saved all artifacts to {:?}", params.config_dir);
    Ok(())
}

fn write_file(path: PathBuf, content: &str) -> std::io::Result<()> {
    let mut f = File::create(&path)?;
    f.write_all(content.as_bytes())?;
    Ok(())
}

fn generate_bigquery_ddl(params: &ArtifactParams) -> String {
    let project = params.project;
    let dataset = params.dataset;
    let table = params.table;
    let columns = params.columns;
    let col_types = params.col_types;
    let raw_strings = params.raw_types;
    let col_comments = params.col_comments;
    let virtual_cols_map = params.virtual_cols_map;
    let enable_row_hash = params.enable_row_hash;
    let pk_cols = params.pk_cols;
    let partition_cols = params.partition_cols;
    let index_cols = params.index_cols;
    let mut lines = Vec::new();
    
    // --- 1. Physical Table DDL (Excluding Virtual Columns) ---
    // Virtual columns in BigQuery are best handled as Views or Computed Columns (limited support).
    // Given the request to "Create DDL + View", we will create the backing table with only stored columns.

    let physical_table_name = format!("{}_PHYSICAL", table);
    let full_physical_name = format!("`{}.{}.{}`", project, dataset, physical_table_name);
    
    lines.push("-- 1. Physical Table (Stored Data)".to_string());
    lines.push(format!("CREATE OR REPLACE TABLE {} (", full_physical_name));
    
    let mut stored_cols_indices = Vec::new();

    // Identify Stored Columns
    for (i, name) in columns.iter().enumerate() {
        if !virtual_cols_map.contains_key(&name.to_uppercase()) {
            stored_cols_indices.push(i);
        }
    }

    // Column Definitions (Stored Only)
    for (idx, &opt_i) in stored_cols_indices.iter().enumerate() {
        let name = &columns[opt_i];
        let otype = &col_types[opt_i];
        let rtype = &raw_strings[opt_i];
        let bq_type = bigquery_schema_mapper::map_oracle_to_bq_ddl(otype, Some(rtype));
        
        // Handle Options (Description)
        let options_clause = if let Some(Some(comment)) = col_comments.get(opt_i) {
             let escaped = comment.replace("\"", "\\\"");
             format!(" OPTIONS(description=\"{}\")", escaped)
        } else {
             String::new()
        };

        let comma = if idx < stored_cols_indices.len() - 1 || enable_row_hash || pk_cols.is_some() { "," } else { "" };
        lines.push(format!("  {} {}{}{}", name, bq_type, options_clause, comma));
    }
    
    if enable_row_hash {
        let comma = if pk_cols.is_some() { "," } else { "" };
        lines.push(format!("  ROW_HASH STRING NOT NULL OPTIONS(description=\"SHA256 hash for duplicate detection\"){}", comma));
    }

    // Primary Key (Not Enforced) - Only include if all PK cols are physical (usually true)
    if let Some(pks) = pk_cols {
        // Filter PKs to ensure they exist in physical table
        let physical_pks: Vec<&String> = pks.iter().filter(|pk| !virtual_cols_map.contains_key(&pk.to_uppercase())).collect();
        if !physical_pks.is_empty() {
             let pk_list = physical_pks.iter().map(|s| s.as_str()).collect::<Vec<&str>>().join(", ");
             lines.push(format!("  PRIMARY KEY ({}) NOT ENFORCED", pk_list));
        }
    }
    
    lines.push(")".to_string());

    // Partitioning & Clustering Logic (For Physical Table)
    let mut partition_by_clause = None;
    let mut cluster_candidates = Vec::new();

    // 1. Identify Partition Column
    for p_col in partition_cols {
        if let Some(pos) = columns.iter().position(|c| c.eq_ignore_ascii_case(p_col)) {
             // Only if physical
             if virtual_cols_map.contains_key(&columns[pos].to_uppercase()) { continue; }

             let otype = &col_types[pos];
             let rtype = &raw_strings[pos];
             let bq_type = bigquery_schema_mapper::map_oracle_to_bq_ddl(otype, Some(rtype));
             
             if bq_type.starts_with("TIMESTAMP") || bq_type.starts_with("DATE") || bq_type.starts_with("DATETIME") {
                 if partition_by_clause.is_none() {
                     partition_by_clause = Some(format!("PARTITION BY {}", p_col));
                 }
             } else if bq_type.starts_with("INT") || bq_type.starts_with("INTEGER") || bq_type.contains("NUMERIC") || bq_type.contains("DECIMAL") {
                 if partition_by_clause.is_none() {
                     partition_by_clause = Some(format!("PARTITION BY RANGE_BUCKET({}, GENERATE_ARRAY(0, 1000000, 1000))", p_col));
                 }
             } else {
                 cluster_candidates.push(p_col.clone());
             }
        }
    }

    if let Some(clause) = partition_by_clause {
        lines.push(clause);
    }

    // 2. Build Clustering Keys
    let mut final_cluster_keys = Vec::new();
    let mut seen = std::collections::HashSet::new();

    if let Some(pks) = pk_cols {
        for pk in pks {
            // Only physical
            if virtual_cols_map.contains_key(&pk.to_uppercase()) { continue; }

            if !seen.contains(pk) {
                final_cluster_keys.push(pk.clone());
                seen.insert(pk.clone());
            }
        }
    }

    for idx in index_cols {
         if virtual_cols_map.contains_key(&idx.to_uppercase()) { continue; }
         if !seen.contains(idx) {
             final_cluster_keys.push(idx.clone());
             seen.insert(idx.clone());
         }
    }

    for p in cluster_candidates {
        if !seen.contains(&p) {
            final_cluster_keys.push(p.clone());
            seen.insert(p.clone());
        }
    }

    if !final_cluster_keys.is_empty() {
        let count = std::cmp::min(4, final_cluster_keys.len());
        let cluster_str = final_cluster_keys[0..count].join(", ");
        lines.push(format!("CLUSTER BY {}", cluster_str));
    }

    lines.push(";".to_string());
    lines.push("".to_string());
    
    // --- 2. Logical View DDL (Including Virtual Columns) ---
    // This view restores the original table interface
    let full_view_name = format!("`{}.{}.{}`", project, dataset, table);
    lines.push("-- 2. Logical View (Virtual Columns Restored)".to_string());
    lines.push(format!("CREATE OR REPLACE VIEW {} AS SELECT", full_view_name));
    
    for (i, name) in columns.iter().enumerate() {
        let select_expr = if let Some(expr) = virtual_cols_map.get(&name.to_uppercase()) {
            // Translate Oracle Expressions to BigQuery
            // 1. Handle TO_CHAR(x) -> CAST(x AS STRING)
            let re = regex::Regex::new(r"TO_CHAR\(([^)]+)\)").unwrap();
            let translated = re.replace_all(expr, "CAST($1 AS STRING)").to_string();
            
            // 2. Handle identifier quoting: Oracle "ID" -> BQ `ID`
            let translated = translated.replace("\"", "`"); 

            format!("  /* Virtual: {} */ {} AS {},", expr, translated, name)
        } else {
             // Check if it's SDO_GEOMETRY using raw_strings
             if raw_strings[i].to_uppercase().contains("SDO_GEOMETRY") {
                 format!("  SAFE.ST_GEOGFROMTEXT({}) AS {},", name, name)
             } else {
                 format!("  {},", name)
             }
        };
        lines.push(select_expr);
    }
    
    if enable_row_hash {
        lines.push("  ROW_HASH".to_string());
    } else {
        // Remove trailing comma from last item if no row has
        if let Some(last) = lines.last_mut() {
            if last.ends_with(",") {
                last.pop(); 
            }
        }
    }
    
    lines.push(format!("FROM {};", full_physical_name));

    lines.join("\n")
}

fn generate_load_command(project: &str, dataset: &str, table: &str, delimiter: &str, gcs_bucket: Option<&str>) -> String {
    // bq load requires special syntax for hex/non-printable chars
    let bq_delimiter = if delimiter == "\u{0010}" {
        "$'\\x10'".to_string()
    } else {
        format!("'{}'", delimiter)
    };

    let bucket_logic = if let Some(bucket) = gcs_bucket {
        format!(r#"
    # GCS Upload Enabled
    GCS_URI="gs://{}/{}/{}/{}/data/$(basename "$f")"
    echo "Uploading $f to $GCS_URI..."
    gcloud storage cp "$f" "$GCS_URI"
    LOAD_SOURCE="$GCS_URI"
"#, bucket, project, dataset, table)
    } else {
        r#"
    LOAD_SOURCE="$f"
"#.to_string()
    };

    format!(r#"#!/bin/bash
# BigQuery Load Command
# Generated by Oracle Rust Exporter

# Iterate over chunk files to handle headers and compression correctly
# First file replaces the table, subsequent files append.

first=1
for f in ../data/*.csv.gz; do
  if [ ! -f "$f" ]; then continue; fi
  {}

  if [ "$first" -eq 1 ]; then
    echo "Loading initial chunk (REPLACE): $f"
    bq load \
        --source_format=CSV \
        --field_delimiter={} \
        --preserve_ascii_control_characters=true \
        --skip_leading_rows=1 \
        --null_marker='' \
        --replace \
        --allow_jagged_rows=false \
        --schema=schema.json \
        {}:{}.{} \
        "$LOAD_SOURCE"
    first=0
  else
    echo "Loading chunk (APPEND): $f"
    bq load \
        --source_format=CSV \
        --field_delimiter={} \
        --preserve_ascii_control_characters=true \
        --skip_leading_rows=1 \
        --null_marker='' \
        --noreplace \
        --allow_jagged_rows=false \
        --schema=schema.json \
        {}:{}.{} \
        "$LOAD_SOURCE"
  fi
  
  if [ $? -ne 0 ]; then
     echo "Load failed for $f"
     exit 1
  fi
done
"#, 
    bucket_logic,
    bq_delimiter, project, dataset, table, 
    bq_delimiter, project, dataset, table
    )
}

fn generate_validation_sql(project: &str, dataset: &str, table: &str, stats: &ValidationStats) -> String {
    let row_count = stats.row_count;
    let mut sql = format!(r#"-- Validation SQL for {}.{}
-- Goal: Rapidly verify that BigQuery ingestion matches Oracle source metadata.

-- ASSERT that the row count matches exactly
ASSERT (
  SELECT COUNT(*) FROM `{}.{}.{}`
) = {} 
AS "Row count mismatch for {}! Check BigQuery table for actual count.";

-- Success message
SELECT "SUCCESS: Row count matches expected {} for {}.{}" as validation_result;
"#, dataset, table, project, dataset, table, row_count, table, row_count, dataset, table);

    // Add Aggregate Checks (if any)
    if let Some(aggs) = &stats.aggregates {
        for agg in aggs {
            if agg.agg_type == "SUM" {
                sql.push_str(&format!(r#"
SELECT 
    '{}_SUM' as check_type,
    SUM({}) as bq_value,
    {} as oracle_value,
    CASE 
        WHEN ABS(COALESCE(SUM({}), 0) - {}) < 0.01 THEN 'PASS'
        ELSE 'FAIL'
    END as status
FROM `{}.{}.{}`;
"#, agg.column_name, agg.column_name, agg.value, agg.column_name, agg.value, project, dataset, table));
            }
        }
    }
    
    // PK Hash Check (Conceptual)
    // To properly check PK Hash in BigQuery, we'd need to compute ORA_HASH in BQ which doesn't exist identically.
    // So usually DVT uses `farm_fingerprint` or `md5`. 
    // Since we can't easily replicate ORA_HASH in BQ, we just log it as a comment for manual verification if tools align.
    if let Some(pk_hash) = &stats.pk_hash {
        sql.push_str(&format!(r#"
-- CHECK PK HASH (Manual)
-- Oracle PK Hash (SUM(ORA_HASH(PK))): {}
-- BigQuery equivalent requires custom UDF or specific logic matching Oracle's ORA_HASH
"#, pk_hash));
    }

    sql
}

fn generate_export_sql(schema: &str, table: &str, columns: &[String], col_types: &[OracleType], raw_types: &[String], enable_row_hash: bool, delimiter: &str) -> String {
    let mut select_exprs = Vec::new();


    for (i, (name, _otype)) in columns.iter().zip(col_types.iter()).enumerate() {
        let rtype = &raw_types[i].to_uppercase();
        // 1. Column Expression (Matching main exporter logic for parity)
        let expr = if rtype.contains("TIME ZONE") {
            format!("TO_CHAR(SYS_EXTRACT_UTC(\"{}\"), 'YYYY-MM-DD\"T\"HH24:MI:SS.FF6\"Z\"')", name)
        } else if rtype.contains("INTERVAL YEAR") {
            format!("CASE WHEN \"{}\" IS NULL THEN NULL ELSE CASE WHEN EXTRACT(YEAR FROM \"{}\") < 0 OR EXTRACT(MONTH FROM \"{}\") < 0 THEN '-' ELSE '' END || ABS(EXTRACT(YEAR FROM \"{}\")) || '-' || ABS(EXTRACT(MONTH FROM \"{}\")) || ' 0 0:0:0' END", name, name, name, name, name)
        } else if rtype.contains("INTERVAL DAY") {
            format!("CASE WHEN \"{}\" IS NULL THEN NULL ELSE '0-0 ' || CASE WHEN EXTRACT(DAY FROM \"{}\") < 0 OR EXTRACT(HOUR FROM \"{}\") < 0 OR EXTRACT(MINUTE FROM \"{}\") < 0 OR EXTRACT(SECOND FROM \"{}\") < 0 THEN '-' ELSE '' END || ABS(EXTRACT(DAY FROM \"{}\")) || ' ' || ABS(EXTRACT(HOUR FROM \"{}\")) || ':' || ABS(EXTRACT(MINUTE FROM \"{}\")) || ':' || ABS(EXTRACT(SECOND FROM \"{}\")) END", name, name, name, name, name, name, name, name, name)
        } else if rtype == "XMLTYPE" {
            format!("REPLACE(REPLACE(sys.XMLType.getStringVal(\"{}\"), CHR(10), ''), CHR(13), '')", name)
        } else if rtype == "JSON" {
            format!("REPLACE(REPLACE(JSON_SERIALIZE(\"{}\"), CHR(10), ''), CHR(13), '')", name)
        } else if rtype == "BOOLEAN" {
            format!("CASE WHEN \"{}\" THEN 'true' ELSE 'false' END", name)
        } else {
            format!("\"{}\"", name)
        };
        select_exprs.push(expr);
    }

    // 2. Hash Part (Outside Loop since it aggregates all columns)
    if enable_row_hash {
        select_exprs.push(crate::sql_generator_utils::build_row_hash_select(columns, col_types));
    }

    let cols = select_exprs.join(", ");
    
    // SQLcl Script Format
    format!(r#"-- Reference Export Script for SQLcl
SET SQLFORMAT delimited
SET DELIMITER '{}'
SET FEEDBACK OFF
SET HEAD ON
SET TRIMSPOOL ON
SET PAGESIZE 50000

SPOOL {}_{}_fallback.csv

SELECT {} FROM "{}"."{}";

SPOOL OFF
"#, delimiter, schema, table, cols, schema, table)
}

fn generate_column_metadata(columns: &[String], col_types: &[OracleType], raw_types: &[String], col_comments: &[Option<String>], enable_row_hash: bool) -> serde_json::Value {
    let mut cols = Vec::new();
    for (i, (name, otype)) in columns.iter().zip(col_types.iter()).enumerate() {
        let mut obj = json!({
            "name": name,
            "oracle_type": format!("{:?}", otype), 
            "raw_type": raw_types[i],
            "bigquery_type": bigquery_schema_mapper::map_oracle_to_bq(otype, Some(&raw_types[i]))
        });
        if let Some(Some(comment)) = col_comments.get(i) {
            obj["description"] = json!(comment);
        }
        cols.push(obj);
    }
    if enable_row_hash {
        cols.push(json!({
            "name": "ROW_HASH",
            "oracle_type": "VARCHAR2 (Computed)",
            "bigquery_type": "STRING",
            "description": "SHA256 Hash of all columns"
        }));
    }
    serde_json::to_value(cols).unwrap()
}

fn generate_column_mapping_doc(schema: &str, table: &str, columns: &[String], col_types: &[OracleType], raw_strings: &[String], enable_row_hash: bool) -> String {
    let mut lines = Vec::new();
    lines.push(format!("# Column Mapping for {}.{}", schema, table));
    lines.push("".to_string());
    lines.push("| Oracle Column | Oracle Type | Raw Type | BigQuery Type |".to_string());
    lines.push("|--------------|-------------|----------|---------------|".to_string());
    
    for (i, (name, otype)) in columns.iter().zip(col_types.iter()).enumerate() {
         let bq_type = bigquery_schema_mapper::map_oracle_to_bq(otype, Some(&raw_strings[i]));
         lines.push(format!("| {} | {:?} | {} | {} |", name, otype, raw_strings[i], bq_type));
    }
    
    if enable_row_hash {
        lines.push("| ROW_HASH | (Computed) | STRING |".to_string());
    }
    
    lines.join("\n")
}
