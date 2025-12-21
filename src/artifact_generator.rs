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
pub fn save_all_artifacts(
    config_dir: &Path,
    schema: &str,
    table: &str,
    columns: &[String],
    col_types: &[OracleType],
    raw_types: &[String],
    stats: &ValidationStats,
    oracle_ddl: Option<&str>,
    duration_secs: f64,
    enable_row_hash: bool,
    field_delimiter: &str,
    project: &str,
    dataset: &str,
    pk_cols: Option<&[String]>,
    partition_cols: &[String],
    index_cols: &[String],
    gcs_bucket: Option<&str>,
) -> std::io::Result<()> {
    // 1. Generate BigQuery DDL
    let bq_ddl = generate_bigquery_ddl(project, dataset, table, columns, col_types, raw_types, enable_row_hash, pk_cols, partition_cols, index_cols);
    write_file(config_dir.join("bigquery.ddl"), &bq_ddl)?;

    // 1b. Generate BigQuery Schema JSON
    let schema_path = config_dir.join("schema.json");
    if let Err(e) = crate::bigquery_schema_mapper::generate_schema(columns, col_types, raw_types, schema_path.to_str().unwrap(), enable_row_hash) {
        warn!("Failed to generate schema.json: {}", e);
    }
    
    // 1c. Generate Column Mapping Doc (Parity)
    let mapping_doc = generate_column_mapping_doc(schema, table, columns, col_types, raw_types, enable_row_hash);
    write_file(config_dir.join("column_mapping.md"), &mapping_doc)?;

    // 2. Save Oracle DDL
    let oracle_ddl_content = oracle_ddl.unwrap_or("-- No Oracle DDL available");
    write_file(config_dir.join("oracle.ddl"), oracle_ddl_content)?;

    // 3. Generate Load Command (shell script)
    let load_cmd = generate_load_command(project, dataset, table, field_delimiter, gcs_bucket);
    write_file(config_dir.join("load_command.sh"), &load_cmd)?;
    // Make executable? In Docker/Linux, we can try, but std::fs::set_permissions is unix specific.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let p = config_dir.join("load_command.sh");
        if let Ok(f) = File::open(&p) {
             let _ = f.set_permissions(std::fs::Permissions::from_mode(0o755));
        }
    }

    // 4. Generate Validation SQL
    let val_sql = generate_validation_sql(project, dataset, table, stats);
    write_file(config_dir.join("validation.sql"), &val_sql)?;

    // 5. Generate Reference Export SQL
    let export_sql = generate_export_sql(schema, table, columns, col_types, raw_types, enable_row_hash, field_delimiter);
    write_file(config_dir.join("export.sql"), &export_sql)?;

    // 6. Generate Metadata JSON (Expanded)
    let metadata = json!({
        "table_name": table,
        "schema_name": schema,
        "full_name": format!("{}.{}", schema, table),
        "export_timestamp": chrono::Utc::now().to_rfc3339(),
        "validation": stats,
        "export_duration_seconds": duration_secs,
        "columns": generate_column_metadata(columns, col_types, raw_types, enable_row_hash),
        "oracle_ddl_file": "oracle.ddl",
        "bigquery_ddl_file": "bigquery.ddl"
    });
    let metadata_path = config_dir.join("metadata.json");
    let f = File::create(metadata_path)?;
    serde_json::to_writer_pretty(f, &metadata)?;

    info!("Saved all artifacts to {:?}", config_dir);
    Ok(())
}

fn write_file(path: PathBuf, content: &str) -> std::io::Result<()> {
    let mut f = File::create(&path)?;
    f.write_all(content.as_bytes())?;
    Ok(())
}

fn generate_bigquery_ddl(
    project: &str, 
    dataset: &str, 
    table: &str, 
    columns: &[String], 
    col_types: &[OracleType], 
    raw_strings: &[String], 
    enable_row_hash: bool,
    pk_cols: Option<&[String]>,
    partition_cols: &[String],
    index_cols: &[String]
) -> String {
    let mut lines = Vec::new();
    lines.push(format!("CREATE OR REPLACE TABLE `{}.{}.{}` (", project, dataset, table));
    
    // Column Definitions
    for (i, ((name, otype), rtype)) in columns.iter().zip(col_types.iter()).zip(raw_strings.iter()).enumerate() {
        let bq_type = bigquery_schema_mapper::map_oracle_to_bq_ddl(otype, Some(rtype));
        let comma = if i < columns.len() - 1 || enable_row_hash || pk_cols.is_some() { "," } else { "" };
        lines.push(format!("  {} {}{}", name, bq_type, comma));
    }
    
    if enable_row_hash {
        let comma = if pk_cols.is_some() { "," } else { "" };
        lines.push(format!("  ROW_HASH STRING NOT NULL{}", comma));
    }

    // Primary Key (Not Enforced)
    if let Some(pks) = pk_cols {
        let pk_list = pks.join(", ");
        lines.push(format!("  PRIMARY KEY ({}) NOT ENFORCED", pk_list));
    }
    
    lines.push(")".to_string());

    // Partitioning & Clustering Logic
    let mut partition_by_clause = None;
    let mut cluster_candidates = Vec::new();

    // 1. Identify Partition Column
    // BQ supports only ONE partition column (usually). We pick the first valid one.
    // If it's a STRING, we treat it as a candidate for Clustering instead.
    for p_col in partition_cols {
        // Find type
        if let Some(pos) = columns.iter().position(|c| c.eq_ignore_ascii_case(p_col)) {
             let otype = &col_types[pos];
             let rtype = &raw_strings[pos];
             let bq_type = bigquery_schema_mapper::map_oracle_to_bq_ddl(otype, Some(rtype));
             
             // Check compatibility
             if bq_type.starts_with("TIMESTAMP") || bq_type.starts_with("DATE") || bq_type.starts_with("DATETIME") {
                 if partition_by_clause.is_none() {
                     partition_by_clause = Some(format!("PARTITION BY {}", p_col));
                 }
             } else if bq_type.starts_with("INT") || bq_type.starts_with("INTEGER") || bq_type.contains("NUMERIC") || bq_type.contains("DECIMAL") {
                 // Integer Range Partitioning
                 // We use a safe default range. In production, users should adjust this.
                 if partition_by_clause.is_none() {
                     // Default: 0 to 1M with step 1000 (1000 partitions)
                     partition_by_clause = Some(format!("PARTITION BY RANGE_BUCKET({}, GENERATE_ARRAY(0, 1000000, 1000))", p_col));
                 }
             } else {
                 // Strings etc -> Clustering
                 cluster_candidates.push(p_col.clone());
             }
        }
    }

    if let Some(clause) = partition_by_clause {
        lines.push(clause);
    }

    // 2. Build Clustering Keys
    // Order: PKs -> Indexes -> (Partition Keys that were demoted)
    let mut final_cluster_keys = Vec::new();
    let mut seen = std::collections::HashSet::new();

    // Add PKs
    if let Some(pks) = pk_cols {
        for pk in pks {
            if !seen.contains(pk) {
                final_cluster_keys.push(pk.clone());
                seen.insert(pk.clone());
            }
        }
    }

    // Add Indexes
    for idx in index_cols {
         if !seen.contains(idx) {
             final_cluster_keys.push(idx.clone());
             seen.insert(idx.clone());
         }
    }

    // Add Demoted Partition Keys
    for p in cluster_candidates {
        if !seen.contains(&p) {
            final_cluster_keys.push(p.clone());
            seen.insert(p.clone());
        }
    }

    // BQ Cap: 4 columns
    if !final_cluster_keys.is_empty() {
        let count = std::cmp::min(4, final_cluster_keys.len());
        let cluster_str = final_cluster_keys[0..count].join(", ");
        lines.push(format!("CLUSTER BY {}", cluster_str));
    }

    lines.push(";".to_string());
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

fn generate_column_metadata(columns: &[String], col_types: &[OracleType], raw_types: &[String], enable_row_hash: bool) -> serde_json::Value {
    let mut cols = Vec::new();
    for (i, (name, otype)) in columns.iter().zip(col_types.iter()).enumerate() {
        cols.push(json!({
            "name": name,
            "oracle_type": format!("{:?}", otype), // Debug representation
            "raw_type": raw_types[i],
            "bigquery_type": bigquery_schema_mapper::map_oracle_to_bq(otype, Some(&raw_types[i]))
        }));
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
