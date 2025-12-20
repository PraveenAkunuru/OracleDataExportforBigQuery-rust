//! # BigQuery Module
//!
//! Handles type mapping and schema generation for BigQuery.
//!
//! ## Mapping Strategy
//! - `NUMBER(p,s)` -> `NUMERIC` or `BIGNUMERIC` depending on precision/scale.
//! - `DATE` -> `DATETIME` (No time zone in Oracle DATE).
//! - `TIMESTAMP` -> `DATETIME` (No time zone).
//! - `TIMESTAMP WITH TIME ZONE` -> `TIMESTAMP` (UTC).

use oracle::sql_type::OracleType;
use serde::Serialize;
use std::fs::File;

#[derive(Serialize)]
pub struct BigQueryField {
    pub name: String,
    pub r#type: String, // "type" is reserved in Rust
    pub mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

pub fn generate_schema(col_names: &[String], col_types: &[OracleType], raw_types: &[String], output_path: &str, enable_row_hash: bool) -> std::io::Result<()> {
    let mut fields = Vec::new();
    
    for i in 0..col_names.len() {
        let bq_type = map_oracle_to_bq(&col_types[i], Some(&raw_types[i]));
        fields.push(serde_json::json!({
            "name": col_names[i],
            "type": bq_type,
            "mode": "NULLABLE"
        }));
    }
    
    if enable_row_hash {
        fields.push(serde_json::json!({
            "name": "ROW_HASH",
            "type": "STRING",
            "mode": "REQUIRED",
             "description": "SHA256 hash for duplicate detection"
        }));
    }

    let file = File::create(output_path)?;
    serde_json::to_writer_pretty(file, &fields)?;
    Ok(())
}

/// Maps Oracle DB types to BigQuery equivalent types.
/// Incorporates both the rust-oracle OracleType and the raw DATA_TYPE string from Oracle metadata.
pub fn map_oracle_to_bq(oracle_type: &OracleType, raw_type: Option<&str>) -> String {
    // 1. Check raw type string first for advanced types (XMLTYPE, JSON, BOOLEAN)
    if let Some(r) = raw_type {
        let upper = r.to_uppercase();
        if upper.contains("XMLTYPE") { return "STRING".to_string(); }
        if upper.contains("JSON") { return "JSON".to_string(); }
        if upper.contains("BOOLEAN") { return "BOOL".to_string(); }
    }

    // 2. Fallback to OracleType variant matching
    match oracle_type {
        OracleType::Number(precision, scale) => {
            if *scale == 0 {
                if *precision > 0 && *precision <= 18 {
                    "INTEGER".to_string()
                } else {
                    "NUMERIC".to_string()
                }
            } else if *scale == -127 { 
                // FLOAT in Oracle
                "FLOAT64".to_string()
            } else {
                "NUMERIC".to_string()
            }
        },
        OracleType::Int64 => "INTEGER".to_string(),
        OracleType::Float(_) | OracleType::BinaryFloat | OracleType::BinaryDouble => "FLOAT64".to_string(),
        
        OracleType::Char(_) | OracleType::NChar(_) | 
        OracleType::Varchar2(_) | OracleType::NVarchar2(_) | 
        OracleType::Long | OracleType::CLOB | OracleType::NCLOB | OracleType::Rowid => "STRING".to_string(),
        
        OracleType::Date | OracleType::Timestamp(_) => "DATETIME".to_string(),
        OracleType::TimestampTZ(_) | OracleType::TimestampLTZ(_) => "TIMESTAMP".to_string(),
        
        OracleType::IntervalYM(..) | OracleType::IntervalDS(..) => "INTERVAL".to_string(),
        OracleType::Xml => "STRING".to_string(),
        // Oracle 23c+ BOOLEAN map to BigQuery BOOL
        OracleType::Boolean => "BOOL".to_string(),
        
        OracleType::Raw(_) | OracleType::BLOB | OracleType::BFILE => "BYTES".to_string(),
        
        _ => "STRING".to_string(), // Fallback
    }
}

/// Maps Oracle DB types to BigQuery SQL DDL types (e.g. NUMERIC(38,9)).
pub fn map_oracle_to_bq_ddl(oracle_type: &OracleType, raw_type: Option<&str>) -> String {
    // 1. Check raw type string first for advanced types (XMLTYPE, JSON, BOOLEAN)
    if let Some(r) = raw_type {
        let upper = r.to_uppercase();
        if upper.contains("XMLTYPE") { return "STRING".to_string(); }
        if upper.contains("JSON") { return "JSON".to_string(); }
        if upper.contains("BOOLEAN") { return "BOOL".to_string(); }
    }

    match oracle_type {
        OracleType::Number(prec, scale) => {
             // Logic for INT64 vs NUMERIC vs BIGNUMERIC
             if *scale == 0 {
                 if *prec > 18 { "NUMERIC".to_string() } else { "INT64".to_string() }
             } else {
                 let p = *prec;
                 let s = *scale;
                 
                 if (s > 0 && s <= 9) && (p > 0 && p <= 38) {
                      format!("NUMERIC({}, {})", p, s)
                 } else {
                      if p > 0 && s > 0 {
                          format!("BIGNUMERIC({}, {})", p, s)
                      } else {
                          "BIGNUMERIC".to_string()
                      }
                 }
             }
        },
        // Reuse string/json mapping for non-numeric types
        t => map_oracle_to_bq(t, raw_type), 
    }
}
