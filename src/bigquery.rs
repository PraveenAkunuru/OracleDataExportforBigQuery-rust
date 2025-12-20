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
use std::io::Write;

#[derive(Serialize)]
pub struct BigQueryField {
    pub name: String,
    pub r#type: String, // "type" is reserved in Rust
    pub mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

pub fn generate_schema(col_names: &[String], col_types: &[OracleType], output_path: &str, enable_row_hash: bool) -> std::io::Result<()> {
    let mut fields = Vec::new();
    
    for (name, r#type) in col_names.iter().zip(col_types.iter()) {
        let bq_type = map_oracle_to_bq(r#type);
        fields.push(BigQueryField {
            name: name.clone(),
            r#type: bq_type,
            mode: "NULLABLE".to_string(), // Default logic
            description: None,
        });
    }

    if enable_row_hash {
        fields.push(BigQueryField {
            name: "ROW_HASH".to_string(),
            r#type: "STRING".to_string(),
            mode: "REQUIRED".to_string(),
            description: Some("SHA256 hash for duplicate detection".to_string()),
        });
    }
    
    let json = serde_json::to_string_pretty(&fields)?;
    let mut file = File::create(output_path)?;
    file.write_all(json.as_bytes())?;
    
    Ok(())
}

pub fn map_oracle_to_bq(oracle_type: &OracleType) -> String {
    match oracle_type {
        OracleType::Number(prec, scale) => {
            // scale is i8. -127 means float/undefined.
            if *scale == 0 {
                // strict INT64 usually up to 19 digits.
                // If precision > 19, use NUMERIC to avoid overflow.
                if *prec > 19 { "NUMERIC".to_string() } else { "INTEGER".to_string() }
            } else {
                 // > 38 digits needs BIGNUMERIC
                 if *prec > 38 { "BIGNUMERIC".to_string() } else { "NUMERIC".to_string() }
            }
        },
        OracleType::Float(_) | OracleType::BinaryFloat | OracleType::BinaryDouble => "FLOAT".to_string(),
        
        OracleType::Char(_) | OracleType::NChar(_) | 
        OracleType::Varchar2(_) | OracleType::NVarchar2(_) | 
        OracleType::Long | OracleType::CLOB | OracleType::NCLOB | OracleType::Rowid => "STRING".to_string(),
        
        OracleType::Date | OracleType::Timestamp(_) => "DATETIME".to_string(),
        OracleType::TimestampTZ(_) | OracleType::TimestampLTZ(_) => "TIMESTAMP".to_string(),
        
        OracleType::Raw(_) | OracleType::BLOB | OracleType::BFILE => "BYTES".to_string(),
        
        _ => "STRING".to_string(), // Fallback
    }
}

// Replaces existing implementation
pub fn map_oracle_to_bq_ddl(oracle_type: &OracleType) -> String {
    match oracle_type {
        OracleType::Number(prec, scale) => {
             // Logic for INT64 vs NUMERIC vs BIGNUMERIC
             // 1. If scale == 0:
             //    prec <= 19 => INT64 (safe for 64-bit signed)
             //    prec > 19  => NUMERIC (up to 38 digits) or BIGNUMERIC (>38)
             //    Oracle MAX is 38. So usually NUMERIC is sufficient.
             //    But sometimes NUMBER without args implies max.
             
             if *scale == 0 {
                 if *prec > 19 { "NUMERIC".to_string() } else { "INT64".to_string() }
             } else {
                 // scale != 0
                 // BQ NUMERIC: max precision 38, max scale 9
                 // Oracle NUMBER(p,s): p <= 38, s can be anything.
                 // If s > 9, we MUST use BIGNUMERIC.
                 // If p > 38, we MUST use BIGNUMERIC.
                 let p = *prec;
                 let s = *scale;
                 
                 // Note: rust-oracle might return 0 for precision if unspecified?
                 // If p=0 && s=-127 (float), we map to FLOAT usually, but if it fell through here?
                 // (OracleType::Number handles it).
                 
                 if (s > 0 && s <= 9) && (p > 0 && p <= 38) {
                      format!("NUMERIC({}, {})", p, s)
                 } else {
                      // fallback to BIGNUMERIC for high precision/scale
                      // We can default to just BIGNUMERIC without params to be safe,
                      // or carry over params if valid?
                      // BQ BIGNUMERIC(P, S) supports P<=76.76.
                      if p > 0 && s > 0 {
                          format!("BIGNUMERIC({}, {})", p, s)
                      } else {
                          "BIGNUMERIC".to_string()
                      }
                 }
             }
        },
        // Reuse string/json mapping for non-numeric types as they are usually same (STRING, BYTES, DATETIME, TIMESTAMP)
        t => map_oracle_to_bq(t), 
    }
}
