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

pub fn map_oracle_to_bq_ddl(oracle_type: &OracleType) -> String {
    match oracle_type {
        OracleType::Number(prec, scale) => {
             if *scale == 0 {
                 if *prec > 19 { "NUMERIC".to_string() } else { "INT64".to_string() }
             } else {
                 // Use precision/scale if available and valid
                 // BQ NUMERIC supports max precision 38, scale 9.
                 // BQ BIGNUMERIC supports max precision 76.76.
                 // Oracle sometimes reports prec=0, scale=-127 for float.
                 let p = *prec;
                 let s = *scale;
                 if s > 0 && p > 0 {
                      if p <= 38 && s <= 9 {
                          format!("NUMERIC({}, {})", p, s)
                      } else {
                          // Allow BIGNUMERIC with params? BIGNUMERIC(P, S) is valid.
                          format!("BIGNUMERIC({}, {})", p, s)
                      }
                 } else {
                      if p > 38 { "BIGNUMERIC".to_string() } else { "NUMERIC".to_string() }
                 }
             }
        },
        // Reuse JSON mapping for others, or specialize if needed
        t => map_oracle_to_bq(t), 
    }
}
