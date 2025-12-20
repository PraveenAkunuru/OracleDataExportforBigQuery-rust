use oracle::{Connection, Result};
use serde::Serialize;
use log::{info, warn};
use std::fs::File;
use std::io::Write;

#[derive(Serialize, Debug)]
pub struct ValidationStats {
    pub table_name: String,
    pub row_count: u64,
    pub pk_hash: Option<String>,
    pub aggregates: Option<Vec<ColumnAggregate>>,
}

#[derive(Serialize, Debug)]
pub struct ColumnAggregate {
    pub column_name: String,
    pub agg_type: String, // SUM, MIN, MAX
    pub value: String,
}

pub fn validate_table(conn: &Connection, schema: &str, table: &str, pk_col: Option<&str>, agg_cols: Option<&[String]>) -> Result<ValidationStats> {
    info!("Validating {}.{}", schema, table);
    
    // Tier 1: Row Count
    let count_sql = format!("SELECT COUNT(*) FROM \"{}\".\"{}\"", schema, table);
    let mut rows = conn.query(&count_sql, &[])?;
    let row_count: u64 = if let Some(Ok(r)) = rows.next() {
        r.get(0)?
    } else {
        0
    };

    // Tier 2: PK Hash (if PK provided)
    let mut pk_hash = None;
    if let Some(pk) = pk_col {
        // Use STANDARD_HASH on the PK column.
        // SUM(ORA_HASH(...)) is also good but STANDARD_HASH is more collision resistant for DVT.
        // DVT often uses: SELECT SUM(ORA_HASH(column_name)) ...
        // Let's use SUM(ORA_HASH) as it returns a Number, easier to compare than raw bytes sometimes, 
        // but STANDARD_HASH is better for "Fingerprinting".
        // The user asked for "efficient". SUM(ORA_HASH) is standard Oracle efficient way.
        // Let's stick to SUM(ORA_HASH(concat cols)) or just simple col.
        
        let hash_sql = format!("SELECT SUM(ORA_HASH(\"{}\")) FROM \"{}\".\"{}\"", pk, schema, table);
        // Note: ORA_HASH returns NUMBER.
        match conn.query(&hash_sql, &[]) {
            Ok(mut rows) => {
               if let Some(Ok(r)) = rows.next() {
                   let val: Option<String> = r.get(0)?; // Use String to avoid u128 issues
                   // Oracle NUMBER can be large. String is safest.
                   let val_str = val; 
                   pk_hash = val_str;
               }
            },
            Err(e) => warn!("Failed to compute PK hash: {}", e),
        }
    }

    // Tier 3: Aggregates
    let mut aggregates = None;
    if let Some(cols) = agg_cols {
        let mut agg_results = Vec::new();
        for col in cols {
            // Try SUM for now, need type info to separate MIN/MAX for dates? 
            // For simplicity, just SUM(col) and ignore if it fails (e.g. non-numeric).
            // Actually, we should probably check type or just try.
            let agg_sql = format!("SELECT SUM(\"{}\") FROM \"{}\".\"{}\"", col, schema, table);
            if let Ok(mut rows) = conn.query(&agg_sql, &[]) {
                if let Some(Ok(r)) = rows.next() {
                     let val: Option<String> = r.get(0).ok();
                     if let Some(v) = val {
                         agg_results.push(ColumnAggregate {
                             column_name: col.clone(),
                             agg_type: "SUM".to_string(),
                             value: v,
                         });
                     }
                }
            }
        }
        if !agg_results.is_empty() {
            aggregates = Some(agg_results);
        }
    }

    Ok(ValidationStats {
        table_name: table.to_string(),
        row_count,
        pk_hash,
        aggregates,
    })
}

pub fn write_validation_report(stats: &ValidationStats, path: &str) -> std::io::Result<()> {
    let json = serde_json::to_string_pretty(stats)?;
    let mut file = File::create(path)?;
    file.write_all(json.as_bytes())?;
    Ok(())
}
