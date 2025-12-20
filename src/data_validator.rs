//! # Validation Module
//!
//! Performs "Tiered Validation" on tables before/after export to ensure data integrity.
//!
//! ## Tiers
//! 1. **Row Count**: Basic check (COUNT(*)).
//! 2. **Primary Key Hash**: `SUM(ORA_HASH(PK))` to detect missing/duplicate rows.
//! 3. **Aggregates**: `SUM(NumericCol)` for up to 3 numeric columns to detect value corruption.
//!
//! Results are saved in `validation.json` and used to generate `validation.sql` for BigQuery.

use oracle::{Connection, Result};
use serde::Serialize;
use log::{info, warn};


#[derive(Serialize, Debug)]
/// Container for all validation metrics found for a table
pub struct ValidationStats {
    pub table_name: String,
    pub row_count: u64,
    pub pk_hash: Option<String>,
    pub aggregates: Option<Vec<ColumnAggregate>>,
}

#[derive(Serialize, Debug)]
/// Represents a single column aggregation result
pub struct ColumnAggregate {
    pub column_name: String,
    pub agg_type: String, // SUM, MIN, MAX
    pub value: String,
}

pub fn validate_table(conn: &Connection, schema: &str, table: &str, pk_cols: Option<&[String]>, agg_cols: Option<&[String]>) -> Result<ValidationStats> {
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
    if let Some(pks) = pk_cols {
        if !pks.is_empty() {
             // Construct Hash Expression
             // Single: "COL"
             // Composite: "COL1" || '_' || "COL2" ...
             let hash_expr = if pks.len() == 1 {
                 format!("\"{}\"", pks[0])
             } else {
                 pks.iter().map(|c| format!("\"{}\"", c)).collect::<Vec<_>>().join(" || '_' || ")
             };
             
             let hash_sql = format!("SELECT SUM(ORA_HASH({})) FROM \"{}\".\"{}\"", hash_expr, schema, table);
             
             match conn.query(&hash_sql, &[]) {
                 Ok(mut rows) => {
                    if let Some(Ok(r)) = rows.next() {
                        let val: Option<String> = r.get(0)?; 
                        pk_hash = val;
                    }
                 },
                 Err(e) => warn!("Failed to compute PK hash: {}", e),
             }
        }
    }

    // Tier 3: Aggregates
    let mut aggregates = None;
    if let Some(cols) = agg_cols {
        let mut agg_results = Vec::new();
        for col in cols {
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


