//! # Exporter Module
//!
//! Handles the low-level "Extract and Save" operation for a single table or chunk.
//! 
//! ## Features
//! - **Streaming Export**: Uses `BufWriter` and `GzEncoder` to stream data directly to disk without loading entire tables into memory.
//! - **Type Parity**: Carefully maps Oracle types to BigQuery-compatible CSV formats (e.g. UTC conversion for Timestamps).
//! - **ROW_HASH**: Optionally computes a SHA256 hash of every row (using Oracle `STANDARD_HASH`) for data validation.

use oracle::{Connection, Result};
use oracle::sql_type::OracleType;
use oracle::sql_type::Timestamp;
use std::fs::File;
use std::io::BufWriter;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::time::Instant;
use log::{info, warn};
use base64::{Engine as _, engine::general_purpose};
use csv::{WriterBuilder, QuoteStyle};

use sha2::{Sha256, Digest};

/// Formats Oracle Timestamp to BigQuery-compliant string
/// Format: `YYYY-MM-DD HH:MI:SS.FF6`
pub fn format_timestamp(ts: &Timestamp) -> String {
    let mut year = ts.year();
    let month = ts.month();
    let day = ts.day();
    let hour = ts.hour();
    let minute = ts.minute();
    let second = ts.second();
    let nanosec = ts.nanosecond();
    
    // Handle potential overflow/underflow if necessary, typical Oracle dates fit in 4 digits
    if year < 0 { year = 0; } // BQ doesn't like BC often, simplistic handling
    
    format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}",
            year, month, day, hour, minute, second, nanosec / 1000)
}

#[cfg(test)]
mod tests {
    // use super::*; // Unused
}

use crate::oracle_metadata;

/// Builds the SELECT query dynamically based on table metadata
pub fn build_select_query(conn: &Connection, schema: &str, table: &str, where_clause: Option<&String>, enable_row_hash: bool, use_client_hash: bool) -> Result<(String, Vec<String>)> {
    let mut stmt = conn.statement(oracle_metadata::SQL_GET_COLUMNS).build()?;
    let table_param: &dyn oracle::sql_type::ToSql = &table;
    let schema_param: &dyn oracle::sql_type::ToSql = &schema;
    let rows = stmt.query(&[table_param, schema_param])?;

    let mut select_parts = Vec::new();
    let mut raw_col_names = Vec::new();
    let mut hash_parts = Vec::new(); // Collects StandardHash(Coalesce(ToChar(Col)))
    
    for row_result in rows {
        let row = row_result?;
        let col_name: String = row.get(0)?;
        let data_type: String = row.get(1)?;
        
        raw_col_names.push(col_name.clone());
        
        // Handle Time Zones or specialized transforms here
        let upper_type = data_type.to_uppercase();
        let raw_expr = if upper_type.contains("TIME ZONE") {
             // Convert to UTC char for easy CSV export
             format!("TO_CHAR(SYS_EXTRACT_UTC(\"{}\"), 'YYYY-MM-DD\"T\"HH24:MI:SS.FF6\"Z\"')", col_name)
        } else if upper_type.contains("INTERVAL YEAR") {
             // Robust Interval Year to Month transformation for BigQuery
             format!("CASE WHEN \"{}\" IS NULL THEN NULL ELSE \
                      CASE WHEN EXTRACT(YEAR FROM \"{}\") < 0 OR EXTRACT(MONTH FROM \"{}\") < 0 THEN '-' ELSE '' END || \
                      ABS(EXTRACT(YEAR FROM \"{}\")) || '-' || ABS(EXTRACT(MONTH FROM \"{}\")) || ' 0 0:0:0' END", 
                      col_name, col_name, col_name, col_name, col_name)
        } else if upper_type.contains("INTERVAL DAY") {
             // Robust Interval Day to Second transformation for BigQuery
             format!("CASE WHEN \"{}\" IS NULL THEN NULL ELSE \
                      '0-0 ' || CASE WHEN EXTRACT(DAY FROM \"{}\") < 0 OR EXTRACT(HOUR FROM \"{}\") < 0 OR \
                      EXTRACT(MINUTE FROM \"{}\") < 0 OR EXTRACT(SECOND FROM \"{}\") < 0 THEN '-' ELSE '' END || \
                      ABS(EXTRACT(DAY FROM \"{}\")) || ' ' || ABS(EXTRACT(HOUR FROM \"{}\")) || ':' || \
                      ABS(EXTRACT(MINUTE FROM \"{}\")) || ':' || ABS(EXTRACT(SECOND FROM \"{}\")) END",
                      col_name, col_name, col_name, col_name, col_name, col_name, col_name, col_name, col_name)
        } else if upper_type == "XMLTYPE" {
             format!("REPLACE(REPLACE(sys.XMLType.getStringVal(\"{}\"), CHR(10), ''), CHR(13), '')", col_name)
        } else if upper_type == "JSON" {
             // Use JSON_SERIALIZE if Oracle 21c/23c, else it might have been CLOB with constraint
             format!("REPLACE(REPLACE(JSON_SERIALIZE(\"{}\"), CHR(10), ''), CHR(13), '')", col_name)
        } else if upper_type == "BOOLEAN" {
             format!("CASE WHEN \"{}\" THEN 'true' ELSE 'false' END", col_name)
        } else if upper_type.contains("SDO_GEOMETRY") {
             // Convert Spatial to WKT (Well Known Text) for universal compatibility
             format!("SDO_UTIL.TO_WKTGEOMETRY(\"{}\")", col_name)
        } else if upper_type.contains("UROWID") {
             format!("TO_CHAR(\"{}\")", col_name)
        } else {
             format!("\"{}\"", col_name)
        };
        // Always alias to ensure CSV header matches column name (especially for expressions)
        select_parts.push(format!("{} AS \"{}\"", raw_expr, col_name));

        if enable_row_hash && !use_client_hash {
            if let Some(h) = crate::sql_generator_utils::get_hash_expr_from_str(&col_name, &data_type) {
                hash_parts.push(h);
            }
        }
    }

    if enable_row_hash && !use_client_hash && !hash_parts.is_empty() {
        let final_expr = crate::sql_generator_utils::build_hash_from_parts(&hash_parts);
        select_parts.push(format!("{} AS ROW_HASH", final_expr));
    }
    
    if select_parts.is_empty() {
        warn!("No columns found via metadata query for {}.{}. Using SELECT *", schema, table);
        return Ok((format!("SELECT * FROM \"{}\".\"{}\"", schema, table), vec![]));
    }
    
    let mut sql = format!("SELECT {} FROM \"{}\".\"{}\"", select_parts.join(", "), schema, table);
    
    if let Some(clause) = where_clause {
        sql.push_str(" WHERE ");
        sql.push_str(clause);
    }
    
    Ok((sql, raw_col_names))
}

/// Parameters for a single table export job
pub struct ExportParams {
    /// Full Oracle Connection String (e.g. //host:port/service)
    pub connection_string: String,
    /// Database Username
    pub username: String,
    /// Database Password
    pub password: String,
    /// Output Gzip filename (e.g. "data.csv.gz")
    pub output_file: String,
    /// Rows to prefetch per round-trip (Performance tuning)
    pub prefetch_rows: u32,
    /// Target Schema (Owner)
    pub schema: String,
    /// Table Name
    pub table: String,
    /// Optional WHERE clause (without "WHERE") for filtering/chunking
    pub query_where: Option<String>,
    /// If true, computes SHA256(ROW) as extra column
    pub enable_row_hash: bool,
    /// If true, computes ROW_HASH in Rust client instead of Oracle
    pub use_client_hash: bool,
    /// CSV Field Delimiter (e.g. "\x10")
    pub field_delimiter: String,
}

#[derive(Debug, Default)]
pub struct ExportStats {
    pub rows: u64,
    pub bytes: u64,
    pub duration_secs: f64,
}

pub fn export_table(params: ExportParams) -> Result<ExportStats> {
    info!("Connecting to {} as {}", params.connection_string, params.username);

    let conn = Connection::connect(&params.username, &params.password, &params.connection_string)?;
    
    // Build Query
    let (sql, _) = build_select_query(&conn, &params.schema, &params.table, params.query_where.as_ref(), params.enable_row_hash, params.use_client_hash)?;

    // Prepare Statement
    let mut stmt = conn.statement(&sql)
        .prefetch_rows(params.prefetch_rows)
        .build()?;
    
    info!("Executing query: {}", sql);
    let start = Instant::now();
    let rows = stmt.query(&[])?; 

    // Setup Output
    info!("Writing output to: {}", params.output_file);
    let file = File::create(&params.output_file).expect("Unable to create output file");
    // Increase buffer to 128KB for better throughput
    let buf_writer = BufWriter::with_capacity(128 * 1024, file); 
    let encoder = GzEncoder::new(buf_writer, Compression::fast()); 
    
    let delimiter = params.field_delimiter.bytes().next().unwrap_or(b'\x10'); // Default Ctrl+P
    
    let mut wtr = WriterBuilder::new()
        .delimiter(delimiter)
        .quote_style(QuoteStyle::NonNumeric)
        .from_writer(encoder);

    let col_infos = rows.column_info();
    let mut col_names: Vec<String> = col_infos.iter().map(|c| c.name().to_string()).collect();
    let col_types: Vec<OracleType> = col_infos.iter().map(|c| c.oracle_type().clone()).collect();
    
    if params.enable_row_hash && params.use_client_hash {
        col_names.push("ROW_HASH".to_string());
    }

    wtr.write_record(&col_names).expect("Failed to write header");

    let mut row_count = 0;
    let mut uncompressed_bytes: u64 = 0;
    let mut last_log = Instant::now();

    info!("Step 3 [{}]: Starting record fetch from Oracle...", params.table);
    for row_result in rows {
        let row = row_result?; 
        let mut record = Vec::with_capacity(col_names.len());
        
        for (i, sql_type) in col_types.iter().enumerate() {
            let val_str: String = match sql_type {
                OracleType::Number(_, _) | OracleType::Float(_) | OracleType::BinaryFloat | OracleType::BinaryDouble => {
                    let val: Option<String> = row.get(i)?;
                    val.unwrap_or_default()
                },
                OracleType::Date | OracleType::Timestamp(_) | OracleType::TimestampTZ(_) | OracleType::TimestampLTZ(_) => {
                    let val: Option<Timestamp> = row.get(i)?;
                    match val {
                        Some(ts) => format_timestamp(&ts),
                        None => String::new(),
                    }
                },
                OracleType::Raw(_) | OracleType::BLOB => {
                    let val: Option<Vec<u8>> = row.get(i)?;
                    match val {
                        Some(bytes) => general_purpose::STANDARD.encode(&bytes), 
                        None => String::new(),
                    }
                },
                OracleType::Xml => {
                    let val: Option<String> = row.get(i).unwrap_or(None);
                    match val {
                        Some(s) => s.replace(['\n', '\r'], " "),
                        None => String::new(),
                    }
                },
                 OracleType::Char(_) | OracleType::NChar(_) => {
                    let val: Option<String> = row.get(i).unwrap_or(None);
                    val.unwrap_or_default().trim_end().to_string()
                },
                // Default handling for Strings and everything else
                _ => {
                    let val: Option<String> = row.get(i).unwrap_or(None);
                    val.unwrap_or_default()
                }
            };
            
            uncompressed_bytes += val_str.len() as u64;
            if i < col_names.len() - 1 {
                uncompressed_bytes += 1;
            }
            record.push(val_str);
        }
        
        // Client-Side Hashing
        if params.enable_row_hash && params.use_client_hash {
            let mut hasher = Sha256::new();
            for val in record.iter() {
                // Determine if we should hash this column
                // Logic mimics build_hash_from_parts: STANDARD_HASH(val)
                // We hash the value. If it's NOT the first column, we first hash the previous accumulator (which is implicit in Sha256 state? No, Oracle does ||)
                // Oracle: STANDARD_HASH( ... || STANDARD_HASH(col, 'SHA256'), 'SHA256')
                // Actually, our SQL generator does: 
                // STANDARD_HASH(STANDARD_HASH(c1) || STANDARD_HASH(c2) ..., 'SHA256')
                
                // So we must:
                // 1. Hash the column value -> h_val
                // 2. Append h_val to a master buffer
                // 3. Hash the master buffer at the end
                
                let val_bytes = val.as_bytes();
                let col_hash = if val.is_empty() {
                    // Oracle STANDARD_HASH('') depends. 
                    // SQL: STANDARD_HASH(COALESCE(TO_CHAR(col), ''), 'SHA256')
                    // SHA256('') = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
                    let mut h = Sha256::new();
                    h.update(b"");
                    hex::encode_upper(h.finalize())
                } else {
                    let mut h = Sha256::new();
                    h.update(val_bytes);
                    hex::encode_upper(h.finalize())
                };

                // Update master hasher with this column's hash
                hasher.update(col_hash.as_bytes());
            }
            let final_hash = hex::encode_upper(hasher.finalize());
            record.push(final_hash);
        }

        uncompressed_bytes += 1; 
        wtr.write_record(&record).expect("Failed to write record");
        row_count += 1;
        
        if last_log.elapsed().as_secs() >= 5 || (row_count > 0 && row_count % 100_000 == 0) {
             let mb = uncompressed_bytes as f64 / (1024.0 * 1024.0);
             let elapsed = start.elapsed().as_secs_f64();
             let rate = if elapsed > 0.0 { mb / elapsed } else { 0.0 };
             info!("Progress [{}]: Exported {} rows... (~{:.2} MB uncompressed, {:.2} MB/s)", params.table, row_count, mb, rate);
             last_log = Instant::now();
        }
    }

    wtr.flush().expect("Failed to flush CSV writer");
    
    let duration = start.elapsed();
    let duration_secs = duration.as_secs_f64();
    let mb = uncompressed_bytes as f64 / (1024.0 * 1024.0);
    info!("Completed: {} rows, {:.2} MB in {:.2?}", row_count, mb, duration);

    Ok(ExportStats {
        rows: row_count,
        bytes: uncompressed_bytes,
        duration_secs,
    })
}
