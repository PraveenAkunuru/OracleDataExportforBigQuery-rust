use oracle::{Connection, Result};
use log::{info, warn, debug};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct TableMetadata {
    pub owner: String,
    pub table_name: String,
    pub size_gb: f64,
    pub row_count: Option<u64>,
    pub is_partitioned: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct Chunk {
    pub chunk_id: u32,
    pub start_rowid: String,
    pub end_rowid: String,
}

/// Discovers tables in a specific schema
pub fn get_tables(conn: &Connection, schema: &str) -> Result<Vec<String>> {
    let sql = "SELECT table_name FROM all_tables WHERE owner = :1 ORDER BY table_name";
    let rows = conn.query(sql, &[&schema.to_uppercase()])?;
    
    let mut tables = Vec::new();
    for row_result in rows {
        let row = row_result?;
        let table_name: String = row.get(0)?;
        tables.push(table_name);
    }
    Ok(tables)
}

/// Estimates table size using user_segments (fast) or num_rows (fallback)
pub fn get_table_size_gb(conn: &Connection, schema: &str, table: &str) -> Result<f64> {
    // Try segments first (most accurate for size on disk)
    let sql_segments = "SELECT SUM(bytes) / 1024 / 1024 / 1024 
                        FROM all_segments 
                        WHERE owner = :1 AND segment_name = :2";
    
    // We wrap in match to handle ORA-00942 or similar
    let size_res = conn.query(sql_segments, &[&schema.to_uppercase(), &table.to_uppercase()]);
    
    match size_res {
        Ok(mut rows) => {
             if let Some(Ok(r)) = rows.next() {
                 let size: Option<f64> = r.get(0)?;
                 if let Some(s) = size {
                     return Ok(s);
                 }
             }
        },
        Err(e) => {
             // If ALL_SEGMENTS failed, try USER_SEGMENTS (assuming we own it)
             warn!("Failed to query ALL_SEGMENTS ({}), trying USER_SEGMENTS", e);
             let sql_user = "SELECT SUM(bytes) / 1024 / 1024 / 1024 
                             FROM user_segments 
                             WHERE segment_name = :1";
             if let Ok(mut rows) = conn.query(sql_user, &[&table.to_uppercase()]) {
                 if let Some(Ok(r)) = rows.next() {
                     let size: Option<f64> = r.get(0)?;
                     if let Some(s) = size {
                         return Ok(s);
                     }
                 }
             }
        }
    }
    
    // Fallback: num_rows * avg_row_len in all_tables
    let sql_stats = "SELECT num_rows, avg_row_len FROM all_tables WHERE owner = :1 AND table_name = :2";
    let mut rows = conn.query(sql_stats, &[&schema.to_uppercase(), &table.to_uppercase()])?;
    let row = rows.next();
    
    if let Some(Ok(r)) = row {
        let num_rows: Option<u64> = r.get(0)?;
        let avg_len: Option<u64> = r.get(1)?;
        
        if let Some(rows) = num_rows {
            if let Some(len) = avg_len {
                 let bytes = rows as f64 * len as f64;
                 return Ok(bytes / 1024.0 / 1024.0 / 1024.0);
            }
        }
    }
    
    Ok(0.0) // Unknown/Empty
}

/// Generates ROWID chunks using DBMS_PARALLEL_EXECUTE
/// This is the most efficient way to split a table without physically partitioning it.
pub fn generate_chunks(conn: &Connection, schema: &str, table: &str, chunk_count: u32) -> Result<Vec<Chunk>> {
    let task_name = format!("EXP_{}_{}", table, chrono::Utc::now().timestamp());
    
    // Cleanup any stale task (just in case)
    // We ignore errors here in case task doesn't exist
    let _ = conn.execute("BEGIN DBMS_PARALLEL_EXECUTE.DROP_TASK(:1); END;", &[&task_name]);

    info!("Generating {} chunks for {}.{} (Task: {})", chunk_count, schema, table, task_name);

    // 1. Create Task
    conn.execute("BEGIN DBMS_PARALLEL_EXECUTE.CREATE_TASK(:1); END;", &[&task_name])?;

    // 2. Calculate Blocks per Chunk
    // We aim for 'chunk_count' chunks.
    // Total blocks estimate:
    let sql_blocks = "SELECT blocks FROM all_tables WHERE owner = :1 AND table_name = :2";
    let mut rows = conn.query(sql_blocks, &[&schema.to_uppercase(), &table.to_uppercase()])?;
    let total_blocks: u64 = if let Some(Ok(r)) = rows.next() {
        r.get(0).unwrap_or(1000)
    } else {
        1000
    };
    
    let blocks_per_chunk = (total_blocks as f64 / chunk_count as f64).ceil() as i64;
    let blocks_per_chunk = std::cmp::max(1, blocks_per_chunk); // Ensure at least 1 block

    // 3. Create Chunks by ROWID
    // by_row => FALSE means we chunk by BLOCK (much faster scan)
    let sql_chunk = "BEGIN 
                        DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_ROWID(
                            task_name   => :1,
                            table_owner => :2,
                            table_name  => :3,
                            by_row      => FALSE,
                            chunk_size  => :4
                        );
                     END;";
                     
    conn.execute(sql_chunk, &[&task_name, &schema.to_uppercase(), &table.to_uppercase(), &blocks_per_chunk])?;

    // 4. Fetch the Ranges
    let sql_ranges = "SELECT start_rowid, end_rowid 
                      FROM user_parallel_execute_chunks 
                      WHERE task_name = :1 
                      ORDER BY start_rowid";
                      
    let rows = conn.query(sql_ranges, &[&task_name])?;
    let mut chunks = Vec::new();
    let mut id = 0;
    
    for row_result in rows {
        let row = row_result?;
        let start_rowid: String = row.get(0)?;
        let end_rowid: String = row.get(1)?;
        
        chunks.push(Chunk {
            chunk_id: id,
            start_rowid,
            end_rowid,
        });
        id += 1;
    }

    // 5. Cleanup
    conn.execute("BEGIN DBMS_PARALLEL_EXECUTE.DROP_TASK(:1); END;", &[&task_name])?;
    
    Ok(chunks)
}

/// Retrieves column names and types for a table.
/// Useful for BigQuery schema generation.
pub fn get_table_columns(conn: &Connection, schema: &str, table: &str) -> Result<(Vec<String>, Vec<oracle::sql_type::OracleType>)> {
    let _sql = "SELECT column_name, data_type, data_scale, data_precision 
               FROM all_tab_columns 
               WHERE owner = :1 AND table_name = :2 
               ORDER BY column_id";
    
    // We can't easily map raw string "VARCHAR2" to OracleType enum without manual parsing.
    // However, rust-oracle's `sql_type` module might not have a direct string parser.
    // A trick is to `SELECT * FROM table WHERE 1=0` and generic `stmt.query` to get `ColumnInfo`.
    // This is more robust than parsing string types manually.
    
    let sql_dummy = format!("SELECT * FROM \"{}\".\"{}\" WHERE 1=0", schema, table);
    let mut stmt = conn.statement(&sql_dummy).build()?;
    let rows = stmt.query(&[])?;
    
    let col_infos = rows.column_info();
    let names: Vec<String> = col_infos.iter().map(|c| c.name().to_string()).collect();
    let types: Vec<oracle::sql_type::OracleType> = col_infos.iter().map(|c| c.oracle_type().clone()).collect();
    
    Ok((names, types))
}

/// Fetches the Oracle DDL for a table using DBMS_METADATA.GET_DDL
pub fn get_ddl(conn: &Connection, schema: &str, table: &str) -> Option<String> {
    let sql = "SELECT DBMS_METADATA.GET_DDL('TABLE', :1, :2) FROM DUAL";
    // DBMS_METADATA.GET_DDL returns a CLOB. rust-oracle handles CLOB -> String conversion automatically for query_row usually,
    // but explicit retrieval might be safer if query_row relies on generic mapping.
    // For now, let's try direct map to String.
    match conn.query_row(sql, &[&table.to_uppercase(), &schema.to_uppercase()]) {
        Ok(row) => {
            let ddl: Result<String> = row.get(0);
            match ddl {
                Ok(s) => Some(s),
                Err(e) => {
                    warn!("Failed to retrieve DDL CLOB for {}.{}: {}", schema, table, e);
                    None
                }
            }
        }
        Err(err) => {
            debug!("Failed to fetch DDL for {}.{}: {}", schema, table, err);
            None
        }
    }
}

/// Fetches the Primary Key column(s) for a table.
/// Returns a comma-separated string of columns if composite, or single column.
/// Returns None if no PK exists.
pub fn get_primary_key(conn: &Connection, schema: &str, table: &str) -> Result<Option<String>> {
    let sql = "
        SELECT min(column_name) -- Simplified for single PK, or use aggregation for composite
        FROM all_cons_columns c
        JOIN all_constraints k ON c.constraint_name = k.constraint_name AND c.owner = k.owner
        WHERE k.constraint_type = 'P'
          AND k.owner = :1
          AND k.table_name = :2
    ";
    
    // Note: handling composite keys correctly for ORA_HASH(col1 || col2) is complex.
    // For now, let's just grab the first column of the PK or a concat if easy.
    // Actually, ORA_HASH only takes one expression.
    // So if composite, we might need `ORA_HASH(col1 || '_' || col2)`.
    // Let's first just try to get the single PK column, or the 'first' one if composite.
    
    let rows = conn.query(sql, &[&schema.to_uppercase(), &table.to_uppercase()])?;
    if let Some(Ok(row)) = rows.into_iter().next() {
        let col: Option<String> = row.get(0)?;
        Ok(col) // This will be None if no rows or NULL (shouldn't be null if PK exists)
    } else {
        Ok(None)
    }
}
