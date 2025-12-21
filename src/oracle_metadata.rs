//! # Metadata Module
//!
//! Handles all queries related to database structure and statistics.
//!
//! ## Key Functions
//! - `get_tables`: Lists tables in schema.
//! - `get_table_size_gb`: Estimates size using `ALL_SEGMENTS` (fast) or `num_rows` (fallback).
//! - `generate_chunks`: Uses `DBMS_PARALLEL_EXECUTE` to split tables by ROWID ranges.
use oracle::{Connection, Result};
use log::{info, warn, debug};
use serde::Serialize;

// SQL Constants
const SQL_LIST_TABLES: &str = "SELECT table_name FROM all_tables WHERE owner = :1 ORDER BY table_name";
const SQL_SEGMENTS_SIZE: &str = "SELECT SUM(bytes) / 1024 / 1024 / 1024 FROM all_segments WHERE owner = :1 AND segment_name = :2";
const SQL_USER_SEGMENTS_SIZE: &str = "SELECT SUM(bytes) / 1024 / 1024 / 1024 FROM user_segments WHERE segment_name = :1";
const SQL_TABLE_STATS: &str = "SELECT num_rows, avg_row_len FROM all_tables WHERE owner = :1 AND table_name = :2";
const SQL_TABLE_BLOCKS: &str = "SELECT blocks FROM all_tables WHERE owner = :1 AND table_name = :2";
const SQL_FETCH_CHUNKS: &str = "SELECT start_rowid, end_rowid FROM user_parallel_execute_chunks WHERE task_name = :1 ORDER BY start_rowid";
const SQL_GET_DDL: &str = "SELECT DBMS_METADATA.GET_DDL('TABLE', :1, :2) FROM DUAL";
const SQL_GET_PK: &str = "
    SELECT column_name
    FROM all_cons_columns c
    JOIN all_constraints k ON c.constraint_name = k.constraint_name AND c.owner = k.owner
    WHERE k.constraint_type = 'P'
      AND k.owner = :1
      AND k.table_name = :2
    ORDER BY c.position
";
const SQL_DROP_PARALLEL_TASK: &str = "BEGIN DBMS_PARALLEL_EXECUTE.DROP_TASK(:1); END;";
pub const SQL_GET_COLUMNS: &str = "
    SELECT c.column_name, c.data_type, cm.comments
    FROM all_tab_columns c
    LEFT JOIN all_col_comments cm 
      ON c.owner = cm.owner 
      AND c.table_name = cm.table_name 
      AND c.column_name = cm.column_name
    WHERE c.table_name = :1 
      AND c.owner = :2 
    ORDER BY c.column_id
";
const SQL_GET_PARTITION_KEYS: &str = "
    SELECT column_name
    FROM all_part_key_columns
    WHERE owner = :1 
      AND name = :2 
      AND object_type = 'TABLE'
    ORDER BY column_position
";
const SQL_GET_INDEXES: &str = "
    SELECT column_name
    FROM all_ind_columns
    WHERE table_owner = :1
      AND table_name = :2
    ORDER BY index_name, column_position
";

#[derive(Debug, Clone, Serialize)]
/// Basic table metadata summary
pub struct TableMetadata {
    pub owner: String,
    pub table_name: String,
    pub size_gb: f64,
    pub row_count: Option<u64>,
    pub is_partitioned: bool,
}

#[derive(Debug, Clone, Serialize)]
/// Represents a ROWID range for parallel processing
pub struct Chunk {
    pub chunk_id: u32,
    /// Oracle ROWID string (Base64-like)
    pub start_rowid: String,
    pub end_rowid: String,
}

/// Discovers tables in a specific schema
pub fn get_tables(conn: &Connection, schema: &str) -> Result<Vec<String>> {
    let rows = conn.query(SQL_LIST_TABLES, &[&schema.to_uppercase()])?;
    
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
    let size_res = conn.query(SQL_SEGMENTS_SIZE, &[&schema.to_uppercase(), &table.to_uppercase()]);
    
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
             if let Ok(mut rows) = conn.query(SQL_USER_SEGMENTS_SIZE, &[&table.to_uppercase()]) {
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
    let mut rows = conn.query(SQL_TABLE_STATS, &[&schema.to_uppercase(), &table.to_uppercase()])?;
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
    let _ = conn.execute("BEGIN DBMS_PARALLEL_EXECUTE.DROP_TASK(:1); END;", &[&task_name]);

    info!("Generating {} chunks for {}.{} (Task: {})", chunk_count, schema, table, task_name);

    // 1. Create Task
    conn.execute("BEGIN DBMS_PARALLEL_EXECUTE.CREATE_TASK(:1); END;", &[&task_name])?;

    // 2. Calculate Blocks per Chunk
    let mut rows = conn.query(SQL_TABLE_BLOCKS, &[&schema.to_uppercase(), &table.to_uppercase()])?;
    let total_blocks: u64 = if let Some(Ok(r)) = rows.next() {
        r.get(0).unwrap_or(1000)
    } else {
        1000
    };
    
    let blocks_per_chunk = (total_blocks as f64 / chunk_count as f64).ceil() as i64;
    let blocks_per_chunk = std::cmp::max(1, blocks_per_chunk); // Ensure at least 1 block

    // 3. Create Chunks by ROWID
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
    let rows = conn.query(SQL_FETCH_CHUNKS, &[&task_name])?;
    let mut chunks = Vec::new();
    for (id, row_result) in rows.enumerate() {
        let row = row_result?;
        let start_rowid: String = row.get(0)?;
        let end_rowid: String = row.get(1)?;
        
        chunks.push(Chunk {
            chunk_id: id as u32,
            start_rowid,
            end_rowid,
        });
    }

    // 5. Cleanup
    conn.execute(SQL_DROP_PARALLEL_TASK, &[&task_name])?;
    
    Ok(chunks)
}

/// Struct to hold column metadata
pub struct TableColumns {
    pub names: Vec<String>,
    pub types: Vec<oracle::sql_type::OracleType>,
    pub raw_types: Vec<String>,
    pub comments: Vec<Option<String>>,
}

/// Retrieves column names, types, raw types, and comments for a table.
/// Useful for BigQuery schema generation.
pub fn get_table_columns(conn: &Connection, schema: &str, table: &str) -> Result<TableColumns> {
    // 1. Get exact OracleType via dummy query (best for Numeric precision/scale)
    let sql_dummy = format!("SELECT * FROM \"{}\".\"{}\" WHERE 1=0", schema, table);
    let mut stmt = conn.statement(&sql_dummy).build()?;
    let rows = stmt.query(&[])?;
    let col_infos = rows.column_info();
    
    // 2. Get Data Type String via ALL_TAB_COLUMNS (best for advanced types like JSON/BOOLEAN/XMLTYPE)
    let mut stmt_meta = conn.statement(SQL_GET_COLUMNS).build()?;
    let rows_meta = stmt_meta.query(&[&table.to_uppercase(), &schema.to_uppercase()])?;
    
    let mut type_strings = std::collections::HashMap::new();
    let mut comments_map = std::collections::HashMap::new();

    for row_res in rows_meta {
        let row = row_res?;
        let name: String = row.get(0)?;
        let data_type: String = row.get(1)?;
        let comment: Option<String> = row.get(2)?; // Index 2 is comments
        
        let name_upper = name.to_uppercase();
        type_strings.insert(name_upper.clone(), data_type);
        if let Some(c) = comment {
            comments_map.insert(name_upper, c);
        }
    }

    let mut names = Vec::new();
    let mut types = Vec::new();
    let mut strings = Vec::new();
    let mut comments = Vec::new();

    for c in col_infos {
        let name = c.name().to_string();
        let name_upper = name.to_uppercase();
        let type_str = type_strings.get(&name_upper).cloned().unwrap_or_else(|| "UNKNOWN".to_string());
        let comment = comments_map.get(&name_upper).cloned();
        
        names.push(name);
        types.push(c.oracle_type().clone());
        strings.push(type_str);
        comments.push(comment);
    }
    
    Ok(TableColumns {
        names,
        types,
        raw_types: strings,
        comments,
    })
}

/// Retrieves a map of Virtual Columns and their generation expressions (DATA_DEFAULT).
/// Uses XML trick to read DATA_DEFAULT (LONG type) safely.
pub fn get_virtual_columns_map(conn: &Connection, schema: &str, table: &str) -> std::collections::HashMap<String, String> {
    // Note: ALL_TAB_COLUMNS has DATA_DEFAULT as LONG.
    // XMLGen is the standard workaround to read LONGs in SQL.
    let sql = "
        SELECT COLUMN_NAME, 
               extract(xmltype(dbms_xmlgen.getxml('select data_default from all_tab_cols where owner = ''' || owner || ''' and table_name = ''' || table_name || ''' and column_name = ''' || column_name || '''')), '//text()').getStringVal() as DATA_DEFAULT 
        FROM all_tab_cols 
        WHERE owner = :1 
          AND table_name = :2 
          AND virtual_column = 'YES'
    ";
    
    let mut map = std::collections::HashMap::new();
    let stmt_res = conn.statement(sql).build();
    
    if let Ok(mut stmt) = stmt_res {
        info!("Executing Virtual Column Query for {}.{}", schema, table);
        let rows_res = stmt.query(&[&schema.to_uppercase(), &table.to_uppercase()]);
         if let Ok(rows) = rows_res {
             for row in rows.flatten() {
                 let name: Result<String> = row.get(0);
                     let expr: Result<String> = row.get(1);
                     
                     if let (Ok(n), Ok(e)) = (name, expr) {
                         let unescaped_expr = e.replace("&quot;", "\"")
                                               .replace("&apos;", "'")
                                               .replace("&lt;", "<")
                                               .replace("&gt;", ">")
                                               .replace("&amp;", "&");
                         map.insert(n.to_uppercase(), unescaped_expr);
                     }
             }
         }
        }

    
    map
}

/// Fetches the Oracle DDL for a table using DBMS_METADATA.GET_DDL
pub fn get_ddl(conn: &Connection, schema: &str, table: &str) -> Option<String> {
    match conn.query_row(SQL_GET_DDL, &[&table.to_uppercase(), &schema.to_uppercase()]) {
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
pub fn get_primary_key(conn: &Connection, schema: &str, table: &str) -> Result<Option<Vec<String>>> {
    let rows = conn.query(SQL_GET_PK, &[&schema.to_uppercase(), &table.to_uppercase()])?;
    let mut cols = Vec::new();
    
    for row_result in rows {
        let row = row_result?;
        let col: String = row.get(0)?;
        cols.push(col);
    }
    
    if cols.is_empty() {
        Ok(None)
    } else {
        Ok(Some(cols))
    }
}

/// Fetches Partition Key columns
pub fn get_partition_keys(conn: &Connection, schema: &str, table: &str) -> Result<Vec<String>> {
    let rows = conn.query(SQL_GET_PARTITION_KEYS, &[&schema.to_uppercase(), &table.to_uppercase()])?;
    let mut cols = Vec::new();
    for row_result in rows {
        let row = row_result?;
        let col: String = row.get(0)?;
        cols.push(col);
    }
    Ok(cols)
}

/// Fetches all columns involved in indexes
pub fn get_index_columns(conn: &Connection, schema: &str, table: &str) -> Result<Vec<String>> {
    let rows = conn.query(SQL_GET_INDEXES, &[&schema.to_uppercase(), &table.to_uppercase()])?;
    let mut cols = Vec::new();
    for row_result in rows {
        let row = row_result?;
        let col: String = row.get(0)?;
        cols.push(col);
    }
    Ok(cols)
}

/// Attempts to get the CPU count of the Oracle Database Server.
/// Tries `V$OSSTAT` first, then `V$PARAMETER`.
/// Returns Ok(None) if permissions are denied or rows not found.
pub fn get_db_cpu_count(conn: &Connection) -> Result<Option<usize>> {
    // Try v$osstat
    // Requires SELECT privileges on v$osstat
    match conn.query_row("SELECT value FROM v$osstat WHERE stat_name = 'NUM_CPUS'", &[]) {
        Ok(row) => {
            let val: Result<f64> = row.get(0); // Often returns number
            if let Ok(v) = val {
                return Ok(Some(v as usize));
            }
        },
        Err(e) => {
            debug!("Failed to query v$osstat: {}", e);
        }
    }

    // Try v$parameter
    match conn.query_row("SELECT value FROM v$parameter WHERE name = 'cpu_count'", &[]) {
        Ok(row) => {
             let val: Result<String> = row.get(0);
             if let Ok(s) = val {
                 if let Ok(num) = s.parse::<usize>() {
                     return Ok(Some(num));
                 }
             }
        },
        Err(e) => {
            debug!("Failed to query v$parameter: {}", e);
        }
    }

    Ok(None)
}
