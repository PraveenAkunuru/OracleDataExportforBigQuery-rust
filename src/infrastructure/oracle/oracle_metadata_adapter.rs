//! Infrastructure adapter for reading Oracle schema metadata and calculating chunks.

use crate::domain::bq_type_mapper;
use crate::domain::error_definitions::{ExportError, Result};
use crate::domain::export_models::{ColumnMetadata, TableMetadata};
use crate::ports::schema_reader::SchemaReader;
use log::{debug, info, warn};
use oracle::Connection;

/// Concrete implementation of `SchemaReader` for Oracle databases.
///
/// This adapter uses standard Oracle system views and the `DBMS_PARALLEL_EXECUTE`
/// package to discover schema information and plan parallel workloads.
pub struct OracleMetadataAdapter {
    conn_str: String,
    user: String,
    pass: String,
}

impl OracleMetadataAdapter {
    /// Creates a new OracleMetadataAdapter with connection details.
    pub fn new(conn_str: String, user: String, pass: String) -> Self {
        Self {
            conn_str,
            user,
            pass,
        }
    }

    /// Establishes a fresh connection to the Oracle database.
    fn get_conn(&self) -> Result<Connection> {
        Connection::connect(&self.user, &self.pass, &self.conn_str).map_err(ExportError::from)
    }
}

// SQL Constants (Preserved from oracle_metadata.rs)
const SQL_LIST_TABLES: &str =
    "SELECT table_name FROM all_tables WHERE owner = :1 ORDER BY table_name";
const SQL_SEGMENTS_SIZE: &str = "SELECT SUM(bytes) / 1024 / 1024 / 1024 FROM all_segments WHERE owner = :1 AND segment_name = :2";
const SQL_USER_SEGMENTS_SIZE: &str =
    "SELECT SUM(bytes) / 1024 / 1024 / 1024 FROM user_segments WHERE segment_name = :1";
const SQL_TABLE_STATS: &str =
    "SELECT num_rows, avg_row_len FROM all_tables WHERE owner = :1 AND table_name = :2";
const SQL_TABLE_BLOCKS: &str = "SELECT blocks FROM all_tables WHERE owner = :1 AND table_name = :2";
const SQL_FETCH_CHUNKS: &str = "SELECT start_rowid, end_rowid FROM user_parallel_execute_chunks WHERE task_name = :1 ORDER BY start_rowid";
const SQL_GET_PK: &str = "
    SELECT column_name
    FROM all_cons_columns c
    JOIN all_constraints k ON c.constraint_name = k.constraint_name AND c.owner = k.owner
    WHERE k.constraint_type = 'P'
      AND k.owner = :1
      AND k.table_name = :2
    ORDER BY c.position
";
pub const SQL_GET_COLUMNS: &str = "
    SELECT c.column_name, c.data_type, cm.comments, c.virtual_column, c.hidden_column, c.identity_column, c.user_generated
    FROM all_tab_cols c
    LEFT JOIN all_col_comments cm 
      ON c.owner = cm.owner 
      AND c.table_name = cm.table_name 
      AND c.column_name = cm.column_name
    WHERE c.table_name = :1 
      AND c.owner = :2 
    ORDER BY c.column_id, c.internal_column_id
";
const SQL_GET_PARTITION_KEYS: &str = "
    SELECT column_name
    FROM all_part_key_columns
    WHERE owner = :1 AND name = :2 AND object_type = 'TABLE'
    ORDER BY column_position
";
const SQL_GET_INDEXES: &str = "
    SELECT column_name
    FROM all_ind_columns
    WHERE table_owner = :1 AND table_name = :2
    ORDER BY index_name, column_position
";

impl SchemaReader for OracleMetadataAdapter {
    fn get_tables(&self, schema_name: &str) -> Result<Vec<String>> {
        let conn = self.get_conn()?;
        let rows = conn
            .query(SQL_LIST_TABLES, &[&schema_name.to_uppercase()])
            .map_err(ExportError::from)?;
        let mut tables = Vec::new();
        for row_result in rows {
            let row = row_result.map_err(ExportError::from)?;
            let name: String = row.get(0).map_err(ExportError::from)?;
            tables.push(name);
        }
        Ok(tables)
    }

    fn get_table_metadata(&self, schema: &str, table: &str) -> Result<TableMetadata> {
        let conn = self.get_conn()?;
        let schema_up = schema.to_uppercase();
        let table_up = table.to_uppercase();

        let size_gb = self.fetch_size(&conn, &schema_up, &table_up)?;
        let columns = self.fetch_columns(&conn, &schema_up, &table_up)?;
        let pk_cols = self.fetch_pk(&conn, &schema_up, &table_up)?;
        let partition_cols = self.fetch_partitions(&conn, &schema_up, &table_up)?;
        let index_cols = self.fetch_indexes(&conn, &schema_up, &table_up)?;

        Ok(TableMetadata {
            schema: schema.to_string(),
            table_name: table.to_string(),
            columns,
            size_gb,
            pk_cols,
            partition_cols,
            index_cols,
        })
    }

    fn get_db_cpu_count(&self) -> Result<usize> {
        let conn = self.get_conn()?;
        let row = conn
            .query_row(
                "SELECT value FROM v$parameter WHERE name = 'cpu_count'",
                &[],
            )
            .map_err(ExportError::from)?;
        let count_str: String = row.get(0).map_err(ExportError::from)?;
        count_str
            .parse::<usize>()
            .map_err(|_| ExportError::MetadataError("Failed to parse cpu_count".to_string()))
    }

    fn generate_table_chunks(
        &self,
        schema: &str,
        table: &str,
        chunk_count: usize,
    ) -> Result<Vec<String>> {
        let conn = self.get_conn()?;
        let task_name = format!("EXP_{}_{}", table, chrono::Utc::now().timestamp());

        let _ = conn.execute(
            "BEGIN DBMS_PARALLEL_EXECUTE.DROP_TASK(:1); END;",
            &[&task_name],
        );
        conn.execute(
            "BEGIN DBMS_PARALLEL_EXECUTE.CREATE_TASK(:1); END;",
            &[&task_name],
        )
        .map_err(ExportError::from)?;

        let total_blocks: u64 = match conn.query_row(
            SQL_TABLE_BLOCKS,
            &[&schema.to_uppercase(), &table.to_uppercase()],
        ) {
            Ok(row) => row.get::<usize, u64>(0).unwrap_or(1000),
            Err(_) => 1000,
        };

        let blocks_per_chunk = (total_blocks as f64 / chunk_count as f64).ceil() as i64;
        let blocks_per_chunk = std::cmp::max(1, blocks_per_chunk);

        conn.execute(
            "BEGIN DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_ROWID(:1, :2, :3, FALSE, :4); END;",
            &[
                &task_name,
                &schema.to_uppercase(),
                &table.to_uppercase(),
                &blocks_per_chunk,
            ],
        )
        .map_err(ExportError::from)?;

        let rows = conn
            .query(SQL_FETCH_CHUNKS, &[&task_name])
            .map_err(ExportError::from)?;
        let mut chunks = Vec::new();
        for r in rows {
            let row = r.map_err(ExportError::from)?;
            let start: String = row.get(0).map_err(ExportError::from)?;
            let end: String = row.get(1).map_err(ExportError::from)?;
            chunks.push(format!("ROWID BETWEEN '{}' AND '{}'", start, end));
        }

        let _ = conn.execute(
            "BEGIN DBMS_PARALLEL_EXECUTE.DROP_TASK(:1); END;",
            &[&task_name],
        );
        Ok(chunks)
    }

    fn validate_table(
        &self,
        schema: &str,
        table: &str,
        pk_cols: Option<&[String]>,
        agg_cols: Option<&[String]>,
    ) -> Result<crate::domain::export_models::ValidationStats> {
        let conn = self.get_conn()?;
        info!("Validating {}.{}", schema, table);

        // Tier 1: Row Count
        let count_sql = format!("SELECT COUNT(*) FROM \"{}\".\"{}\"", schema, table);
        let row_count: u64 = conn
            .query_row(&count_sql, &[])
            .map_err(ExportError::from)?
            .get::<usize, u64>(0)
            .map_err(ExportError::from)?;

        // Tier 2: PK Hash
        let mut pk_hash = None;
        if let Some(pks) = pk_cols {
            if !pks.is_empty() {
                let hash_expr = if pks.len() == 1 {
                    format!("\"{}\"", pks[0])
                } else {
                    pks.iter()
                        .map(|c| format!("\"{}\"", c))
                        .collect::<Vec<_>>()
                        .join(" || '_' || ")
                };

                let hash_sql = format!(
                    "SELECT SUM(ORA_HASH({})) FROM \"{}\".\"{}\"",
                    hash_expr, schema, table
                );
                match conn.query_row(&hash_sql, &[]) {
                    Ok(row) => {
                        pk_hash = row.get::<usize, Option<String>>(0).ok().flatten();
                    }
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
                if let Ok(row) = conn.query_row(&agg_sql, &[]) {
                    if let Ok(Some(v)) = row.get::<usize, Option<String>>(0) {
                        agg_results.push(crate::domain::export_models::ColumnAggregate {
                            column_name: col.clone(),
                            agg_type: "SUM".to_string(),
                            value: v,
                        });
                    }
                }
            }
            if !agg_results.is_empty() {
                aggregates = Some(agg_results);
            }
        }

        Ok(crate::domain::export_models::ValidationStats {
            table_name: table.to_string(),
            row_count,
            pk_hash,
            aggregates,
        })
    }
}

impl OracleMetadataAdapter {
    /// Queries the size of a table in Gigabytes using multiple fallback strategies.
    fn fetch_size(&self, conn: &Connection, schema: &str, table: &str) -> Result<f64> {
        let size_res = conn.query(SQL_SEGMENTS_SIZE, &[&schema, &table]);
        match size_res {
            Ok(mut rows) => {
                if let Some(Ok(r)) = rows.next() {
                    if let Ok(Some(s)) = r.get::<usize, Option<f64>>(0) {
                        return Ok(s);
                    }
                }
            }
            Err(e) => warn!("Failed to query ALL_SEGMENTS: {}", e),
        }

        if let Ok(mut rows) = conn.query(SQL_USER_SEGMENTS_SIZE, &[&table]) {
            if let Some(Ok(r)) = rows.next() {
                if let Ok(Some(s)) = r.get::<usize, Option<f64>>(0) {
                    return Ok(s);
                }
            }
        }

        if let Ok(row) = conn.query_row(SQL_TABLE_STATS, &[&schema, &table]) {
            let num_rows: Option<u64> = row.get(0).unwrap_or(None);
            let avg_len: Option<u64> = row.get(1).unwrap_or(None);
            if let (Some(rows), Some(len)) = (num_rows, avg_len) {
                return Ok((rows as f64 * len as f64) / 1024.0 / 1024.0 / 1024.0);
            }
        }

        Ok(0.0)
    }

    /// Discovers all columns for a table and maps them to BigQuery types.
    fn fetch_columns(
        &self,
        conn: &Connection,
        schema: &str,
        table: &str,
    ) -> Result<Vec<ColumnMetadata>> {
        debug!("Fetching columns for {}.{}", schema, table);
        let mut stmt_meta = conn
            .statement(SQL_GET_COLUMNS)
            .build()
            .map_err(ExportError::from)?;
        let rows_meta = stmt_meta
            .query(&[&table.to_uppercase(), &schema.to_uppercase()])
            .map_err(ExportError::from)?;

        struct ColEntry {
            name: String,
            data_type: String,
            comment: Option<String>,
            is_virtual: bool,
            is_hidden: bool,
            is_identity: bool,
            is_user_gen: bool,
            oracle_type: Option<oracle::sql_type::OracleType>,
        }

        let mut entries = Vec::new();
        for row_res in rows_meta {
            let row = row_res.map_err(ExportError::from)?;
            entries.push(ColEntry {
                name: row.get(0).map_err(ExportError::from)?,
                data_type: row.get(1).map_err(ExportError::from)?,
                comment: row.get(2).map_err(ExportError::from)?,
                is_virtual: row.get::<usize, String>(3).map_err(ExportError::from)? == "YES",
                is_hidden: row.get::<usize, String>(4).map_err(ExportError::from)? == "YES",
                is_identity: row.get::<usize, String>(5).map_err(ExportError::from)? == "YES",
                is_user_gen: row.get::<usize, String>(6).map_err(ExportError::from)? == "YES",
                oracle_type: None,
            });
        }

        entries.retain(|e| !e.is_hidden || e.is_user_gen);
        if entries.is_empty() {
            return Ok(vec![]);
        }

        let quoted_names: Vec<String> = entries.iter().map(|e| format!("\"{}\"", e.name)).collect();
        let sql_dummy = format!(
            "SELECT {} FROM \"{}\".\"{}\" WHERE 1=0",
            quoted_names.join(", "),
            schema,
            table
        );
        let mut stmt = conn
            .statement(&sql_dummy)
            .build()
            .map_err(ExportError::from)?;
        let rows = stmt.query(&[]).map_err(ExportError::from)?;
        let col_infos = rows.column_info();

        for (i, c) in col_infos.iter().enumerate() {
            if i < entries.len() {
                entries[i].oracle_type = Some(c.oracle_type().clone());
            }
        }

        let mut final_cols = Vec::new();
        for e in entries {
            let otype = e
                .oracle_type
                .unwrap_or(oracle::sql_type::OracleType::Varchar2(4000));
            final_cols.push(ColumnMetadata {
                name: e.name.clone(),
                raw_type: e.data_type.clone(),
                bq_type: bq_type_mapper::map_oracle_to_bq(&otype, Some(&e.data_type)),
                is_virtual: e.is_virtual,
                is_hidden: e.is_hidden,
                is_identity: e.is_identity,
                comment: e.comment,
            });
        }
        Ok(final_cols)
    }

    /// Fetches primary key column names for the specified table.
    fn fetch_pk(&self, conn: &Connection, schema: &str, table: &str) -> Result<Vec<String>> {
        let rows = conn
            .query(SQL_GET_PK, &[&schema, &table])
            .map_err(ExportError::from)?;
        let mut pks = Vec::new();
        for r in rows {
            let row = r.map_err(ExportError::from)?;
            pks.push(row.get(0).map_err(ExportError::from)?);
        }
        Ok(pks)
    }

    /// Fetches partition key column names for the specified table.
    fn fetch_partitions(
        &self,
        conn: &Connection,
        schema: &str,
        table: &str,
    ) -> Result<Vec<String>> {
        let rows = conn
            .query(SQL_GET_PARTITION_KEYS, &[&schema, &table])
            .map_err(ExportError::from)?;
        let mut parts = Vec::new();
        for r in rows {
            let row = r.map_err(ExportError::from)?;
            parts.push(row.get(0).map_err(ExportError::from)?);
        }
        Ok(parts)
    }

    /// Fetches all indexed column names for the specified table.
    fn fetch_indexes(&self, conn: &Connection, schema: &str, table: &str) -> Result<Vec<String>> {
        let rows = conn
            .query(SQL_GET_INDEXES, &[&schema, &table])
            .map_err(ExportError::from)?;
        let mut idxs = Vec::new();
        for r in rows {
            let row = r.map_err(ExportError::from)?;
            idxs.push(row.get(0).map_err(ExportError::from)?);
        }
        Ok(idxs)
    }
}

/// Retrieves a map of virtual column names to their SQL expressions.
///
/// This is used during BigQuery View generation to restore the logic
/// of Oracle virtual columns.
pub fn get_virtual_columns_map(
    conn: &Connection,
    schema: &str,
    table: &str,
) -> std::collections::HashMap<String, String> {
    let sql = "SELECT column_name, data_default FROM all_tab_cols WHERE owner = :1 AND table_name = :2 AND virtual_column = 'YES'";
    let mut map = std::collections::HashMap::new();
    if let Ok(rows) = conn.query(sql, &[&schema.to_uppercase(), &table.to_uppercase()]) {
        for row in rows.flatten() {
            let name: String = row.get(0).unwrap_or_default();
            let expr: Option<String> = row.get(1).unwrap_or(None);
            if let Some(e) = expr {
                map.insert(name.to_uppercase(), e);
            }
        }
    }
    map
}
