// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! # Oracle Metadata Adapter
//!
//! This is the "Brain" for Oracle discovery. It knows how to talk to Oracle's
//! system catalog to find out what tables and columns exist.
//!
//! It also handles "Parallel Chunking" by using the built-in Oracle
//! `DBMS_PARALLEL_EXECUTE` package.

use crate::domain::entities::{ColumnMetadata, TableMetadata};
use crate::domain::errors::{ExportError, Result};
use crate::domain::mapping;
use crate::infrastructure::oracle::connection_manager::OracleConnectionManager;
use crate::infrastructure::oracle::sql_utils;
use crate::ports::metadata_port::MetadataPort;
use log::{debug, info, warn};
use oracle::Connection;
use r2d2::Pool;
use std::sync::Arc;

/// `MetadataAdapter` implements the `MetadataPort`.
pub struct MetadataAdapter {
    pool: Arc<Pool<OracleConnectionManager>>,
}

impl MetadataAdapter {
    pub fn new(pool: Arc<Pool<OracleConnectionManager>>) -> Self {
        Self { pool }
    }

    fn get_conn(&self) -> Result<r2d2::PooledConnection<OracleConnectionManager>> {
        self.pool.get().map_err(|e| {
            ExportError::OracleError(format!("Failed to get connection from pool: {}", e))
        })
    }
}

// --- ORACLE DICTIONARY QUERIES ---
// These are standard SQL queries that look into Oracle's metadata tables.

/// Lists all tables owned by a specific user.
const SQL_LIST_TABLES: &str =
    "SELECT table_name FROM all_tables WHERE owner = :1 ORDER BY table_name";

/// Tries to find the size of a table (in GB).
const SQL_SEGMENTS_SIZE: &str = "SELECT SUM(bytes) / 1024 / 1024 / 1024 FROM all_segments WHERE owner = :1 AND segment_name = :2";

/// Fetches the ROWID ranges for a specific parallel task.
const SQL_FETCH_CHUNKS: &str = "SELECT start_rowid, end_rowid FROM user_parallel_execute_chunks WHERE task_name = :1 ORDER BY start_rowid";

/// Finds primary key columns.
const SQL_GET_PK: &str = "
    SELECT column_name
    FROM all_cons_columns c
    JOIN all_constraints k ON c.constraint_name = k.constraint_name AND c.owner = k.owner
    WHERE k.constraint_type = 'P'
      AND k.owner = :1
      AND k.table_name = :2
    ORDER BY c.position
";

/// THE BIG ONE: Fetches every column in a table along with its type and comments.
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

impl MetadataPort for MetadataAdapter {
    /// Returns a simple list of strings containing table names.
    fn get_tables(&self, schema_name: &str) -> Result<Vec<String>> {
        let conn = self.get_conn()?;
        let rows = conn
            .query(SQL_LIST_TABLES, &[&schema_name.to_uppercase()])
            .map_err(|e| ExportError::OracleError(e.to_string()))?;
        let mut tables = Vec::new();
        for row_result in rows {
            let row = row_result.map_err(|e| ExportError::OracleError(e.to_string()))?;
            let name: String = row
                .get(0)
                .map_err(|e| ExportError::OracleError(e.to_string()))?;
            tables.push(name);
        }
        Ok(tables)
    }

    /// Fetches all the details for a table.
    fn get_table_metadata(&self, schema: &str, table: &str) -> Result<TableMetadata> {
        let conn = self.get_conn()?;
        let schema_up = schema.to_uppercase();
        let table_up = table.to_uppercase();

        // We run multiple small queries to gather all the metadata pieces.
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

    /// Retrieves CPU count from Oracle's parameters (requires SELECT on V$ views).
    fn get_db_cpu_count(&self) -> Result<usize> {
        let conn = self.get_conn()?;
        let row = conn
            .query_row(
                "SELECT value FROM v$parameter WHERE name = 'cpu_count'",
                &[],
            )
            .map_err(|e| ExportError::OracleError(e.to_string()))?;
        let count_str: String = row
            .get(0)
            .map_err(|e| ExportError::OracleError(e.to_string()))?;
        count_str
            .parse::<usize>()
            .map_err(|_| ExportError::MetadataError("Failed to parse cpu_count".to_string()))
    }

    /// CHUNKING LOGIC: Uses Oracle's internal parallel execution package.
    ///
    /// The process is:
    /// 1. Create a "Task" in Oracle.
    /// 2. Oracle calculates "Chunks" (ranges of ROWIDs) based on the tables blocks.
    /// 3. We fetch these ranges into Rust and use them to create parallel export threads.
    /// 4. Drop the task.
    fn generate_table_chunks(
        &self,
        schema: &str,
        table: &str,
        chunk_count: usize,
    ) -> Result<Vec<String>> {
        let conn = self.get_conn()?;
        let task_name = format!("EXP_{}_{}", table, chrono::Utc::now().timestamp());

        // Cleanup any old crashed tasks with the same name.
        let _ = conn.execute(
            "BEGIN DBMS_PARALLEL_EXECUTE.DROP_TASK(:1); END;",
            &[&task_name],
        );
        conn.execute(
            "BEGIN DBMS_PARALLEL_EXECUTE.CREATE_TASK(:1); END;",
            &[&task_name],
        )
        .map_err(|e| {
            ExportError::OracleError(format!(
                "Failed to create parallel task '{}': {}",
                task_name, e
            ))
        })?;

        // Internal function to run the actual chunking logic.
        let run_plan = |conn: &oracle::Connection| -> Result<Vec<String>> {
            // How many blocks are in this table?
            let total_blocks: u64 = match conn.query_row(
                "SELECT blocks FROM all_tables WHERE owner = :1 AND table_name = :2",
                &[&schema.to_uppercase(), &table.to_uppercase()],
            ) {
                Ok(row) => row.get::<usize, u64>(0).unwrap_or(1000),
                Err(_) => 1000,
            };

            // Divide blocks by chunk_count to decide chunk size.
            let blocks_per_chunk = (total_blocks as f64 / chunk_count as f64).ceil() as i64;
            let blocks_per_chunk = std::cmp::max(1, blocks_per_chunk);

            // Oracle actually does the heavy math here.
            conn.execute(
                "BEGIN DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_ROWID(:1, :2, :3, FALSE, :4); END;",
                &[
                    &task_name,
                    &schema.to_uppercase(),
                    &table.to_uppercase(),
                    &blocks_per_chunk,
                ],
            )
            .map_err(|e| {
                ExportError::OracleError(format!(
                    "Failed to create chunks for task '{}': {}",
                    task_name, e
                ))
            })?;

            // Fetch the calculated ROWID ranges.
            let rows = conn.query(SQL_FETCH_CHUNKS, &[&task_name]).map_err(|e| {
                ExportError::OracleError(format!(
                    "Failed to fetch chunks for task '{}': {}",
                    task_name, e
                ))
            })?;

            let mut chunks = Vec::new();
            for r in rows {
                let row = r.map_err(|e| ExportError::OracleError(e.to_string()))?;
                let start: String = row
                    .get(0)
                    .map_err(|e| ExportError::OracleError(e.to_string()))?;
                let end: String = row
                    .get(1)
                    .map_err(|e| ExportError::OracleError(e.to_string()))?;
                chunks.push(format!("ROWID BETWEEN '{}' AND '{}'", start, end));
            }
            Ok(chunks)
        };

        let result = run_plan(&conn);

        // Always drop the task when finished.
        if let Err(e) = conn.execute(
            "BEGIN DBMS_PARALLEL_EXECUTE.DROP_TASK(:1); END;",
            &[&task_name],
        ) {
            warn!("Failed to drop parallel task '{}': {}", task_name, e);
        }

        result
    }

    /// VALIDATION: Queries Oracle for SUMs and COUNTs to verify data fidelity.
    fn validate_table(
        &self,
        schema: &str,
        table: &str,
        pk_cols: Option<&[String]>,
        agg_cols: Option<&[String]>,
    ) -> Result<crate::domain::entities::ValidationStats> {
        let conn = self.get_conn()?;
        info!("Validating {}.{}", schema, table);

        // Row count is the most basic check.
        let count_sql = format!("SELECT COUNT(*) FROM \"{}\".\"{}\"", schema, table);
        let row_count: u64 = conn
            .query_row(&count_sql, &[])
            .map_err(|e| ExportError::OracleError(e.to_string()))?
            .get::<usize, u64>(0)
            .map_err(|e| ExportError::OracleError(e.to_string()))?;

        // Primary Key Hash verify that every row is uniqueness.
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

        // Aggregate sums for numeric columns.
        let mut aggregates = None;
        if let Some(cols) = agg_cols {
            let mut agg_results = Vec::new();
            for col in cols {
                let agg_sql = format!("SELECT SUM(\"{}\") FROM \"{}\".\"{}\"", col, schema, table);
                if let Ok(row) = conn.query_row(&agg_sql, &[]) {
                    if let Ok(Some(v)) = row.get::<usize, Option<String>>(0) {
                        agg_results.push(crate::domain::entities::ColumnAggregate {
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

        Ok(crate::domain::entities::ValidationStats {
            table_name: table.to_string(),
            row_count,
            pk_hash,
            aggregates,
        })
    }
}

impl MetadataAdapter {
    /// Queries the size of a table (in GB) using fallback views.
    fn fetch_size(&self, conn: &oracle::Connection, schema: &str, table: &str) -> Result<f64> {
        // Try ALL_SEGMENTS first (requires privileges).
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

        // Try USER_SEGMENTS (only works for current user).
        if let Ok(mut rows) = conn.query(
            "SELECT SUM(bytes) / 1024 / 1024 / 1024 FROM user_segments WHERE segment_name = :1",
            &[&table],
        ) {
            if let Some(Ok(r)) = rows.next() {
                if let Ok(Some(s)) = r.get::<usize, Option<f64>>(0) {
                    return Ok(s);
                }
            }
        }

        // Final fallback: Estimate size from table stats.
        if let Ok(row) = conn.query_row(
            "SELECT num_rows, avg_row_len FROM all_tables WHERE owner = :1 AND table_name = :2",
            &[&schema, &table],
        ) {
            let num_rows: Option<u64> = row.get(0).unwrap_or(None);
            let avg_len: Option<u64> = row.get(1).unwrap_or(None);
            if let (Some(rows), Some(len)) = (num_rows, avg_len) {
                return Ok((rows as f64 * len as f64) / 1024.0 / 1024.0 / 1024.0);
            }
        }

        Ok(0.0)
    }

    /// Discovers columns and handles a "Fake Query" to get precise data types.
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

        // We filter out internal Oracle columns that users didn't create.
        entries.retain(|e| !e.is_hidden || e.is_user_gen);
        if entries.is_empty() {
            return Ok(vec![]);
        }

        // THE TRICK: We run a "SELECT ... WHERE 1=0" query.
        // This doesn't return any rows, but it DOES return the "Column Info"
        // which tells us exactly what Oracle internal types these columns are.
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
        let virtual_map = get_virtual_columns_map(conn, schema, table);

        for e in entries {
            let otype = e
                .oracle_type
                .unwrap_or(oracle::sql_type::OracleType::Varchar2(4000));

            let v_expr = if e.is_virtual {
                virtual_map.get(&e.name.to_uppercase()).cloned()
            } else {
                None
            };

            final_cols.push(ColumnMetadata {
                name: e.name.clone(),
                raw_type: e.data_type.clone(),
                // Use our domain mapper to bridge Oracle -> BigQuery!
                bq_type: mapping::map_oracle_to_bq(&otype, Some(&e.data_type)),
                is_virtual: e.is_virtual,
                virtual_expr: v_expr,
                is_transformed: sql_utils::is_transformed_type(&e.data_type),
                is_hidden: e.is_hidden,
                is_identity: e.is_identity,
                comment: e.comment,
            });
        }
        Ok(final_cols)
    }

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

    fn fetch_partitions(
        &self,
        conn: &Connection,
        schema: &str,
        table: &str,
    ) -> Result<Vec<String>> {
        let rows = conn
            .query("SELECT column_name FROM all_part_key_columns WHERE owner = :1 AND name = :2 AND object_type = 'TABLE' ORDER BY column_position", &[&schema, &table])
            .map_err(ExportError::from)?;
        let mut parts = Vec::new();
        for r in rows {
            let row = r.map_err(ExportError::from)?;
            parts.push(row.get(0).map_err(ExportError::from)?);
        }
        Ok(parts)
    }

    fn fetch_indexes(&self, conn: &Connection, schema: &str, table: &str) -> Result<Vec<String>> {
        let rows = conn
            .query("SELECT column_name FROM all_ind_columns WHERE table_owner = :1 AND table_name = :2 ORDER BY index_name, column_position", &[&schema, &table])
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

    // Explicitly set log level to debug for this discovery step
    debug!(
        "Fetching virtual column expressions for {}.{}",
        schema, table
    );

    match conn.query(sql, &[&schema.to_uppercase(), &table.to_uppercase()]) {
        Ok(rows) => {
            for row_res in rows {
                match row_res {
                    Ok(row) => {
                        let name: String = row.get(0).unwrap_or_default();
                        let expr: Option<String> = row
                            .get(1)
                            .map_err(|e| {
                                warn!("Failed to fetch DATA_DEFAULT for {}.{}: {}", table, name, e);
                                e
                            })
                            .unwrap_or(None);

                        if let Some(e) = expr {
                            // OCI default buffer for LONG is often 32KB.
                            // If exactly matching or very close, warn about potential truncation.
                            if e.len() >= 32760 {
                                warn!("Virtual column expression for {}.{} is very long ({} bytes). It might be truncated.", 
                                    table, name, e.len());
                            }
                            map.insert(name.to_uppercase(), e.trim().to_string());
                        }
                    }
                    Err(e) => warn!("Error iteration over virtual columns: {}", e),
                }
            }
        }
        Err(e) => warn!(
            "Failed to query virtual columns for {}.{}: {}",
            schema, table, e
        ),
    }
    map
}
