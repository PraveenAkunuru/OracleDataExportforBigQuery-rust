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

//! # Runtime Context
//!
//! This module acts as the "Engine Room" of the application. It sets up the
//! underlying resources that the application needs to run efficiently:
//! 1. **Thread Pool**: Using the `rayon` crate for data-parallelism.
//! 2. **Connection Pool**: Using `r2d2` to manage multiple Oracle database connections.
//! 3. **CPU Scaling**: Automatically adjusting the workload based on available CPU cores.

use crate::config::AppConfig;
use crate::domain::errors::{ExportError, Result};
use crate::infrastructure::oracle::connection_manager::OracleConnectionManager;
use r2d2::Pool;
use std::sync::Arc;
use tracing::{info, warn};

/// `RuntimeContext` holds shared resources that exist for the entire life of the app.
pub struct RuntimeContext {
    /// `Arc` stands for "Atomic Reference Counter". It allows multiple parts of the
    /// application to "own" a piece of data safely across different threads.
    pub pool: Arc<Pool<OracleConnectionManager>>,
    /// The number of parallel tasks we can run at once.
    pub num_threads: usize,
}

impl RuntimeContext {
    /// Initializes the global thread pool and Oracle connection pool.
    ///
    /// This function performs "Self-Tuning": it looks at your computer's CPU count
    /// and the `cpu_percent` setting to decide how many threads to start.
    pub fn init(config: &AppConfig) -> Result<Self> {
        // --- STEP 2: CONNECTION POOL SETUP ---

        let conn_str = config.database.get_connection_string();

        // We look for the password in the YAML config, then fall back to an environment variable.
        let password = config
            .database
            .password
            .clone()
            .or_else(|| std::env::var("ORACLE_PASSWORD").ok())
            .unwrap_or_default();

        info!("Initializing connection pool for {}...", conn_str);

        // The manager tells r2d2 *how* to create a new connection.
        let manager = OracleConnectionManager::new(&config.database.username, &password, &conn_str);

        // Initial sizing for the pool. We might resize or just set a safe upper bound.
        // We accept that we might not know the exact thread count yet, but we can guess.
        // A temporary max size is fine; it won't allocate all connections immediately.
        let pool = Pool::builder()
            .max_size(64) // Temporary safe limit, will effectively be limited by thread count usage
            .build(manager)
            .map_err(|e| {
                ExportError::OracleError(format!("Failed to create connection pool: {}", e))
            })?;
        let pool = Arc::new(pool);

        // --- STEP 2: PARALLELISM CALCULATION ---

        // Auto-tuning Logic:
        // 1. If user provided `parallel` -> Use it.
        // 2. If user provided `cpu_percent` -> Use it (Client based).
        // 3. If NEITHER provided -> Query DB for `cpu_count` and use 80% of that (Server based).

        let num_threads = if let Some(p) = config.export.parallel {
            info!("Using explicit parallelism: {} threads", p);
            p
        } else if let Some(pct) = config.export.cpu_percent {
            // Client-side scaling
            let total_cpus = num_cpus::get();
            let p = (total_cpus as f64 * (pct as f64 / 100.0)).ceil() as usize;
            info!(
                "Using client-side CPU scaling: {}% of {} cores = {} threads",
                pct, total_cpus, p
            );
            p
        } else {
            // Server-side safety scaling (Default)
            info!("No parallelism specified. Querying DB for safe default (80% of DB CPUs)...");
            match Self::get_db_cpu_count(&pool) {
                Ok(db_cpus) => {
                    let safe_threads = (db_cpus as f64 * 0.8).floor() as usize;
                    let safe_threads = std::cmp::max(1, safe_threads);
                    info!(
                        "DB Server has {} CPUs. Setting parallelism to {} (80% safe limit).",
                        db_cpus, safe_threads
                    );
                    safe_threads
                }
                Err(e) => {
                    // Fallback if we can't query DB metdata (unlikely if pool works, but good for resilience)
                    let fallback = 2;
                    warn!(
                        "Failed to query DB CPU count ({:?}). Defaulting to safe fallback: {} threads.",
                        e, fallback
                    );
                    fallback
                }
            }
        };

        // Always run at least 1 thread.
        let num_threads = std::cmp::max(1, num_threads);

        // --- STEP 3: GLOBALS INITIALIZATION ---

        // Rayon's global thread pool is used by `.into_par_iter()` throughout the app.
        rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build_global()
            .unwrap_or_else(|e| {
                info!(
                    "Global thread pool already initialized (likely in a test): {}",
                    e
                );
            });

        Ok(Self { pool, num_threads })
    }

    /// Helper to fetch CPU count from Oracle
    fn get_db_cpu_count(pool: &Pool<OracleConnectionManager>) -> Result<usize> {
        let conn = pool
            .get()
            .map_err(|e| ExportError::OracleError(format!("Failed to get connection: {}", e)))?;

        let sql = "SELECT value FROM v$parameter WHERE name = 'cpu_count'";
        let row = conn
            .query_row(sql, &[])
            .map_err(|e| ExportError::OracleError(format!("Failed to query cpu_count: {}", e)))?;

        // v$parameter value is a VARCHAR2
        let count_str: String = row.get(0).map_err(|e| {
            ExportError::OracleError(format!("Failed to get cpu_count column: {}", e))
        })?;

        count_str
            .parse::<usize>()
            .map_err(|_| ExportError::MetadataError("Failed to parse cpu_count".to_string()))
    }
}
