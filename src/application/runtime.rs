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
use log::info;
use r2d2::Pool;
use std::sync::Arc;

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
        // --- STEP 1: PARALLELISM CALCULATION ---

        // We calculate how many CPU cores to use. If the user didn't specify,
        // we default to 50% of the available cores to avoid "freezing" the machine.
        let cpu_percent = config.export.cpu_percent.unwrap_or(50);
        let total_cpus = num_cpus::get();
        let num_threads = config
            .export
            .parallel
            .unwrap_or_else(|| (total_cpus as f64 * (cpu_percent as f64 / 100.0)).ceil() as usize);

        // Always run at least 1 thread.
        let num_threads = std::cmp::max(1, num_threads);

        info!(
            "Initializing worker pool with {} threads (Target CPU: {}%)",
            num_threads, cpu_percent
        );

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

        // We set the pool size slightly larger than the thread count.
        // This ensures that even if all workers are busy, we have a spare
        // connection for metadata lookups or progress tracking.
        let pool_size = (num_threads + 2) as u32;

        let pool = Pool::builder()
            .max_size(pool_size)
            .build(manager)
            .map_err(|e| {
                ExportError::OracleError(format!("Failed to create connection pool: {}", e))
            })?;

        Ok(Self {
            // We wrap the pool in an Arc so we can clone the pointer, not the whole pool.
            pool: Arc::new(pool),
            num_threads,
        })
    }
}
