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

use oracle::{Connection, Error};
use r2d2::ManageConnection;

/// Custom R2D2 connection manager for Oracle.
///
/// This struct implements the `r2d2::ManageConnection` trait, allowing it to be used
/// with a generic connection pool. It handles the details of establishing
/// a new session with the Oracle database.
///
/// # Security
/// This struct implements `Debug` manually to ensure passwords are **never** logged,
/// even if the application crashes or trace logging is enabled.
#[derive(Clone)]
pub struct OracleConnectionManager {
    user: String,
    pass: String,
    conn_str: String,
}

impl OracleConnectionManager {
    /// Creates a new connection manager.
    ///
    /// # Arguments
    /// * `user` - The database username.
    /// * `pass` - The database password.
    /// * `conn_str` - The Easy Connect string (e.g., `host:port/service`) or TNS alias.
    pub fn new(user: &str, pass: &str, conn_str: &str) -> Self {
        Self {
            user: user.to_string(),
            pass: pass.to_string(),
            conn_str: conn_str.to_string(),
        }
    }
}

impl std::fmt::Display for OracleConnectionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "OracleConnectionManager(user={}, conn={})",
            self.user, self.conn_str
        )
    }
}

// Security: Mask password in Debug output
impl std::fmt::Debug for OracleConnectionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OracleConnectionManager")
            .field("user", &self.user)
            .field("pass", &"*****") // SCRUBBED
            .field("conn_str", &self.conn_str)
            .finish()
    }
}

impl ManageConnection for OracleConnectionManager {
    type Connection = Connection;
    type Error = Error;

    /// Establishes a new connection to the database.
    ///
    /// This is called by the pool when it needs to create a new connection.
    fn connect(&self) -> std::result::Result<Self::Connection, Self::Error> {
        Connection::connect(&self.user, &self.pass, &self.conn_str)
    }

    /// Verifies that the connection is still alive.
    ///
    /// This is called by the pool when retrieving a connection to ensure it hasn't
    /// timed out or been closed by the server. It performs a lightweight `ping`.
    fn is_valid(&self, conn: &mut Self::Connection) -> std::result::Result<(), Self::Error> {
        conn.ping()
    }

    /// Checks if the connection has broken.
    ///
    /// We always return `false` here and rely on `is_valid` (ping) for health checks.
    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}
