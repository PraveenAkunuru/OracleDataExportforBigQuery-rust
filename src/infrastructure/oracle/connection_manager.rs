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

/// Custom R2D2 connection manager for Oracle to ensure consistent library versions.
#[derive(Debug)]
pub struct OracleConnectionManager {
    user: String,
    pass: String,
    conn_str: String,
}

impl OracleConnectionManager {
    pub fn new(user: &str, pass: &str, conn_str: &str) -> Self {
        Self {
            user: user.to_string(),
            pass: pass.to_string(),
            conn_str: conn_str.to_string(),
        }
    }
}

impl ManageConnection for OracleConnectionManager {
    type Connection = Connection;
    type Error = Error;

    fn connect(&self) -> std::result::Result<Self::Connection, Self::Error> {
        Connection::connect(&self.user, &self.pass, &self.conn_str)
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> std::result::Result<(), Self::Error> {
        conn.ping()
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}
