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

//! # Metadata Port
//!
//! In Hexagonal Architecture, a **Port** is like a "Slot" or a "Contract".
//!
//! This Port defines what it means to "read metadata" from a database.
//! It doesn't care IF the database is Oracle, PostgreSQL, or even a Mock
//! for testing. Any struct that implements the `MetadataPort` trait can
//! be used by the Orchestrator.

use crate::domain::entities::{TableMetadata, ValidationStats};
use crate::domain::errors::Result;

/// `MetadataPort` is a **Trait**. Think of it as an Interface.
///
/// We add `: Send + Sync` here. This is a Rust requirement for types
/// that are shared across multiple threads.
/// - `Send`: safe to send to another thread.
/// - `Sync`: safe to access from multiple threads at the same time.
pub trait MetadataPort: Send + Sync {
    /// Returns a list of all tables in a specific database schema.
    fn get_tables(&self, schema: &str) -> Result<Vec<String>>;

    /// Returns the "Blueprint" (columns, types, sizes) for a single table.
    fn get_table_metadata(&self, schema: &str, table: &str) -> Result<TableMetadata>;

    /// Asks the database server how many CPUs it has (for performance tuning).
    fn get_db_cpu_count(&self) -> Result<usize>;

    /// Plans how to split a large table into smaller chunks.
    /// It returns a list of SQL "WHERE" clauses.
    fn generate_table_chunks(
        &self,
        schema: &str,
        table: &str,
        chunk_count: usize,
    ) -> Result<Vec<String>>;

    /// Queries the source table for counts and sums to verify the export later.
    fn validate_table(
        &self,
        schema: &str,
        table: &str,
        pk_cols: Option<&[String]>,
        agg_cols: Option<&[String]>,
    ) -> Result<ValidationStats>;
}
