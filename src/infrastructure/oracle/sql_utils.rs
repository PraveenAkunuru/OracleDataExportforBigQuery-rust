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

//! Utility functions for generating Oracle SQL expressions.
//!
//! This module provides helpers for building hashes, handling type conversions,
//! and ensuring SQL compatibility across different Oracle data types.

/// Securely wraps identifiers in double quotes and escapes existing quotes to prevent SQL injection.
pub fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Generates a `STANDARD_HASH` expression for a specific column and data type.
///
/// Handles type-specific conversions (e.g., `RAWTOHEX` for RAW, `ROWIDTOCHAR` for ROWID).
/// Returns `None` for excluded types like BLOB or XMLTYPE.
pub fn get_hash_expr_from_str(col_name: &str, data_type: &str) -> Option<String> {
    if is_excluded_type(data_type) {
        return None;
    }

    let upper = data_type.to_uppercase();
    let q_col = quote_ident(col_name);

    let expr = if upper.contains("RAW") && !upper.contains("LONG") {
        format!("RAWTOHEX({})", q_col)
    } else if upper.contains("ROWID") {
        format!("ROWIDTOCHAR({})", q_col)
    } else if upper.contains("TIME ZONE") {
        format!(
            "TO_CHAR(SYS_EXTRACT_UTC({}), 'YYYY-MM-DD\"T\"HH24:MI:SS.FF6\"Z\"')",
            q_col
        )
    } else {
        format!("TO_CHAR({})", q_col)
    };

    Some(format!("STANDARD_HASH(COALESCE({}, ''), 'SHA256')", expr))
}

/// Checks if a data type should be excluded from hash calculations.
fn is_excluded_type(data_str: &str) -> bool {
    let upper = data_str.to_uppercase();
    let base = upper.split('(').next().unwrap_or("").trim();
    matches!(
        base,
        "BLOB"
            | "CLOB"
            | "NCLOB"
            | "BFILE"
            | "LONG"
            | "LONG RAW"
            | "XMLTYPE"
            | "XML"
            | "SDO_GEOMETRY"
    )
}

/// Concatenates multiple hash parts into a final SHA256 hash.
///
/// Uses chunking (50 parts per chunk) to avoid exceeding Oracle's literal length limits.
pub fn build_hash_from_parts(hash_parts: &[String]) -> String {
    if hash_parts.is_empty() {
        return "STANDARD_HASH('', 'SHA256')".to_string();
    }

    let chunk_size = 50;
    let mut chunk_hashes = Vec::new();

    for chunk in hash_parts.chunks(chunk_size) {
        let chunk_concat = chunk.join(" || ");
        chunk_hashes.push(format!("STANDARD_HASH({}, 'SHA256')", chunk_concat));
    }

    if chunk_hashes.len() == 1 {
        format!("STANDARD_HASH({}, 'SHA256')", chunk_hashes[0])
    } else {
        format!("STANDARD_HASH({}, 'SHA256')", chunk_hashes.join(" || "))
    }
}

/// Builds an Oracle SQL expression for a specific column and data type.
///
/// Handles types that require transformation before export (e.g., TIMESTAMP TZ to UTC string,
/// XMLType to string, SDO_GEOMETRY to WKT, INTERVALs to ISO-like strings).
pub fn build_column_expression(name: &str, data_type: &str) -> String {
    let upper_type = data_type.to_uppercase();
    let q_name = quote_ident(name);

    if upper_type.contains("TIME ZONE") {
        format!("SYS_EXTRACT_UTC({})", q_name)
    } else if upper_type == "XMLTYPE" {
        format!(
            "REPLACE(REPLACE(sys.XMLType.getClobVal({}), CHR(10), ''), CHR(13), '')",
            q_name
        )
    } else if upper_type == "JSON" {
        format!(
            "REPLACE(REPLACE(JSON_SERIALIZE({}), CHR(10), ''), CHR(13), '')",
            q_name
        )
    } else if upper_type.contains("SDO_GEOMETRY") {
        format!("SDO_UTIL.TO_WKTGEOMETRY({})", q_name)
    } else if upper_type.contains("UROWID") || upper_type.contains("ROWID") {
        format!("ROWIDTOCHAR({})", q_name)
    } else if upper_type.contains("INTERVAL YEAR") {
        format!(
            "CASE WHEN {} IS NULL THEN NULL ELSE \
             CASE WHEN EXTRACT(YEAR FROM {}) < 0 OR EXTRACT(MONTH FROM {}) < 0 THEN '-' ELSE '' END || \
             ABS(EXTRACT(YEAR FROM {})) || '-' || ABS(EXTRACT(MONTH FROM {})) || ' 0 0:0:0' END",
            q_name, q_name, q_name, q_name, q_name
        )
    } else if upper_type.contains("INTERVAL DAY") {
        format!(
            "CASE WHEN {} IS NULL THEN NULL ELSE \
             '0-0 ' || CASE WHEN EXTRACT(DAY FROM {}) < 0 OR EXTRACT(HOUR FROM {}) < 0 OR \
             EXTRACT(MINUTE FROM {}) < 0 OR EXTRACT(SECOND FROM {}) < 0 THEN '-' ELSE '' END || \
             ABS(EXTRACT(DAY FROM {})) || ' ' || ABS(EXTRACT(HOUR FROM {})) || ':' || \
             ABS(EXTRACT(MINUTE FROM {})) || ':' || ABS(EXTRACT(SECOND FROM {})) END",
            q_name, q_name, q_name, q_name, q_name, q_name, q_name, q_name, q_name
        )
    } else if upper_type.contains("INTERVAL") {
        format!("TO_CHAR({})", q_name)
    } else {
        q_name
    }
}

/// Helper to identify if a column type requires a SQL transformation during export.
pub fn is_transformed_type(data_type: &str) -> bool {
    let upper = data_type.to_uppercase();
    upper.contains("TIME ZONE")
        || upper == "XMLTYPE"
        || upper == "JSON"
        || upper.contains("SDO_GEOMETRY")
        || upper.contains("UROWID")
        || upper.contains("ROWID")
        || upper.contains("INTERVAL")
}

/// Builds the full SQL SELECT statement for exporting a table, including optional hash calculation and where clause.
pub fn build_export_query(
    schema: &str,
    table: &str,
    columns: &[crate::domain::entities::ColumnMetadata],
    enable_row_hash: bool,
    where_clause: Option<&str>,
) -> String {
    let mut select_list: Vec<String> = columns
        .iter()
        .filter(|c| !c.is_virtual || c.is_transformed)
        .map(|c| {
            format!(
                "{} AS {}",
                build_column_expression(&c.name, &c.raw_type),
                quote_ident(&c.name)
            )
        })
        .collect();

    if enable_row_hash {
        let hash_parts: Vec<String> = columns
            .iter()
            .filter(|c| !c.is_virtual || c.is_transformed)
            .filter_map(|c| get_hash_expr_from_str(&c.name, &c.raw_type))
            .collect();
        select_list.push(format!(
            "{} AS ROW_HASH",
            build_hash_from_parts(&hash_parts)
        ));
    }

    let mut sql = format!(
        "SELECT {} FROM {}.{}",
        select_list.join(", "),
        quote_ident(schema),
        quote_ident(table)
    );

    if let Some(w) = where_clause {
        sql.push_str(" WHERE ");
        sql.push_str(w);
    }

    sql
}
