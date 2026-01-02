//! Utility functions for generating Oracle SQL expressions.
//!
//! This module provides helpers for building hashes, handling type conversions,
//! and ensuring SQL compatibility across different Oracle data types.

/// Generates a `STANDARD_HASH` expression for a specific column and data type.
///
/// Handles type-specific conversions (e.g., `RAWTOHEX` for RAW, `ROWIDTOCHAR` for ROWID).
/// Returns `None` for excluded types like BLOB or XMLTYPE.
pub fn get_hash_expr_from_str(col_name: &str, data_type: &str) -> Option<String> {
    if is_excluded_type(data_type) {
        return None;
    }

    let upper = data_type.to_uppercase();
    let expr = if upper.contains("RAW") && !upper.contains("LONG") {
        format!("RAWTOHEX(\"{}\")", col_name)
    } else if upper.contains("ROWID") {
        format!("ROWIDTOCHAR(\"{}\")", col_name)
    } else if upper.contains("TIME ZONE") {
        format!(
            "TO_CHAR(SYS_EXTRACT_UTC(\"{}\"), 'YYYY-MM-DD\"T\"HH24:MI:SS.FF6\"Z\"')",
            col_name
        )
    } else {
        format!("TO_CHAR(\"{}\")", col_name)
    };

    Some(format!("STANDARD_HASH(COALESCE({}, ''), 'SHA256')", expr))
}

/// Checks if a data type should be excluded from hash calculations.
fn is_excluded_type(data_str: &str) -> bool {
    let upper = data_str.to_uppercase();
    let base = upper.split('(').next().unwrap_or("").trim();
    matches!(
        base,
        "BLOB" | "CLOB" | "NCLOB" | "BFILE" | "LONG" | "LONG RAW" | "XMLTYPE" | "XML"
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
    if upper_type.contains("TIME ZONE") {
        format!(
            "TO_CHAR(SYS_EXTRACT_UTC(\"{}\"), 'YYYY-MM-DD\"T\"HH24:MI:SS.FF6\"Z\"')",
            name
        )
    } else if upper_type == "XMLTYPE" {
        format!(
            "REPLACE(REPLACE(sys.XMLType.getStringVal(\"{}\"), CHR(10), ''), CHR(13), '')",
            name
        )
    } else if upper_type == "JSON" {
        format!(
            "REPLACE(REPLACE(JSON_SERIALIZE(\"{}\"), CHR(10), ''), CHR(13), '')",
            name
        )
    } else if upper_type == "BOOLEAN" {
        format!("CASE WHEN \"{}\" THEN 'true' ELSE 'false' END", name)
    } else if upper_type.contains("SDO_GEOMETRY") {
        format!("SDO_UTIL.TO_WKTGEOMETRY(\"{}\")", name)
    } else if upper_type.contains("UROWID") || upper_type.contains("ROWID") {
        format!("ROWIDTOCHAR(\"{}\")", name)
    } else if upper_type.contains("INTERVAL YEAR") {
        format!(
            "CASE WHEN \"{}\" IS NULL THEN NULL ELSE \
             CASE WHEN EXTRACT(YEAR FROM \"{}\") < 0 OR EXTRACT(MONTH FROM \"{}\") < 0 THEN '-' ELSE '' END || \
             ABS(EXTRACT(YEAR FROM \"{}\")) || '-' || ABS(EXTRACT(MONTH FROM \"{}\")) || ' 0 0:0:0' END",
            name, name, name, name, name
        )
    } else if upper_type.contains("INTERVAL DAY") {
        format!(
            "CASE WHEN \"{}\" IS NULL THEN NULL ELSE \
             '0-0 ' || CASE WHEN EXTRACT(DAY FROM \"{}\") < 0 OR EXTRACT(HOUR FROM \"{}\") < 0 OR \
             EXTRACT(MINUTE FROM \"{}\") < 0 OR EXTRACT(SECOND FROM \"{}\") < 0 THEN '-' ELSE '' END || \
             ABS(EXTRACT(DAY FROM \"{}\")) || ' ' || ABS(EXTRACT(HOUR FROM \"{}\")) || ':' || \
             ABS(EXTRACT(MINUTE FROM \"{}\")) || ':' || ABS(EXTRACT(SECOND FROM \"{}\")) END",
            name, name, name, name, name, name, name, name, name
        )
    } else if upper_type.contains("INTERVAL") {
        format!("TO_CHAR(\"{}\")", name)
    } else {
        format!("\"{}\"", name)
    }
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
        .map(|c| build_column_expression(&c.name, &c.raw_type))
        .collect();

    if enable_row_hash {
        let hash_parts: Vec<String> = columns
            .iter()
            .filter_map(|c| get_hash_expr_from_str(&c.name, &c.raw_type))
            .collect();
        select_list.push(format!("{} AS ROW_HASH", build_hash_from_parts(&hash_parts)));
    }

    let mut sql = format!(
        "SELECT {} FROM \"{}\".\"{}\"",
        select_list.join(", "),
        schema,
        table
    );

    if let Some(w) = where_clause {
        sql.push_str(" WHERE ");
        sql.push_str(w);
    }

    sql
}
