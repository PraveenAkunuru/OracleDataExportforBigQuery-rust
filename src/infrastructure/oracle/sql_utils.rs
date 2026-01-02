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
