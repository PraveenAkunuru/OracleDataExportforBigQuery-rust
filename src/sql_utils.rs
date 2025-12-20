use oracle::sql_type::OracleType;

/// Internal helper to check if a type should be skipped for hashing.
fn is_excluded_type(data_str: &str) -> bool {
    let upper = data_str.to_uppercase();
    // Match base type before any (scale, precision)
    let base = upper.split('(').next().unwrap_or("").trim();
    matches!(base, "BLOB" | "CLOB" | "NCLOB" | "BFILE" | "LONG" | "LONG RAW" | "XMLTYPE" | "XML")
}

/// Centralized logic for ROW_HASH expression generation using OracleType.
pub fn get_hash_expr(col_name: &str, oracle_type: &OracleType) -> Option<String> {
    let type_str = format!("{:?}", oracle_type).to_uppercase();
    if is_excluded_type(&type_str) {
        return None;
    }

    let expr = match oracle_type {
        OracleType::Raw(_) => format!("RAWTOHEX(\"{}\")", col_name),
        OracleType::Rowid => format!("ROWIDTOCHAR(\"{}\")", col_name),
        OracleType::TimestampTZ(_) | OracleType::TimestampLTZ(_) => {
            format!("TO_CHAR(SYS_EXTRACT_UTC(\"{}\"), 'YYYY-MM-DD\"T\"HH24:MI:SS.FF6\"Z\"')", col_name)
        },
        OracleType::Date | OracleType::Timestamp(_) => {
             format!("TO_CHAR(\"{}\")", col_name)
        },
        _ => format!("TO_CHAR(\"{}\")", col_name),
    };
    
    Some(format!("STANDARD_HASH(COALESCE({}, ''), 'SHA256')", expr))
}

/// Centralized logic for ROW_HASH expression generation using string-based types.
/// Useful for metadata queries where full OracleType enum isn't available.
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
        format!("TO_CHAR(SYS_EXTRACT_UTC(\"{}\"), 'YYYY-MM-DD\"T\"HH24:MI:SS.FF6\"Z\"')", col_name)
    } else {
        format!("TO_CHAR(\"{}\")", col_name)
    };

    Some(format!("STANDARD_HASH(COALESCE({}, ''), 'SHA256')", expr))
}

/// Helper to generate the full ROW_HASH column definition from a list of columns
/// Handles the chunking logic (concatenating 50 cols at a time to avoid limits).
pub fn build_row_hash_select(columns: &[String], types: &[OracleType]) -> String {
    let mut hash_parts = Vec::new();

    for (name, otype) in columns.iter().zip(types.iter()) {
        if let Some(h) = get_hash_expr(name, otype) {
             hash_parts.push(h);
        }
    }

    if hash_parts.is_empty() {
        return "STANDARD_HASH('', 'SHA256') AS ROW_HASH".to_string();
    }

    let chunk_size = 50;
    let mut chunk_hashes = Vec::new();
    
    for chunk in hash_parts.chunks(chunk_size) {
        // Concatenate the hashes of up to 50 columns
        let chunk_concat = chunk.join(" || ");
        // Hash that concatenation
        chunk_hashes.push(format!("STANDARD_HASH({}, 'SHA256')", chunk_concat));
    }
    
    // Finally, if we have multiple chunks (e.g. >50 cols), we concat and hash again (or just concat?).
    // Python legacy: It does `STANDARD_HASH( ... )` on the whole massive string if < limit.
    // If > limit, it might need staged hashing. 
    // The previous code used `STANDARD_HASH( chunk_hashes.join(" || ") , 'SHA256')`.
    // This implies we stick to: Hash( Hash(Chunk1) || Hash(Chunk2) ... )
    // This is safer for Oracle string internal limits (4000/32k) if the row is huge.
    
    if chunk_hashes.len() == 1 {
        format!("STANDARD_HASH({}, 'SHA256') AS ROW_HASH", chunk_hashes[0])
    } else {
        format!("STANDARD_HASH({}, 'SHA256') AS ROW_HASH", chunk_hashes.join(" || "))
    }
}

/// Helper to build ROW_HASH from pre-computed hash parts (strings).
/// Useful when we only have string types/expressions (e.g. from all_tab_columns).
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

