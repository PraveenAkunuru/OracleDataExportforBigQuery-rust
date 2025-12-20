use oracle::sql_type::OracleType;

/// centralized logic for ROW_HASH expression generation.
/// This ensures that the SQL used for validation, the manual export script (export.sql),
/// and the actual application export query all use the EXACT same hashing logic.
pub fn get_hash_expr(col_name: &str, oracle_type: &OracleType) -> Option<String> {
    // Check exclusion types based on Python legacy behavior + limitations
    let skip = matches!(oracle_type, 
        OracleType::BLOB | 
        OracleType::CLOB | 
        OracleType::NCLOB | 
        OracleType::BFILE | 
        OracleType::Long | 
        OracleType::Xml
    );

    if skip {
        return None;
    }

    // Determine the transformation wrap
    let expr = match oracle_type {
        OracleType::Raw(_) => format!("RAWTOHEX(\"{}\")", col_name),
        OracleType::Rowid => format!("ROWIDTOCHAR(\"{}\")", col_name),
        OracleType::TimestampTZ(_) | OracleType::TimestampLTZ(_) => {
            // Normalize TimeZone to UTC string for consistent hashing across systems
            format!("TO_CHAR(SYS_EXTRACT_UTC(\"{}\"), 'YYYY-MM-DD\"T\"HH24:MI:SS.FF6\"Z\"')", col_name)
        },
        OracleType::Date | OracleType::Timestamp(_) => {
             // Standard Date/Timestamp -> explicit TO_CHAR to avoid locale variance
             format!("TO_CHAR(\"{}\")", col_name)
        },
        _ => {
            // Default: "COL" -> TO_CHAR("COL")
            // Note: We use TO_CHAR implicitly for consistency to stringify numbers/varchars
            format!("TO_CHAR(\"{}\")", col_name)
        }
    };
    
    // Final wrap: STANDARD_HASH(COALESCE(expr, ''), 'SHA256')
    // We treat NULL as empty string for hashing purposes (Python legacy).
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

