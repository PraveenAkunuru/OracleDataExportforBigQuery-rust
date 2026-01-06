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

//! # Type Mapping Logic
//!
//! This module is the "Translator". Oracle and BigQuery speak different languages
//! when it comes to data types. This module ensures that a `NUMBER(10,0)` in
//! Oracle becomes an `INT64` in BigQuery, and so on.
//!
//! We map each Oracle type to three different things:
//! 1. **BigQuery Type**: The logical type used in JSON schemas (e.g., `STRING`).
//! 2. **BigQuery DDL Type**: The physical type used in `CREATE TABLE` statements (e.g., `NUMERIC(10,2)`).
//! 3. **Arrow Type**: The internal memory format used by the `Parquet` writer.

use arrow_schema::DataType;
use oracle::sql_type::OracleType;

/// `MappingEntry` holds the three-way mapping for a single column.
pub struct MappingEntry {
    /// BigQuery schema type (e.g., "STRING", "INT64").
    pub bq_type: String,
    /// BigQuery DDL type (e.g., "NUMERIC(38,9)").
    pub bq_ddl_type: String,
    /// Apache Arrow memory type (e.g., `DataType::Int64`).
    pub arrow_type: DataType,
}

/// The main translation function.
///
/// It takes an `OracleType` (provided by the Oracle driver) and an optional
/// `raw_type` (the string name of the type from Oracle metadata).
pub fn get_mapping(oracle_type: &OracleType, raw_type: Option<&str>) -> MappingEntry {
    // 1. Check for special types (XML, JSON, Geometry) based on raw string.
    if let Some(entry) = map_special_type(raw_type) {
        return entry;
    }

    // 2. Map standard Oracle types.
    match oracle_type {
        OracleType::Number(prec, scale) => map_number_type(prec, scale),

        // Floating point numbers.
        OracleType::Float(_) | OracleType::BinaryFloat | OracleType::BinaryDouble => MappingEntry {
            bq_type: "FLOAT64".to_string(),
            bq_ddl_type: "FLOAT64".to_string(),
            arrow_type: DataType::Float64,
        },

        // Strings and Text.
        OracleType::Char(_)
        | OracleType::NChar(_)
        | OracleType::Varchar2(_)
        | OracleType::NVarchar2(_)
        | OracleType::Long
        | OracleType::CLOB
        | OracleType::NCLOB
        | OracleType::Rowid => MappingEntry {
            bq_type: "STRING".to_string(),
            bq_ddl_type: "STRING".to_string(),
            arrow_type: DataType::Utf8,
        },

        // Dates and Times.
        OracleType::Date | OracleType::Timestamp(_) => MappingEntry {
            bq_type: "DATETIME".to_string(),
            bq_ddl_type: "DATETIME".to_string(),
            arrow_type: DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
        },

        // Timezones.
        OracleType::TimestampTZ(_) | OracleType::TimestampLTZ(_) => MappingEntry {
            bq_type: "TIMESTAMP".to_string(),
            bq_ddl_type: "TIMESTAMP".to_string(),
            arrow_type: DataType::Timestamp(
                arrow_schema::TimeUnit::Microsecond,
                Some("UTC".into()),
            ),
        },

        // Binary Data.
        OracleType::Raw(_) | OracleType::BLOB | OracleType::BFILE => MappingEntry {
            bq_type: "BYTES".to_string(),
            bq_ddl_type: "BYTES".to_string(),
            arrow_type: DataType::Binary,
        },

        // Default fallback.
        _ => MappingEntry {
            bq_type: "STRING".to_string(),
            bq_ddl_type: "STRING".to_string(),
            arrow_type: DataType::Utf8,
        },
    }
}

/// Handle special types detected via raw string (e.g., XMLTYPE, JSON).
fn map_special_type(raw_type: Option<&str>) -> Option<MappingEntry> {
    if let Some(r) = raw_type {
        let upper = r.to_uppercase();
        if upper.contains("XMLTYPE") || upper.contains("SDO_GEOMETRY") || upper.contains("UROWID") {
            return Some(MappingEntry {
                bq_type: "STRING".to_string(),
                bq_ddl_type: "STRING".to_string(),
                arrow_type: DataType::Utf8,
            });
        }
        if upper.contains("JSON") {
            return Some(MappingEntry {
                bq_type: "JSON".to_string(),
                bq_ddl_type: "JSON".to_string(),
                arrow_type: DataType::Utf8,
            });
        }
        if upper.contains("BOOLEAN") {
            return Some(MappingEntry {
                bq_type: "BOOL".to_string(),
                bq_ddl_type: "BOOL".to_string(),
                arrow_type: DataType::Boolean,
            });
        }
    }
    None
}

/// Handle complex NUMBER logic.
fn map_number_type(prec: &u8, scale: &i8) -> MappingEntry {
    let (bq, ddl, arrow) = if *scale == -127 {
        // scale -127 means it's a FLOAT (or undefined precision) in Oracle.
        if *prec == 0 {
            // Undefined precision: Map to STRING to ensure safety (avoid BIGNUMERIC mismatches).
            ("STRING".to_string(), "STRING".to_string(), DataType::Utf8)
        } else {
            (
                "FLOAT64".to_string(),
                "FLOAT64".to_string(),
                DataType::Float64,
            )
        }
    } else if *scale == 0 {
        // scale 0 means it's an INTEGER.
        if *prec > 0 && *prec <= 18 {
            // Fits in a standard 64-bit integer.
            ("INT64".to_string(), "INT64".to_string(), DataType::Int64)
        } else {
            // Too big for INT64, use BIGNUMERIC (Decimal128).
            // If precision is 0, it means "unspecified" (likely 38).
            let p = if *prec == 0 { 38 } else { *prec };
            (
                "BIGNUMERIC".to_string(),
                "BIGNUMERIC".to_string(),
                DataType::Decimal128(p, 0),
            )
        }
    } else {
        // It's a DECIMAL (e.g., NUMBER(10,2)).
        let p = *prec;
        let s = *scale;
        let ddl_str = if (s > 0 && s <= 9) && (p > 0 && p <= 38) {
            format!("NUMERIC({}, {})", p, s)
        } else {
            format!("BIGNUMERIC({}, {})", p, s)
        };
        let arrow_t = if p == 0 {
            // 0 precision (e.g. simple NUMBER) is treated as undefined precision in some contexts.
            // Arrow doesn't allow Decimal128(0, s). Fallback to String.
            DataType::Utf8
        } else if (1..=38).contains(&p) && (0..=38).contains(&s) {
            DataType::Decimal128(p, s)
        } else {
            // Fallback to String for extreme cases.
            DataType::Utf8
        };
        ("BIGNUMERIC".to_string(), ddl_str, arrow_t)
    };
    MappingEntry {
        bq_type: bq,
        bq_ddl_type: ddl,
        arrow_type: arrow,
    }
}

// Helper functions for common lookups.
pub fn map_oracle_to_bq(oracle_type: &OracleType, raw_type: Option<&str>) -> String {
    get_mapping(oracle_type, raw_type).bq_type
}

pub fn map_oracle_to_bq_ddl(oracle_type: &OracleType, raw_type: Option<&str>) -> String {
    get_mapping(oracle_type, raw_type).bq_ddl_type
}

pub fn map_oracle_to_arrow(oracle_type: &OracleType, raw_type: Option<&str>) -> DataType {
    get_mapping(oracle_type, raw_type).arrow_type
}

#[cfg(test)]
mod tests {
    use super::*;
    use oracle::sql_type::OracleType;

    #[test]
    fn test_map_numbers() {
        assert_eq!(map_oracle_to_bq(&OracleType::Number(10, 0), None), "INT64");
        assert_eq!(
            map_oracle_to_bq(&OracleType::Number(38, 0), None),
            "BIGNUMERIC"
        );
        assert_eq!(
            map_oracle_to_bq(&OracleType::Number(10, 2), None),
            "BIGNUMERIC"
        );
    }

    #[test]
    fn test_map_raw_types() {
        assert_eq!(
            map_oracle_to_bq(&OracleType::CLOB, Some("XMLTYPE")),
            "STRING"
        );
        assert_eq!(map_oracle_to_bq(&OracleType::CLOB, Some("JSON")), "JSON");
        assert_eq!(
            map_oracle_to_bq(&OracleType::Boolean, Some("BOOLEAN")),
            "BOOL"
        );
    }

    #[test]
    fn test_map_dates() {
        assert_eq!(map_oracle_to_bq(&OracleType::Date, None), "DATETIME");
        assert_eq!(
            map_oracle_to_bq(&OracleType::Timestamp(6), None),
            "DATETIME"
        );
        assert_eq!(
            map_oracle_to_bq(&OracleType::TimestampTZ(6), None),
            "TIMESTAMP"
        );
    }
}
