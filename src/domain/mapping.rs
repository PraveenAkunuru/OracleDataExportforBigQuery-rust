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

/// Returns the BigQuery logical type (e.g., "STRING") and DDL type (e.g., "NUMERIC(10,2)").
pub fn map_oracle_to_bq_types(oracle_type: &OracleType, raw_type: Option<&str>) -> (String, String) {
    // 1. Check for special types
    if let Some((bq, ddl)) = map_special_type_bq(raw_type) {
        return (bq, ddl);
    }

    // 2. Standard Oracle types
    match oracle_type {
        OracleType::Number(prec, scale) => map_number_type_bq(prec, scale),
        
        OracleType::Float(_) | OracleType::BinaryFloat | OracleType::BinaryDouble => (
            "FLOAT64".to_string(),
            "FLOAT64".to_string(),
        ),

        OracleType::Char(_)
        | OracleType::NChar(_)
        | OracleType::Varchar2(_)
        | OracleType::NVarchar2(_)
        | OracleType::Long
        | OracleType::CLOB
        | OracleType::NCLOB
        | OracleType::Rowid => (
            "STRING".to_string(),
            "STRING".to_string(),
        ),

        OracleType::Date | OracleType::Timestamp(_) => (
            "DATETIME".to_string(),
            "DATETIME".to_string(),
        ),

        OracleType::TimestampTZ(_) | OracleType::TimestampLTZ(_) => (
            "TIMESTAMP".to_string(),
            "TIMESTAMP".to_string(),
        ),

        OracleType::Raw(_) | OracleType::BLOB | OracleType::BFILE => (
            "BYTES".to_string(),
            "BYTES".to_string(),
        ),

        _ => ("STRING".to_string(), "STRING".to_string()),
    }
}

/// Returns the Arrow DataType for Parquet export.
pub fn map_oracle_to_arrow(oracle_type: &OracleType, raw_type: Option<&str>) -> DataType {
    // 1. Check for special types
    if let Some(arrow) = map_special_type_arrow(raw_type) {
        return arrow;
    }

    // 2. Standard Oracle types
    match oracle_type {
        OracleType::Number(prec, scale) => map_number_type_arrow(prec, scale),

        OracleType::Float(_) | OracleType::BinaryFloat | OracleType::BinaryDouble => DataType::Float64,

        OracleType::Char(_)
        | OracleType::NChar(_)
        | OracleType::Varchar2(_)
        | OracleType::NVarchar2(_)
        | OracleType::Long
        | OracleType::CLOB
        | OracleType::NCLOB
        | OracleType::Rowid => DataType::Utf8,

        OracleType::Date | OracleType::Timestamp(_) => DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),

        OracleType::TimestampTZ(_) | OracleType::TimestampLTZ(_) => DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),

        OracleType::Raw(_) | OracleType::BLOB | OracleType::BFILE => DataType::Binary,

        _ => DataType::Utf8,
    }
}

fn map_special_type_bq(raw_type: Option<&str>) -> Option<(String, String)> {
    if let Some(r) = raw_type {
        let upper = r.to_uppercase();
        if upper.contains("XMLTYPE") || upper.contains("SDO_GEOMETRY") || upper.contains("UROWID") {
            return Some(("STRING".to_string(), "STRING".to_string()));
        }
        if upper.contains("JSON") {
            return Some(("JSON".to_string(), "JSON".to_string()));
        }
        if upper.contains("BOOLEAN") {
            return Some(("BOOL".to_string(), "BOOL".to_string()));
        }
    }
    None
}

fn map_special_type_arrow(raw_type: Option<&str>) -> Option<DataType> {
    if let Some(r) = raw_type {
        let upper = r.to_uppercase();
        if upper.contains("XMLTYPE") || upper.contains("SDO_GEOMETRY") || upper.contains("UROWID") {
            return Some(DataType::Utf8);
        }
        if upper.contains("JSON") {
            return Some(DataType::Utf8);
        }
        if upper.contains("BOOLEAN") {
            return Some(DataType::Boolean);
        }
    }
    None
}

fn map_number_type_bq(prec: &u8, scale: &i8) -> (String, String) {
    if *scale == -127 {
        if *prec == 0 { ("STRING".to_string(), "STRING".to_string()) } else { ("FLOAT64".to_string(), "FLOAT64".to_string()) }
    } else if *scale == 0 {
        if *prec > 0 && *prec <= 18 {
             ("INT64".to_string(), "INT64".to_string())
        } else {
             ("BIGNUMERIC".to_string(), "BIGNUMERIC".to_string())
        }
    } else {
        let p = *prec;
        let s = *scale;
        let ddl_str = if (s > 0 && s <= 9) && (p > 0 && p <= 38) {
            format!("NUMERIC({}, {})", p, s)
        } else {
            format!("BIGNUMERIC({}, {})", p, s)
        };
        ("BIGNUMERIC".to_string(), ddl_str)
    }
}

fn map_number_type_arrow(prec: &u8, scale: &i8) -> DataType {
    if *scale == -127 {
        if *prec == 0 { DataType::Utf8 } else { DataType::Float64 }
    } else if *scale == 0 {
        if *prec > 0 && *prec <= 18 {
             DataType::Int64
        } else {
             DataType::Decimal128(*prec, 0)
        }
    } else {
        let p = *prec;
        let s = *scale;
        if (1..=38).contains(&p) && (0..=38).contains(&s) {
            DataType::Decimal128(p, s)
        } else {
            DataType::Utf8
        }
    }
}

// Helper functions for common lookups.
pub fn map_oracle_to_bq(oracle_type: &OracleType, raw_type: Option<&str>) -> String {
    map_oracle_to_bq_types(oracle_type, raw_type).0
}

pub fn map_oracle_to_bq_ddl(oracle_type: &OracleType, raw_type: Option<&str>) -> String {
    map_oracle_to_bq_types(oracle_type, raw_type).1
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
