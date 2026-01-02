//! Core logic for mapping Oracle source types to BigQuery target types.
//!
//! This module handles the conversion of various Oracle data types (including
//! specialized types like XMLTYPE and SDO_GEOMETRY) into their BigQuery
//! equivalents used in schema generation and DDL creation.

use oracle::sql_type::OracleType;

/// Maps an Oracle `sql_type::OracleType` and an optional raw type name to a BigQuery data type.
pub fn map_oracle_to_bq(oracle_type: &OracleType, raw_type: Option<&str>) -> String {
    if let Some(r) = raw_type {
        let upper = r.to_uppercase();
        if upper.contains("XMLTYPE") {
            return "STRING".to_string();
        }
        if upper.contains("JSON") {
            return "JSON".to_string();
        }
        if upper.contains("BOOLEAN") {
            return "BOOL".to_string();
        }
        if upper.contains("SDO_GEOMETRY") {
            return "STRING".to_string();
        }
        if upper.contains("UROWID") {
            return "STRING".to_string();
        }
    }

    match oracle_type {
        OracleType::Number(precision, scale) => {
            if *scale == 0 {
                if *precision > 0 && *precision <= 18 {
                    "INT64".to_string()
                } else {
                    "BIGNUMERIC".to_string()
                }
            } else if *scale == -127 {
                if *precision == 0 {
                    "BIGNUMERIC".to_string()
                } else {
                    "FLOAT64".to_string()
                }
            } else {
                "BIGNUMERIC".to_string()
            }
        }
        OracleType::Int64 => "INTEGER".to_string(),
        OracleType::Float(_) | OracleType::BinaryFloat | OracleType::BinaryDouble => {
            "FLOAT64".to_string()
        }
        OracleType::Char(_)
        | OracleType::NChar(_)
        | OracleType::Varchar2(_)
        | OracleType::NVarchar2(_)
        | OracleType::Long
        | OracleType::CLOB
        | OracleType::NCLOB
        | OracleType::Rowid => "STRING".to_string(),
        OracleType::Date | OracleType::Timestamp(_) => "DATETIME".to_string(),
        OracleType::TimestampTZ(_) | OracleType::TimestampLTZ(_) => "TIMESTAMP".to_string(),
        OracleType::IntervalYM(..) | OracleType::IntervalDS(..) => "INTERVAL".to_string(),
        OracleType::Xml => "STRING".to_string(),
        OracleType::Boolean => "BOOL".to_string(),
        OracleType::Raw(_) | OracleType::BLOB | OracleType::BFILE => "BYTES".to_string(),
        _ => "STRING".to_string(),
    }
}

/// Maps Oracle DB types to BigQuery SQL DDL types.
pub fn map_oracle_to_bq_ddl(oracle_type: &OracleType, raw_type: Option<&str>) -> String {
    if let Some(r) = raw_type {
        let upper = r.to_uppercase();
        if upper.contains("XMLTYPE") {
            return "STRING".to_string();
        }
        if upper.contains("JSON") {
            return "JSON".to_string();
        }
        if upper.contains("BOOLEAN") {
            return "BOOL".to_string();
        }
    }

    match oracle_type {
        OracleType::Number(prec, scale) => {
            if *scale == -127 {
                if *prec == 0 {
                    "BIGNUMERIC".to_string()
                } else {
                    "FLOAT64".to_string()
                }
            } else if *scale == 0 {
                if *prec > 0 && *prec <= 18 {
                    "INT64".to_string()
                } else {
                    "BIGNUMERIC".to_string()
                }
            } else {
                let p = *prec;
                let s = *scale;
                if (s > 0 && s <= 9) && (p > 0 && p <= 38) {
                    format!("NUMERIC({}, {})", p, s)
                } else {
                    format!("BIGNUMERIC({}, {})", p, s)
                }
            }
        }
        t => map_oracle_to_bq(t, raw_type),
    }
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
