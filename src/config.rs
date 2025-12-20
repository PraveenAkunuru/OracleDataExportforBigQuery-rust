//! # Configuration Module
//!
//! Handles parsing of YAML/JSON configuration files and command-line arguments.
//!
//! ## Key Structs
//! - `AppConfig`: The top-level configuration object.
//! - `CliArgs`: The struct derived from Clap for CLI parsing.
//!
//! This module also implements strict validation to prevent invalid runtime states (e.g. 0 threads).

use serde::Deserialize;
use std::fs::File;
use std::io::Read;
use clap::Parser;
use std::error::Error;

#[derive(Debug, Deserialize, Clone)]
/// Main application configuration.
pub struct AppConfig {
    /// Database connection details
    pub database: DatabaseConfig,
    /// Export behavior settings
    pub export: ExportConfig,
    /// BigQuery settings (Optional, mostly for schema gen)
    pub bigquery: Option<BigQueryConfig>,
}

#[derive(Debug, Deserialize, Clone)]
/// Database Connection Parameters
pub struct DatabaseConfig {
    pub username: String,
    pub password: Option<String>,
    pub host: String,
    pub port: u16,
    pub service: String,
}

#[derive(Debug, Deserialize, Clone)]
/// Export Process Configuration
pub struct ExportConfig {
    /// Directory where artifacts and data will be saved
    pub output_dir: String,
    /// Schema to export (if not specified, defaults to username)
    pub schema: Option<String>,
    /// Specific table to export (optional)
    pub table: Option<String>,
    /// specific number of threads (overrides dynamic calculation)
    pub parallel: Option<usize>,
    /// Rows to fetch per round-trip to database
    pub prefetch_rows: Option<u32>,
    /// Tables to exclude from export
    pub exclude_tables: Option<Vec<String>>,
    /// Enable ROW_HASH calculation for parity/validation
    pub enable_row_hash: Option<bool>,
    /// Target CPU usage percent for dynamic threading (default 50)
    pub cpu_percent: Option<u8>,
    /// Field delimiter for CSV (default Ctrl+P)
    pub field_delimiter: Option<String>,
    /// List of schemas to export (optional)
    pub schemas: Option<Vec<String>>,
    /// Path to a file containing a list of schemas (optional)
    pub schemas_file: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
/// BigQuery Configuration (Project/Dataset)
pub struct BigQueryConfig {
    pub project: String,
    pub dataset: String,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
/// Command Line Arguments
pub struct CliArgs {
    /// Path to configuration file (YAML or JSON)
    #[arg(short, long)]
    pub config: Option<String>,
    
    // Overrides for ad-hoc runs
    #[arg(long)]
    pub username: Option<String>,
    #[arg(long)]
    pub password: Option<String>,
    #[arg(long)]
    pub host: Option<String>,
    #[arg(long)]
    pub service: Option<String>,
    #[arg(long)]
    pub port: Option<u16>,
    #[arg(long)]
    pub table: Option<String>,
    #[arg(short, long)]
    pub output: Option<String>,
    #[arg(long)]
    pub query_where: Option<String>,
}

impl AppConfig {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn Error>> {
        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        
        let config: AppConfig = if path.ends_with(".json") {
            serde_json::from_str(&contents)?
        } else {
            serde_yaml::from_str(&contents)?
        };
        
        Ok(config)
    }

    pub fn merge_cli(&mut self, args: &CliArgs) {
        if let Some(u) = &args.username { self.database.username = u.clone(); }
        if let Some(p) = &args.password { self.database.password = Some(p.clone()); }
        if let Some(h) = &args.host { self.database.host = h.clone(); }
        if let Some(p) = args.port { self.database.port = p; }
        if let Some(s) = &args.service { self.database.service = s.clone(); }
        if let Some(t) = &args.table { self.export.table = Some(t.clone()); }
        if let Some(o) = &args.output { self.export.output_dir = o.clone(); }
    }

    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        if let Some(p) = self.export.parallel {
            if p == 0 {
                return Err("export.parallel must be > 0".into());
            }
        }
        if let Some(p) = self.export.prefetch_rows {
            if p == 0 {
                return Err("export.prefetch_rows must be > 0".into());
            }
        }
        if let Some(cpu) = self.export.cpu_percent {
            if cpu == 0 || cpu > 100 {
                return Err("export.cpu_percent must be between 1 and 100".into());
            }
        }
        if let Some(d) = &self.export.field_delimiter {
            if d.len() != 1 || !d.is_ascii() {
                 return Err("export.field_delimiter must be a single ASCII character".into());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_load_yaml_config() {
        let yaml = r#"
database:
  username: "test_user"
  password: "test_password"
  host: "localhost"
  port: 1521
  service: "ORCL"
export:
  output_dir: "./output"
  table: "MY_TABLE"
  enable_row_hash: true
"#;
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write!(file, "{}", yaml).unwrap();
        let path = file.path().to_str().unwrap();

        let config = AppConfig::from_file(path).expect("Failed to parse config");
        
        assert_eq!(config.database.username, "test_user");
        assert_eq!(config.database.port, 1521);
        assert_eq!(config.export.table.as_deref(), Some("MY_TABLE"));
        assert_eq!(config.export.enable_row_hash, Some(true));
    }
}
