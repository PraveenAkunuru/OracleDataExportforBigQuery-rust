//! Configuration management for the Oracle Data Exporter.
//!
//! Provides structures and logic to load configuration from YAML/JSON files,
//! override with command-line arguments, and validate all parameters.

use crate::domain::entities::FileFormat;
use clap::Parser;
use serde::Deserialize;
use std::error::Error;
use std::fs::File;
use std::io::Read;

/// Top-level application configuration.
#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    /// Database connection parameters.
    pub database: DatabaseConfig,
    /// Settings governing the export and extraction process.
    pub export: ExportConfig,
    /// Targeted BigQuery project and dataset settings.
    pub bigquery: Option<BigQueryConfig>,
    /// Google Cloud Platform context (used for translation or storage).
    pub gcp: Option<GcpConfig>,
}

/// GCP-specific credentials and configuration.
#[derive(Debug, Deserialize, Clone)]
pub struct GcpConfig {
    pub project_id: String,
    pub location: String,
    pub gcs_bucket: String,
    /// If true, enforces strict decimal precision in BQ translation
    pub decimal_precision_strictness: Option<bool>,
}

#[derive(Debug, Deserialize, Clone)]
/// Database Connection Parameters
pub struct DatabaseConfig {
    pub username: String,
    pub password: Option<String>,
    pub host: String,
    pub port: u16,
    pub service: String,
    /// Optional full connection string (TNS or Easy Connect) that overrides host/port/service
    pub connection_string: Option<String>,
}

impl DatabaseConfig {
    pub fn get_connection_string(&self) -> String {
        if let Some(cs) = &self.connection_string {
            cs.clone()
        } else {
            format!("//{}:{}/{}", self.host, self.port, self.service)
        }
    }
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
    /// If true, enables ROW_HASH generation
    pub enable_row_hash: Option<bool>,
    /// If true, computes ROW_HASH in Rust client instead of Oracle
    pub use_client_hash: Option<bool>,
    /// Target CPU usage percent for dynamic threading (default 50)
    pub cpu_percent: Option<u8>,
    /// Field delimiter for CSV (default Ctrl+P)
    pub field_delimiter: Option<String>,
    /// List of schemas to export (optional)
    pub schemas: Option<Vec<String>>,
    /// Path to a file containing a list of schemas (optional)
    pub schemas_file: Option<String>,
    /// List of specific tables to export (optional)
    pub tables: Option<Vec<String>>,
    /// Path to a file containing a list of tables (optional)
    pub tables_file: Option<String>,
    /// Automatically load data to BigQuery after export (explicit option)
    pub load_to_bq: Option<bool>,
    /// Enable adaptive parallelism (Elastic Worker Pool)
    pub adaptive_parallelism: Option<bool>,
    /// Target MB/s per core for adaptive logic (default 12.0)
    pub target_throughput_per_core: Option<f64>,
    /// Target file format (default CSV)
    pub file_format: Option<FileFormat>,
    /// Compression for Parquet (default ZSTD)
    pub parquet_compression: Option<String>,
    /// Number of rows to buffer before writing a Parquet/Arrow batch (default 10000)
    pub parquet_batch_size: Option<usize>,
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
    /// Automatically load data into BigQuery (explicit option)
    #[arg(long)]
    pub load: bool,

    /// Override parallelism (number of threads)
    #[arg(long)]
    pub parallel: Option<usize>,

    /// Override target CPU usage percent (1-100)
    #[arg(long)]
    pub cpu_percent: Option<u8>,
    /// Target file format (csv, parquet)
    #[arg(long)]
    pub format: Option<String>,
    /// Parquet compression (zstd, snappy, gzip, lzo, brotli, lz4, none)
    #[arg(long)]
    pub compression: Option<String>,
    /// Parquet batch size (rows per write)
    #[arg(long)]
    pub parquet_batch_size: Option<usize>,
    /// Path to a file containing a list of schemas to export
    #[arg(long)]
    pub schemas_file: Option<String>,
    /// Path to a file containing a list of tables to export
    #[arg(long)]
    pub tables_file: Option<String>,
    /// Comma-separated list of tables to exclude
    #[arg(long, value_delimiter = ',')]
    pub exclude_tables: Option<Vec<String>>,
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
        if let Some(u) = &args.username {
            self.database.username = u.clone();
        }
        if let Some(p) = &args.password {
            self.database.password = Some(p.clone());
        }
        if let Some(h) = &args.host {
            self.database.host = h.clone();
        }
        if let Some(p) = args.port {
            self.database.port = p;
        }
        if let Some(s) = &args.service {
            self.database.service = s.clone();
        }
        if let Some(t) = &args.table {
            self.export.table = Some(t.clone());
        }
        if let Some(o) = &args.output {
            self.export.output_dir = o.clone();
        }
        if args.load {
            self.export.load_to_bq = Some(true);
        }
        if let Some(p) = args.parallel {
            self.export.parallel = Some(p);
        }
        if let Some(c) = args.cpu_percent {
            self.export.cpu_percent = Some(c);
        }
        if let Some(f) = &args.format {
            let format = match f.to_lowercase().as_str() {
                "csv" => FileFormat::Csv,
                "parquet" => FileFormat::Parquet,
                _ => FileFormat::Csv, // Default or warn?
            };
            self.export.file_format = Some(format);
        }
        if let Some(c) = &args.compression {
            self.export.parquet_compression = Some(c.clone());
        }
        if let Some(b) = args.parquet_batch_size {
            self.export.parquet_batch_size = Some(b);
        }
        if let Some(sf) = &args.schemas_file {
            self.export.schemas_file = Some(sf.clone());
        }
        if let Some(tf) = &args.tables_file {
            self.export.tables_file = Some(tf.clone());
        }
        if let Some(e) = &args.exclude_tables {
            self.export.exclude_tables = Some(e.clone());
        }
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

    pub fn get_schemas(&self) -> Vec<String> {
        let mut schemas = std::collections::HashSet::new();
        if let Some(s) = &self.export.schema {
            schemas.insert(s.to_uppercase());
        }
        if let Some(list) = &self.export.schemas {
            for s in list {
                schemas.insert(s.to_uppercase());
            }
        }
        if let Some(path) = &self.export.schemas_file {
            if let Ok(content) = std::fs::read_to_string(path) {
                for line in content.lines() {
                    let s = line.trim();
                    if !s.is_empty() {
                        schemas.insert(s.to_uppercase());
                    }
                }
            }
        }
        if schemas.is_empty() {
            schemas.insert(self.database.username.to_uppercase());
        }
        let mut v: Vec<String> = schemas.into_iter().collect();
        v.sort();
        v
    }

    pub fn get_target_tables(&self) -> Option<std::collections::HashSet<String>> {
        let mut tables = std::collections::HashSet::new();
        let mut any = false;

        if let Some(t) = &self.export.table {
            tables.insert(t.to_uppercase());
            any = true;
        }
        if let Some(list) = &self.export.tables {
            for t in list {
                tables.insert(t.to_uppercase());
                any = true;
            }
        }
        if let Some(path) = &self.export.tables_file {
            if let Ok(content) = std::fs::read_to_string(path) {
                for line in content.lines() {
                    let t = line.trim();
                    if !t.is_empty() {
                        tables.insert(t.to_uppercase());
                        any = true;
                    }
                }
            }
        }

        if any {
            Some(tables)
        } else {
            None
        }
    }

    pub fn default_from_cli(args: &CliArgs) -> Self {
        Self {
            database: DatabaseConfig {
                username: args.username.clone().unwrap_or_default(),
                password: args.password.clone(),
                host: args.host.clone().unwrap_or_default(),
                port: args.port.unwrap_or(1521),
                service: args.service.clone().unwrap_or_default(),
                connection_string: None,
            },
            export: ExportConfig {
                output_dir: args.output.clone().unwrap_or_else(|| ".".to_string()),
                schema: None,
                table: args.table.clone(),
                parallel: args.parallel,
                prefetch_rows: Some(5000),
                exclude_tables: None,
                enable_row_hash: None,
                cpu_percent: args.cpu_percent,
                field_delimiter: None,
                schemas: None,
                schemas_file: args.schemas_file.clone(),
                tables: None,
                tables_file: args.tables_file.clone(),
                load_to_bq: Some(args.load),
                use_client_hash: None,
                adaptive_parallelism: None,
                target_throughput_per_core: None,
                file_format: args
                    .format
                    .as_ref()
                    .map(|f| match f.to_lowercase().as_str() {
                        "csv" => FileFormat::Csv,
                        "parquet" => FileFormat::Parquet,
                        _ => FileFormat::Csv,
                    }),
                parquet_compression: args.compression.clone(),
                parquet_batch_size: args.parquet_batch_size,
            },
            bigquery: None,
            gcp: None,
        }
    }

    pub fn is_excluded(&self, table_name: &str) -> bool {
        if let Some(excluded) = &self.export.exclude_tables {
            let up = table_name.to_uppercase();
            return excluded.iter().any(|e| e.to_uppercase() == up);
        }
        false
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

    #[test]
    fn test_merge_cli_overrides() {
        let mut config = AppConfig {
            database: DatabaseConfig {
                username: "u".into(),
                password: None,
                host: "h".into(),
                port: 1521,
                service: "s".into(),
                connection_string: None,
            },
            export: ExportConfig {
                output_dir: ".".into(),
                schema: None,
                table: None,
                parallel: Some(4),
                prefetch_rows: None,
                exclude_tables: None,
                enable_row_hash: None,
                cpu_percent: Some(20),
                field_delimiter: None,
                schemas: None,
                schemas_file: None,
                tables: None,
                tables_file: None,
                load_to_bq: None,
                use_client_hash: None,
                adaptive_parallelism: None,
                target_throughput_per_core: None,
                file_format: None,
                parquet_compression: None,
                parquet_batch_size: None,
            },
            bigquery: None,
            gcp: None,
        };

        let args = CliArgs {
            config: None,
            username: None,
            password: None,
            host: None,
            service: None,
            port: None,
            table: None,
            output: None,
            query_where: None,
            load: true,
            parallel: Some(10),
            cpu_percent: Some(80),
            format: None,
            compression: None,
            parquet_batch_size: None,
            schemas_file: None,
            tables_file: None,
            exclude_tables: None,
        };

        config.merge_cli(&args);

        // Verify CLI overrides Config
        assert_eq!(config.export.parallel, Some(10));
        assert_eq!(config.export.cpu_percent, Some(80));
        assert_eq!(config.export.load_to_bq, Some(true));
    }
}
