use serde::Deserialize;
use std::fs::File;
use std::io::Read;
use clap::Parser;
use std::error::Error;

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub database: DatabaseConfig,
    pub export: ExportConfig,
    pub bigquery: Option<BigQueryConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseConfig {
    pub username: String,
    pub password: Option<String>,
    pub host: String,
    pub port: u16,
    pub service: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ExportConfig {
    pub output_dir: String,
    pub schema: Option<String>,
    pub table: Option<String>,
    pub parallel: Option<usize>,
    pub prefetch_rows: Option<u32>,
    pub exclude_tables: Option<Vec<String>>,
    pub enable_row_hash: Option<bool>,
    pub cpu_percent: Option<u8>,
    pub field_delimiter: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct BigQueryConfig {
    pub project: String,
    pub dataset: String,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
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
        if let Some(s) = &args.service { self.database.service = s.clone(); }
        if let Some(t) = &args.table { self.export.table = Some(t.clone()); }
        if let Some(o) = &args.output { self.export.output_dir = o.clone(); }
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
