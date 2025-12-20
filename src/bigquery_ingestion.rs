use log::{info, warn, error};
use std::process::Command;
use std::path::Path;
use std::fs::File;
use crate::export_coordinator::TaskResult;
use crate::config::ExportConfig;

/// Orchestrates BigQuery ingestion using the generated schema and load scripts.
pub fn run_load_scripts(config: &ExportConfig, results: &[TaskResult]) -> Result<(), Box<dyn std::error::Error>> {
    info!("Starting optional BigQuery load process...");
    
    // Group results by table to avoid re-running for chunks
    let mut tables_processed = std::collections::HashSet::new();
    
    for res in results {
        if res.status != "SUCCESS" { continue; }
        if tables_processed.contains(&res.table) { continue; }
        
        let schema_dir = Path::new(&config.output_dir)
            .join(&res.schema)
            .join(&res.table);
        
        let config_dir = schema_dir.join("config");
        let ddl_path = config_dir.join("bigquery.ddl");
        let load_path = config_dir.join("load_command.sh");
        let val_path = config_dir.join("validation.sql");

        if !ddl_path.exists() || !load_path.exists() {
            warn!("Load artifacts missing for table {}. Skipping BQ load.", res.table);
            continue;
        }

        info!("--- Ingesting table {} into BigQuery ---", res.table);

        // 1. Create/Update Table (DDL)
        info!("Executing DDL: {:?}", ddl_path);
        let ddl_status = Command::new("bq")
            .arg("query")
            .arg("--use_legacy_sql=false")
            .stdin(File::open(&ddl_path)?)
            .status();
        
        match ddl_status {
            Ok(s) if s.success() => {
                // 2. Load Data
                info!("Executing Load Script: {:?}", load_path);
                let load_status = Command::new("bash")
                    .current_dir(&config_dir)
                    .arg("load_command.sh")
                    .status();
                
                if let Ok(ls) = load_status {
                    if ls.success() {
                        info!("Successfully loaded {} into BigQuery.", res.table);
                        
                        // 3. Optional: Run Validation
                        if val_path.exists() {
                            info!("Running Validation: {:?}", val_path);
                            let _ = Command::new("bq")
                                .arg("query")
                                .arg("--use_legacy_sql=false")
                                .stdin(File::open(&val_path)?)
                                .status();
                        }
                    } else {
                        error!("Load failed for table {}", res.table);
                    }
                }
            }
            _ => error!("DDL Execution failed for table {}", res.table),
        }

        tables_processed.insert(res.table.clone());
    }
    
    info!("BigQuery load process completed.");
    Ok(())
}
