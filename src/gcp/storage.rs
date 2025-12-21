use std::process::Command;
use std::error::Error;
use log::info;
use std::io::Write;
use tempfile::NamedTempFile;

/// Uploads content to GCS using `gcloud storage cp` or `gsutil cp`
pub fn upload_file_cli(gcs_path: &str, content: String) -> Result<(), Box<dyn Error>> {
    info!("Uploading DDL to {}", gcs_path);
    
    let mut temp = NamedTempFile::new()?;
    write!(temp, "{}", content)?;
    let temp_path = temp.path().to_str().unwrap();
    
    // Try 'gcloud storage' first, simpler if available
    let status = Command::new("gcloud")
        .args(&["storage", "cp", temp_path, gcs_path])
        .status();

    match status {
        Ok(s) if s.success() => Ok(()),
        _ => {
            // Fallback to gsutil if gcloud storage fails or isn't installed? 
            // Actually 'gcloud storage' is standard now. Let's error if it fails.
            Err(format!("Failed to upload to {}", gcs_path).into())
        }
    }
}

/// Downloads content from GCS using `gcloud storage cat`
pub fn download_file_cli(gcs_path: &str) -> Result<String, Box<dyn Error>> {
    info!("Downloading translated DDL from {}", gcs_path);
    
    let output = Command::new("gcloud")
        .args(&["storage", "cat", gcs_path])
        .output()?;
        
    if !output.status.success() {
         let stderr = String::from_utf8_lossy(&output.stderr);
         return Err(format!("Download failed from {}: {}", gcs_path, stderr).into());
    }
    
    Ok(String::from_utf8(output.stdout)?)
}

/// Lists files in a GCS path using `gcloud storage ls`
pub fn list_files_cli(gcs_path: &str) -> Result<Vec<String>, Box<dyn Error>> {
    let output = Command::new("gcloud")
        .args(&["storage", "ls", gcs_path])
        .output()?;
        
    if !output.status.success() {
         let stderr = String::from_utf8_lossy(&output.stderr);
         return Err(format!("Listing failed for {}: {}", gcs_path, stderr).into());
    }
    
    let stdout = String::from_utf8(output.stdout)?;
    let files: Vec<String> = stdout.lines()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
        
    Ok(files)
}
