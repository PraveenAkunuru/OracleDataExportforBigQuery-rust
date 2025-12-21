use std::process::Command;
use std::error::Error;
use log::info;
use serde_json::Value;

/// Gets a GCP Access Token using `gcloud auth print-access-token`
fn get_access_token() -> Result<String, Box<dyn Error>> {
    let output = Command::new("gcloud")
        .args(["auth", "print-access-token"])
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("Failed to get access token: {}", stderr).into());
    }

    let token = String::from_utf8(output.stdout)?.trim().to_string();
    Ok(token)
}

/// Translates DDL using the BigQuery Migration REST API v2 (via curl)
pub fn translate_ddl_cli(
    project_id: &str, 
    location: &str, 
    gcs_input_path: &str, 
    gcs_output_path: &str
) -> Result<String, Box<dyn Error>> {
    info!("Translating DDL from {} to {} (REST API)", gcs_input_path, gcs_output_path);

    let token = get_access_token()?;
    let display_name = format!("oracle_export_translation_{}", chrono::Utc::now().timestamp());
    
    // Construct JSON Payload for REST API
    // Task Type 'Translation_Oracle2BQ' found in docs for V2
    // Must also specify dialects explicitly even if task type implies it.
    let payload = serde_json::json!({
        "displayName": display_name,
        "tasks": {
            "translation_task": {
                "type": "Translation_Oracle2BQ",
                "translationConfigDetails": {
                    "sourceDialect": { "oracleDialect": {} },
                    "targetDialect": { "bigqueryDialect": {} },
                    "gcsSourcePath": gcs_input_path,
                    "gcsTargetPath": gcs_output_path
                }
            }
        }
    });

    let payload_str = payload.to_string();
    
    // Endpoint: projects.locations.workflows.create
    let url = format!(
        "https://bigquerymigration.googleapis.com/v2/projects/{}/locations/{}/workflows",
        project_id, location
    );
    
    info!("Submitting Workflow to {}", url);
    
    // Use curl to POST
    let output = Command::new("curl")
        .args([
            "-X", "POST",
            "-H", &format!("Authorization: Bearer {}", token),
            "-H", "Content-Type: application/json",
            "-d", &payload_str,
            &url
        ])
        .output()?;
        
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("curl failed: {}", stderr).into());
    }
    
    let stdout = String::from_utf8(output.stdout)?;
    
    // Parse response JSON to find "name" (Workflow ID/Resource)
    let response: Value = serde_json::from_str(&stdout)?;
    
    if let Some(error) = response.get("error") {
        return Err(format!("API Error: {}", error).into());
    }
    
    let workflow_name = response["name"].as_str()
        .ok_or("API response missing 'name' field")?;
        
    info!("Workflow created: {}", workflow_name);
    // workflow_name is like "projects/123/locations/us/workflows/456"

    // Polling Loop
    loop {
        // GET <workflow_name>
        let poll_url = format!("https://bigquerymigration.googleapis.com/v2/{}", workflow_name);
        
        // Refresh token if needed? (Usually valid for 1 hour, enough for typical DDL translation)
        // If long running, might need refresh logic. For now, simplistic.
        
        let poll_out = Command::new("curl")
            .args([
                "-H", &format!("Authorization: Bearer {}", token),
                &poll_url
            ])
            .output()?;
            
        let poll_json: Value = serde_json::from_str(&String::from_utf8(poll_out.stdout)?)?;
        let state = poll_json["state"].as_str().unwrap_or("UNKNOWN");
        
        if state == "COMPLETED" {
            info!("Translation COMPLETED.");
            break;
        } else if state == "FAILED" { // Check for other terminal states
             let error_msg = poll_json["error"].to_string();
             return Err(format!("Translation FAILED: {}", error_msg).into());
        }
        
        info!("Workflow State: {}. Waiting 5s...", state);
        std::thread::sleep(std::time::Duration::from_secs(5));
    }

    Ok(gcs_output_path.to_string())
}
