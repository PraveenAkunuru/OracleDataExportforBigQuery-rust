pub mod storage;
pub mod translator;

use std::process::Command;
use std::error::Error;
use log::{info, error};

/// Checks if gcloud CLI is available
pub fn check_gcloud_availability() -> bool {
    Command::new("gcloud").arg("--version").output().map(|o| o.status.success()).unwrap_or(false)
}
