# Oracle Rust Exporter - Windows Build Script
# Usage: .\build_windows.ps1

$ErrorActionPreference = "Stop"
$DistDir = "dist_windows"
$ArtifactName = "oracle_exporter_win.zip"

Write-Host "=== Oracle Rust Exporter: Windows Builder ===" -ForegroundColor Cyan

# 1. Check for Instant Client
$ZipFile = Get-ChildItem "instantclient-basiclite-windows.x64-*.zip" | Select-Object -First 1

if ($null -eq $ZipFile) {
    Write-Host "❌ Error: Oracle Instant Client ZIP not found!" -ForegroundColor Red
    Write-Host "Please download 'instantclient-basiclite-windows.x64-19.x.zip' from Oracle."
    Exit 1
}

Write-Host "✅ Found Oracle Client: $($ZipFile.Name)"

# 2. Cleanup
if (Test-Path $DistDir) { Remove-Item -Recurse -Force $DistDir }
New-Item -ItemType Directory -Force -Path "$DistDir\lib" | Out-Null

# 3. Extract Libraries
Write-Host "-> Extracting Oracle Libraries..."
Expand-Archive -Path $ZipFile.FullName -DestinationPath "$DistDir\temp_extract"
# Move DLLs from the nested folder to dist/lib
$InnerFolder = Get-ChildItem "$DistDir\temp_extract" | Select-Object -First 1
Move-Item "$($InnerFolder.FullName)\*.dll" "$DistDir\lib"
Remove-Item -Recurse -Force "$DistDir\temp_extract"

# 4. Build Binary
Write-Host "-> Compiling Rust Binary..."
cargo build --release

if (-not $?) {
    Write-Host "❌ Compilation Failed!" -ForegroundColor Red
    Exit 1
}

# 5. Assemble
Write-Host "-> Assembling Bundle..."
Copy-Item "target\release\oracle_rust_exporter.exe" "$DistDir\"
Copy-Item "config.yaml" "$DistDir\config_template.yaml"

# Create run.bat
$BatContent = @"
@echo off
setlocal
set APP_ROOT=%~dp0
set PATH=%APP_ROOT%lib;%PATH%
"%APP_ROOT%oracle_rust_exporter.exe" %*
"@
Set-Content -Path "$DistDir\run.bat" -Value $BatContent

# 6. Zip
Write-Host "-> Creating ZIP: $ArtifactName"
Compress-Archive -Path "$DistDir\*" -DestinationPath $ArtifactName -Force

Write-Host "=== SUCCESS ===" -ForegroundColor Green
Write-Host "Artifact: $ArtifactName"
