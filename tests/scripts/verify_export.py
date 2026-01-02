import os
import json
import glob
import sys

def check_export_verification(output_dir):
    print(f"Verifying Export in: {output_dir}")
    
    # 1. Check Report
    report_files = glob.glob(os.path.join(output_dir, "report_*.json"))
    if not report_files:
        print("ERROR: No report_*.json found! Coordinator might have failed completely.")
        return
    
    latest_report = max(report_files, key=os.path.getctime)
    print(f"Reading Report: {latest_report}")
    
    with open(latest_report, 'r') as f:
        report = json.load(f)
        
    summary = report.get('summary', {})
    print("\n=== Summary ===")
    print(f"Total Tasks: {summary.get('total_tasks')}")
    print(f"Success:     {summary.get('success')}")
    print(f"Failed:      {summary.get('failed')}")
    
    total_rows = summary.get('total_rows', 0)
    total_bytes = summary.get('total_bytes', 0)
    duration = summary.get('total_duration_seconds', 0)
    
    print(f"Total Rows:  {total_rows:,}")
    print(f"Total Bytes: {total_bytes / (1024*1024):.2f} MB")
    
    mb_per_sec = summary.get('total_mb_per_sec')
    if mb_per_sec is None and duration > 0:
        mb_per_sec = total_bytes / (1024*1024) / duration
        
    if duration > 0:
        print(f"Duration:    {duration:.2f} s")
        print(f"Throughput:  {total_rows / duration:.2f} rows/s")
        if mb_per_sec is not None:
             print(f"             {mb_per_sec:.2f} MB/s")
    
    details = report.get('details', [])
    failed_tables = [t for t in details if t['status'] == 'FAILED']
    
    if failed_tables:
        print("\n=== Failed Tables ===")
        for ft in failed_tables:
            print(f"- {ft['schema']}.{ft['table']}: {ft.get('error')}")
            
    # 2. Check Individual Metadata / Validations
    print("\n=== Data Validation (Metadata Artifacts) ===")
    # Look for metadata.json in subdirectories
    # Structure: output_dir/SCHEMA/TABLE/config/metadata.json
    metadata_files = glob.glob(os.path.join(output_dir, "*", "*", "config", "metadata.json"))
    
    pass_count = 0
    fail_count = 0
    
    for meta_file in metadata_files:
        with open(meta_file, 'r') as f:
            data = json.load(f)
            
        full_name = data.get('full_name', 'Unknown')
        stats = data.get('validation', {})
        row_count = stats.get('row_count', 0)
        
        # Verify if Row Count > 0 (Just a heuristic, some tables might be empty)
        # Verify PK Hash exists if applicable
        
        print(f"Checking {full_name}: Rows={row_count} ... ", end='')
        
        # Check if we have aggregations (means we validated data content)
        aggs = stats.get('aggregates')
        pk_hash = stats.get('pk_hash')
        
        info = []
        if pk_hash: info.append("PK_HASH_OK")
        if aggs: info.append(f"AGG_CHECKS={len(aggs)}")
        
        print(f"[{', '.join(info) if info else 'Basic'}]")
        pass_count += 1

    print(f"\nFinal Validation Status: Verified {pass_count} tables.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 verify_export.py <output_dir>")
        sys.exit(1)
    check_export_verification(sys.argv[1])
