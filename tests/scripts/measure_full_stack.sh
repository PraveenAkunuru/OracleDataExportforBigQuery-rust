#!/bin/bash
# Copyright 2026 Google LLC
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# measure_full_stack.sh
# Usage: ./measure_full_stack.sh <command_to_run>

LOG_FILE="resource_full.log"
CONTAINER_NAME="oracle23-free"

echo "Timestamp,App_CPU,App_RSS_MB,DB_CPU,DB_Mem_MB" > $LOG_FILE

# Start the command in background
export LD_LIBRARY_PATH="$(pwd)/lib:$(pwd)/lib/instantclient_19_10:$LD_LIBRARY_PATH"
"$@" &
PID=$!

peak_app_cpu=0
peak_app_rss=0
peak_db_cpu=0
peak_db_mem=0

echo "Monitoring App PID $PID and Container $CONTAINER_NAME..."

while kill -0 $PID 2>/dev/null; do
    # 1. App Stats
    # RSS in KB
    app_stats=$(ps -p $PID -o %cpu,rss --no-headers 2>/dev/null)
    
    # 2. Docker Stats
    # Format: "12.34% 1.23GiB" -> Need to parse
    # We use --no-stream to get a single snapshot
    db_stats=$(docker stats $CONTAINER_NAME --no-stream --format "{{.CPUPerc}} {{.MemUsage}}" 2>/dev/null)
    
    if [ ! -z "$app_stats" ]; then
        app_cpu=$(echo $app_stats | awk '{print $1}')
        app_rss_kb=$(echo $app_stats | awk '{print $2}')
        app_rss_mb=$(echo "$app_rss_kb / 1024" | bc)
        
        # Parse Docker Stats
        # DB CPU: "0.50%" -> Remove %
        db_cpu_raw=$(echo $db_stats | awk '{print $1}' | tr -d '%')
        # DB Mem: "2.34GiB" or "500MiB" -> Complex to parse strictly in bash, 
        # but let's assume GiB for now or use specific formatting if possible.
        # Actually, extracting raw bytes is harder with docker stats default.
        # Let's just store the raw string for DB Mem for now, or try to parse typical "GiB/MiB"
        db_mem_raw=$(echo $db_stats | awk '{print $2}')
        
        # Simple parser for DB Mem (assuming GiB or MiB)
        db_mem_mb=0
        if [[ "$db_mem_raw" == *"GiB"* ]]; then
             val=$(echo $db_mem_raw | tr -d 'GiB')
             db_mem_mb=$(echo "$val * 1024" | bc)
        elif [[ "$db_mem_raw" == *"MiB"* ]]; then
             val=$(echo $db_mem_raw | tr -d 'MiB')
             db_mem_mb=$(echo "$val" | bc)
        fi
        
        # Log it
        echo "$(date +%s),$app_cpu,$app_rss_mb,$db_cpu_raw,$db_mem_mb" >> $LOG_FILE
        
        # Update Peaks
        peak_app_cpu=$(echo "$app_cpu $peak_app_cpu" | awk '{if ($1 > $2) print $1; else print $2}')
        peak_app_rss=$(echo "$app_rss_mb $peak_app_rss" | awk '{if ($1 > $2) print $1; else print $2}')
        peak_db_cpu=$(echo "$db_cpu_raw $peak_db_cpu" | awk '{if ($1 > $2) print $1; else print $2}')
        peak_db_mem=$(echo "$db_mem_mb $peak_db_mem" | awk '{if ($1 > $2) print $1; else print $2}')
    fi
    sleep 1
done

wait $PID
EXIT_CODE=$?

echo "------------------------------------------------"
echo "Command Finished with Exit Code: $EXIT_CODE"
echo "APP [Host] Peak CPU: ${peak_app_cpu}%"
echo "APP [Host] Peak Mem: ${peak_app_rss} MB"
echo "DB  [Docker] Peak CPU: ${peak_db_cpu}%"
echo "DB  [Docker] Peak Mem: ${peak_db_mem} MB"
echo "------------------------------------------------"
