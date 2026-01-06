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

# measure_resources.sh
# Usage: ./measure_resources.sh <command_to_run>

LOG_FILE="resource_usage.log"
echo "Timestamp,CPU_Percent,RSS_KB" > $LOG_FILE

# Start the command in background
export LD_LIBRARY_PATH="$(pwd)/lib:$(pwd)/lib/instantclient_19_10:$LD_LIBRARY_PATH"
"$@" &
PID=$!

peak_cpu=0
peak_rss=0

echo "Monitoring PID $PID..."

while kill -0 $PID 2>/dev/null; do
    # Get %cpu (total across all threads) and rss
    # usage: ps -p PID -o %cpu,rss --no-headers
    stats=$(ps -p $PID -o %cpu,rss --no-headers 2>/dev/null)
    
    if [ ! -z "$stats" ]; then
        cpu=$(echo $stats | awk '{print $1}')
        rss=$(echo $stats | awk '{print $2}')
        
        # Log it
        echo "$(date +%s),$cpu,$rss" >> $LOG_FILE
        
        # Update peaks (using bc for floating point comparison if needed, or simple string compare for int rss)
        # Bash float comparison is tricky, using python/perl or just awk is easier.
        peak_cpu=$(echo "$cpu $peak_cpu" | awk '{if ($1 > $2) print $1; else print $2}')
        peak_rss=$(echo "$rss $peak_rss" | awk '{if ($1 > $2) print $1; else print $2}')
    fi
    sleep 0.5
done

wait $PID
EXIT_CODE=$?

echo "------------------------------------------------"
echo "Command Finished with Exit Code: $EXIT_CODE"
echo "Peak CPU Usage: ${peak_cpu}%"
# Convert KB to MB
peak_rss_mb=$(echo "$peak_rss / 1024" | bc)
echo "Peak Memory (RSS): ${peak_rss_mb} MB"
echo "------------------------------------------------"
