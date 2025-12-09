#!/bin/bash
set -e

TIMESTAMP=$1

# Capture start time
START_TIME_READABLE=$(date "+%Y-%m-%d %H:%M:%S")
START_TIME_EPOCH=$(date +%s)

# Define local output files
EXECUTION_FILE="/tmp/execution_time_${TIMESTAMP// /_}.txt"
MEMORY_FILE="/tmp/memory_usage_${TIMESTAMP// /_}.csv"

# Function to monitor memory
monitor_memory() {
    local sps_pid=$1
    local if_pid=$2
    local price_pid=$3
    local out_file=$4
    
    echo "Timestamp,Service,PID,Memory_MB" > "$out_file"
    
    while true; do
        current_ts=$(date "+%Y-%m-%d %H:%M:%S")
        
        if ps -p $sps_pid > /dev/null 2>&1; then
            mem=$(ps -o rss= -p $sps_pid | awk '{print $1/1024}')
            echo "$current_ts,SPS,$sps_pid,$mem" >> "$out_file"
        fi
        
        if ps -p $if_pid > /dev/null 2>&1; then
            mem=$(ps -o rss= -p $if_pid | awk '{print $1/1024}')
            echo "$current_ts,IF,$if_pid,$mem" >> "$out_file"
        fi
        
        if ps -p $price_pid > /dev/null 2>&1; then
            mem=$(ps -o rss= -p $price_pid | awk '{print $1/1024}')
            echo "$current_ts,Price,$price_pid,$mem" >> "$out_file"
        fi
        
        sleep 10
    done
}

echo "Starting SpotLake Data Collection (TEST) for timestamp: $TIMESTAMP"

# Run collection scripts in parallel
echo "Starting SPS Collection..."
python3 collector/spot-dataset/aws/batch-test/sps/collect_sps.py --timestamp "$TIMESTAMP" &
PID_SPS=$!

echo "Starting IF Collection..."
python3 collector/spot-dataset/aws/batch-test/if/collect_if.py --timestamp "$TIMESTAMP" &
PID_IF=$!

echo "Starting Price Collection..."
python3 collector/spot-dataset/aws/batch-test/price/collect_price.py --timestamp "$TIMESTAMP" &
PID_PRICE=$!

# Start memory monitoring in background
monitor_memory $PID_SPS $PID_IF $PID_PRICE "$MEMORY_FILE" &
MONITOR_PID=$!

# Wait for all background processes to finish
wait $PID_SPS
STATUS_SPS=$?
wait $PID_IF
STATUS_IF=$?
wait $PID_PRICE
STATUS_PRICE=$?

# Stop memory monitoring
kill $MONITOR_PID 2>/dev/null || true

COLLECTION_END_TIME_EPOCH=$(date +%s)
COLLECTION_DURATION=$((COLLECTION_END_TIME_EPOCH - START_TIME_EPOCH))

# Check exit statuses
if [ $STATUS_SPS -eq 0 ] && [ $STATUS_IF -eq 0 ] && [ $STATUS_PRICE -eq 0 ]; then
    echo "All collection jobs completed successfully."
    
    # Read SPS key from file
    if [ -f /tmp/sps_key.txt ]; then
        SPS_KEY=$(cat /tmp/sps_key.txt)
        echo "Found SPS Key: $SPS_KEY"
        
        echo "Starting Merge Job..."
        python3 collector/spot-dataset/aws/batch-test/merge/merge_data.py --sps_key "$SPS_KEY"

        MERGE_END_TIME_EPOCH=$(date +%s)
        MERGE_DURATION=$((MERGE_END_TIME_EPOCH - COLLECTION_END_TIME_EPOCH))
        
        # Write execution stats
        echo "Start Time: $START_TIME_READABLE" > "$EXECUTION_FILE"
        echo "Collection Duration (sec): $COLLECTION_DURATION" >> "$EXECUTION_FILE"
        echo "Merge & Upload Duration (sec): $MERGE_DURATION" >> "$EXECUTION_FILE"
        
        # Upload to S3
        echo "Uploading stats to S3..."
        aws s3 cp "$EXECUTION_FILE" "s3://spotlake-test/rawdata/aws/localfile/"
        aws s3 cp "$MEMORY_FILE" "s3://spotlake-test/rawdata/aws/localfile/"
    else
        echo "Error: /tmp/sps_key.txt not found. SPS collection might have failed to write the key."
        exit 1
    fi
else
    echo "One or more collection jobs failed."
    echo "SPS Status: $STATUS_SPS"
    echo "IF Status: $STATUS_IF"
    echo "Price Status: $STATUS_PRICE"
    exit 1
fi
