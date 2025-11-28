#!/bin/bash
set -e

TIMESTAMP=$1

echo "Starting SpotLake Data Collection for timestamp: $TIMESTAMP"

# Run collection scripts in parallel
echo "Starting SPS Collection..."
python3 collector/spot-dataset/aws/batch/sps/collect_sps.py --timestamp "$TIMESTAMP" &
PID_SPS=$!

echo "Starting IF Collection..."
python3 collector/spot-dataset/aws/batch/if/collect_if.py --timestamp "$TIMESTAMP" &
PID_IF=$!

echo "Starting Price Collection..."
python3 collector/spot-dataset/aws/batch/price/collect_price.py --timestamp "$TIMESTAMP" &
PID_PRICE=$!

# Wait for all background processes to finish
wait $PID_SPS
STATUS_SPS=$?
wait $PID_IF
STATUS_IF=$?
wait $PID_PRICE
STATUS_PRICE=$?

# Check exit statuses
if [ $STATUS_SPS -eq 0 ] && [ $STATUS_IF -eq 0 ] && [ $STATUS_PRICE -eq 0 ]; then
    echo "All collection jobs completed successfully."
    
    # Read SPS key from file
    if [ -f /tmp/sps_key.txt ]; then
        SPS_KEY=$(cat /tmp/sps_key.txt)
        echo "Found SPS Key: $SPS_KEY"
        
        echo "Starting Merge Job..."
        python3 collector/spot-dataset/aws/batch/merge/merge_data.py --sps_key "$SPS_KEY"
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
