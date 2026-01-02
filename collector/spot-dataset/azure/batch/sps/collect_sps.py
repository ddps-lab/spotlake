import sys
import os
import argparse
import pandas as pd
import boto3
import yaml
from datetime import datetime, timezone

# Add parent directory to path to import utils and modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import load_sps
from utils.common import S3, Logger
from utils.constants import AZURE_CONST
from utils.slack_msg_sender import send_slack_message

SPS_METADATA_S3_KEY = f"{AZURE_CONST.S3_RAW_DATA_PATH}/localfile/sps_metadata.yaml"
DESIRED_COUNTS = [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50]
BUCKET_NAME = "spotlake"

def read_metadata():
    try:
        data = S3.read_file(SPS_METADATA_S3_KEY, 'yaml')
        if data:
            Logger.info(f"Read metadata from S3: {SPS_METADATA_S3_KEY}")
            return data
    except Exception as e:
        Logger.info(f"Failed to read metadata from S3: {e}")
    return None

def write_metadata(metadata):
    try:
        S3.upload_file(metadata, SPS_METADATA_S3_KEY, 'yaml')
        Logger.info(f"Saved metadata to S3: {SPS_METADATA_S3_KEY}")
    except Exception as e:
        Logger.error(f"Failed to save metadata to S3: {e}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--timestamp', dest='timestamp', action='store')
    args = parser.parse_args()

    if args.timestamp:
        if args.timestamp.endswith('Z'):
            timestamp_utc = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M:%SZ")
        else:
            timestamp_utc = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M")
    else:
        timestamp_utc = datetime.now(timezone.utc)
        timestamp_utc = timestamp_utc.replace(minute=((timestamp_utc.minute // 10) * 10), second=0, microsecond=0)

    print(f"Script execution start time (UTC): {timestamp_utc}")
    current_date = timestamp_utc.strftime("%Y-%m-%d")

    try:
        metadata = read_metadata()
        sps_df = None
        current_desired_count = 1

        if metadata:
            desired_count_index = metadata.get("desired_count_index", 0)
            current_desired_count = DESIRED_COUNTS[desired_count_index]
            
            workload_date = metadata.get("workload_date")
            is_first_time_optimization = False
            
            if workload_date != current_date:
                Logger.info(f"Workload date changed: {workload_date} -> {current_date}. Application First Time Optimization.")
                is_first_time_optimization = True
                metadata["workload_date"] = current_date
                
                # Check if we should force desired count to 1 for optimization
                # Legacy code says: "Force Desired Count to 1 for First Time Optimization execution"
                current_execution_desired_count = 1
            else:
                current_execution_desired_count = current_desired_count
            
            next_index = (desired_count_index + 1) % len(DESIRED_COUNTS)
            metadata["desired_count_index"] = next_index
            
            write_metadata(metadata)
            
            if is_first_time_optimization:
                Logger.info(f"Executing First Time Optimization with Count: {current_execution_desired_count}")
                sps_df = load_sps.collect_spot_placement_score_first_time(desired_counts=[current_execution_desired_count])
            else:
                Logger.info(f"Executing Regular Collection. Desired Count: {current_execution_desired_count}")
                sps_df = load_sps.collect_spot_placement_score(desired_counts=[current_execution_desired_count])

        else:
            Logger.info("Metadata missing. Starting fresh.")
            # Default behavior
            initial_metadata = {
                "desired_count_index": 1, 
                "workload_date": current_date
            }
            write_metadata(initial_metadata)
            
            Logger.info("Executing First Time Optimization (Fresh Start)")
            sps_df = load_sps.collect_spot_placement_score_first_time(desired_counts=[1])
            current_desired_count = 1

        if sps_df is None or sps_df.empty:
            print("No SPS data collected.")
            return

        # Save Raw Data to S3
        time_str = timestamp_utc.strftime("%H-%M")
        date_path = timestamp_utc.strftime("%Y/%m/%d")
        
        # Add timestamp column if missing
        current_time_str = timestamp_utc.strftime("%Y-%m-%d %H:%M:%S")
        
        # Validate: check if 'time' column already exists (shouldn't happen)
        if 'time' in sps_df.columns:
            existing_times = sps_df['time'].unique()
            Logger.warning(f"SPS DataFrame already has 'time' column with {len(existing_times)} unique values")
            Logger.warning(f"First 5 values: {list(existing_times[:5])}")
        
        sps_df['time'] = current_time_str
        
        # Validate: ensure single timestamp before saving
        unique_times = sps_df['time'].unique()
        if len(unique_times) > 1:
            Logger.error(f"ERROR: Multiple timestamps detected in SPS data: {unique_times}")
            raise ValueError(f"SPS data contains {len(unique_times)} different timestamps!")
        
        Logger.info(f"SPS data ready: {len(sps_df)} rows, timestamp: {current_time_str}")
        
        s3_key = f"{AZURE_CONST.S3_RAW_DATA_PATH}/sps/{date_path}/{time_str}_sps_{current_desired_count}.pkl.gz"
        
        # Validate S3 key matches current date/time
        if date_path not in s3_key or time_str not in s3_key:
            raise ValueError(f"S3 key mismatch! Expected {date_path}/{time_str}, got {s3_key}")
        
        Logger.info(f"Saving to S3: {s3_key}")
        
        local_path = f"/tmp/sps_data.pkl.gz"
        sps_df.to_pickle(local_path, compression='gzip')
        
        s3_client = boto3.client('s3')
        with open(local_path, 'rb') as f:
            s3_client.upload_fileobj(f, BUCKET_NAME, s3_key)
            
        print(f"Uploaded SPS data to S3: {s3_key}")
        
        # Write S3 Key to /tmp/sps_key.txt for downstream processing
        with open("/tmp/sps_key.txt", "w") as f:
            f.write(s3_key)
            
        os.remove(local_path)

    except Exception as e:
        send_slack_message(f"Error in collect_sps.py: {e}")
        raise e

if __name__ == "__main__":
    main()
