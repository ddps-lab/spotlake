# ------ import module ------
from datetime import datetime, timezone
import boto3
import os, pickle, gzip
import subprocess
import pandas as pd
import io
import argparse
import shutil

# ------ import user module ------
import sys
# sys.path.append("/home/ubuntu/spotlake")
# from const_config import AwsCollector, Storage
from utility.slack_msg_sender import send_slack_message

def main():
    # ------ Set Constants ------
    # Constants are now imported from const_config
    S3_PATH_PREFIX = "rawdata/aws"
    BUCKET_NAME = "spotlake"
    
    # ------ Set time data ------
    parser = argparse.ArgumentParser()
    parser.add_argument('--timestamp', dest='timestamp', action='store', help='Timestamp in format YYYY-MM-DDTHH:MM')
    args = parser.parse_args()
    
    if args.timestamp:
        # Handle EventBridge timestamp format (YYYY-MM-DDTHH:MM:SSZ)
        if args.timestamp.endswith('Z'):
            timestamp_utc = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M:%SZ")
        else:
            try:
                timestamp_utc = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                timestamp_utc = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M")
    else:
        start_time = datetime.now(timezone.utc)
        timestamp_utc = start_time.replace(minute=((start_time.minute // 10) * 10), second=0)

    S3_DIR_NAME = timestamp_utc.strftime('%Y/%m/%d')
    S3_OBJECT_PREFIX = timestamp_utc.strftime('%H-%M')
    
    print(f"Collecting Spot IF for timestamp: {timestamp_utc}")
    start_time = datetime.now(timezone.utc)

    try:
        # ------ Load Spot Info System ------
        # Try to find spotinfo in PATH, otherwise check common locations
        executable_path = shutil.which("spotinfo")
        if not executable_path:
            if os.path.exists("/opt/spotinfo"):
                executable_path = "/opt/spotinfo"
            elif os.path.exists("/usr/local/bin/spotinfo"):
                executable_path = "/usr/local/bin/spotinfo"
            else:
                raise FileNotFoundError("spotinfo executable not found")
        
        print(f"Using spotinfo executable at: {executable_path}")

        command = [executable_path, "--output", "csv", "--region", "all"]

        # ------ Execute a Command ------
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        stdout = result.stdout

        # ------ Collect Spot IF ------
        spotinfo_string = stdout
        spotinfo_list = [row.split(',') for row in spotinfo_string.split('\n')]

        spotinfo_dict = {'Region' : [],
                        'InstanceType' : [],
                        'IF' : []}
        
        # remove column name from data using indexing
        # The original code used spotinfo_list[2:-1]. 
        # Let's verify if this is still valid or if we should be safer.
        # Assuming the output format hasn't changed.
        # Row 0: "SpotInfo v..."
        # Row 1: Headers
        # Last row: Empty string (due to split)
        
        for spotinfo in spotinfo_list[2:-1]:
            if len(spotinfo) < 6:
                continue
            spotinfo_dict['Region'].append(spotinfo[0])
            spotinfo_dict['InstanceType'].append(spotinfo[1])
            spotinfo_dict['IF'].append(spotinfo[5])
        
        spotinfo_df = pd.DataFrame(spotinfo_dict)

        frequency_map = {'<5%': 3.0, '5-10%': 2.5, '10-15%': 2.0, '15-20%': 1.5, '>20%': 1.0}
        spotinfo_df['IF'] = spotinfo_df['IF'].replace(frequency_map).infer_objects(copy=False)
        
        print(f"Collected {len(spotinfo_df)} rows of Spot IF data")
        
    except Exception as e:
        send_slack_message(f"Error during spot if collection\n{e}")
        raise e
        
    end_time = datetime.now(timezone.utc)
    print(f"Collecting time is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")

    # ------ Save Raw Data in S3 ------
    start_time = datetime.now(timezone.utc)
    try:
        buffer = io.BytesIO()
        pickle.dump(spotinfo_df, buffer)
        buffer.seek(0)

        compressed_buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=compressed_buffer, mode='wb') as gz:
            gz.write(buffer.getvalue())
        compressed_buffer.seek(0)
        
        s3 = boto3.client('s3')
        key = f"{S3_PATH_PREFIX}/spot_if/{S3_DIR_NAME}/{S3_OBJECT_PREFIX}_spot_if.pkl.gz"
        s3.upload_fileobj(compressed_buffer, BUCKET_NAME, key)
        print(f"Uploaded data to s3://{BUCKET_NAME}/{key}")
        
    except Exception as e:
        send_slack_message(f"Error saving spot if data in s3\n{e}")
        raise e
        
    end_time = datetime.now(timezone.utc)
    print(f"Upload time is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")    

if __name__ == "__main__":
    start_time = datetime.now(timezone.utc)
    main()
    end_time = datetime.now(timezone.utc)
    print(f"Running time is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")
