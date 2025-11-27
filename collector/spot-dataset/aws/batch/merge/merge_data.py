# ------ import module ------
from datetime import datetime, timezone, timedelta
import boto3
import pickle
import gzip
import json
import pandas as pd
import numpy as np
import os
import argparse
import sys

# ------ import user module ------
import sys
sys.path.append("/home/ubuntu/spotlake")
from const_config import AwsCollector, Storage
from utility.slack_msg_sender import send_slack_message
from upload_data import upload_timestream, update_latest, save_raw, update_query_selector, update_config
from compare_data import compare, compare_max_instance

class FirstRunError(Exception):
    pass

def main():
    print("Start Merge Data Script")
    start_time = datetime.now(timezone.utc)

    # ------ Parse Arguments ------
    parser = argparse.ArgumentParser()
    parser.add_argument('--sps_key', dest='sps_key', action='store', help='S3 Key of the SPS file')
    parser.add_argument('--bucket', dest='bucket', action='store', help='S3 Bucket Name')
    parser.add_argument('--timestamp', dest='timestamp', action='store', help='Timestamp in format YYYY-MM-DDTHH:MM (optional override)')
    args = parser.parse_args()

    # ------ Set Constants ------
    BUCKET_NAME = args.bucket if args.bucket else Storage.BUCKET_NAME
    S3_PATH_PREFIX = AwsCollector.S3_PATH_PREFIX
    # BUCKET_FILE_PATH is removed in favor of specific paths from const_config
    
    if args.sps_key:
        sps_file_name = args.sps_key
        # Extract info from key
        # Expected format: .../2023/11/23/02-10_sps_50.pkl.gz
        try:
            filename = sps_file_name.split('/')[-1]
            parts = filename.split('_')
            # parts[0] is "02-10" (time)
            # parts[1] is "sps"
            # parts[2] is "50.pkl.gz"
            time_part = parts[0]
            target_capacity = int(parts[2].split('.')[0])
            
            # Extract date from path
            # .../2023/11/23/...
            path_parts = sps_file_name.split('/')
            date_str = f"{path_parts[-4]}/{path_parts[-3]}/{path_parts[-2]}" # YYYY/MM/DD
            
            TIMESTAMP = datetime.strptime(f"{date_str} {time_part}", "%Y/%m/%d %H-%M").replace(tzinfo=timezone.utc)
            S3_DIR_NAME = date_str
            S3_OBJECT_PREFIX = time_part
            
        except Exception as e:
            print(f"Error parsing SPS key: {sps_file_name}. Error: {e}")
            # Fallback or exit?
            # If we can't parse, we might fail to find other files.
            raise e
    elif args.timestamp:
        # If timestamp provided but no key, try to find the file? 
        # Or assume this mode is for manual run?
        # Handle EventBridge timestamp format (YYYY-MM-DDTHH:MM:SSZ)
        if args.timestamp.endswith('Z'):
            TIMESTAMP = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        else:
            try:
                TIMESTAMP = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
            except ValueError:
                TIMESTAMP = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M").replace(tzinfo=timezone.utc)
        S3_DIR_NAME = TIMESTAMP.strftime('%Y/%m/%d')
        S3_OBJECT_PREFIX = TIMESTAMP.strftime('%H-%M')
        # We need a target capacity. Default to 50? Or loop?
        # The original lambda logic found the file.
        # Let's assume if timestamp is given, we might need to find the file or just fail if sps_key is missing.
        print("Timestamp provided without SPS Key. This mode might be ambiguous regarding Target Capacity.")
        return
    else:
        # Default behavior (like Lambda triggered by schedule? No, Lambda was triggered by something or just ran)
        # The original lambda: TIMESTAMP = start_time ... - 10 mins
        TIMESTAMP = start_time.replace(minute=((start_time.minute // 10) * 10), second=0) - timedelta(minutes=10)
        S3_DIR_NAME = TIMESTAMP.strftime('%Y/%m/%d')
        S3_OBJECT_PREFIX = TIMESTAMP.strftime('%H-%M')
        # It then listed objects to find the file.
        s3_client = boto3.client('s3')
        s3_client = boto3.client('s3')
        s3_client = boto3.client('s3')
        SPS_FILE_PREFIX = f"{S3_PATH_PREFIX}/sps/{S3_DIR_NAME}"
        sps_file_list = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=SPS_FILE_PREFIX)
        sps_files = []
        if 'Contents' in sps_file_list:
            for obj in sps_file_list['Contents']:
                if obj['Key'].startswith(f"{SPS_FILE_PREFIX}/{S3_OBJECT_PREFIX}"):
                    sps_files.append(obj['Key'])
        
        if not sps_files:
            print(f"No SPS files found for {S3_OBJECT_PREFIX}")
            return
            
        sps_file_name = sps_files[0] # Just take the first one? Original code did this.
        target_capacity = int(sps_file_name.split('/')[-1].split('_')[2].split('.')[0])

    print(f"Processing SPS File: {sps_file_name}")
    print(f"Timestamp: {TIMESTAMP}")
    print(f"Target Capacity: {target_capacity}")

    SPOTIF_FILE_NAME = f"{S3_PATH_PREFIX}/spot_if/{S3_DIR_NAME}/{S3_OBJECT_PREFIX}_spot_if.pkl.gz"
    ONDEMAND_PRICE_FILE_NAME = f"{S3_PATH_PREFIX}/ondemand_price/{S3_DIR_NAME}/ondemand_price.pkl.gz"
    SPOTPRICE_FILE_NAME = f"{S3_PATH_PREFIX}/spot_price/{S3_DIR_NAME}/{S3_OBJECT_PREFIX}_spot_price.pkl.gz"

    # ------ Set time data ------
    time_value = TIMESTAMP.strftime("%Y-%m-%d %H:%M:%S")

    try:
        # ------ Create Boto3 Session ------
        s3 = boto3.resource("s3")
        s3_client = boto3.client('s3')

        # ------ Load Data from PKL File in S3 ------
        print("Loading data files...")
        try:
            sps_df = pickle.load(gzip.open(s3.Object(BUCKET_NAME, sps_file_name).get()["Body"]))
        except Exception as e:
             print(f"Failed to load SPS file: {e}")
             raise e
             
        try:
            spotinfo_df = pickle.load(gzip.open(s3.Object(BUCKET_NAME, SPOTIF_FILE_NAME.strip()).get()["Body"]))
        except Exception as e:
            print(f"Failed to load Spot IF file ({SPOTIF_FILE_NAME}): {e}")
            # Should we fail or continue with empty? Original code would fail.
            raise e

        try:
            ondemand_price_df = pickle.load(gzip.open(s3.Object(BUCKET_NAME, ONDEMAND_PRICE_FILE_NAME.strip()).get()["Body"]))
        except Exception as e:
             print(f"Failed to load OnDemand Price file ({ONDEMAND_PRICE_FILE_NAME}): {e}")
             # Maybe ondemand price is not collected every 10 mins? 
             # Original code assumes it exists.
             raise e

        try:
            spot_price_df = pickle.load(gzip.open(s3.Object(BUCKET_NAME, SPOTPRICE_FILE_NAME.strip()).get()["Body"]))
        except Exception as e:
            print(f"Failed to load Spot Price file ({SPOTPRICE_FILE_NAME}): {e}")
            raise e

        # ------ Create a DF by Selecting Only The Columns Required ------
        sps_df = sps_df[['InstanceType', 'Region', 'AZ', 'SPS', 'T3', 'T2']]
        spotinfo_df = spotinfo_df[['InstanceType', 'Region', 'IF']]
        ondemand_price_df = ondemand_price_df[['InstanceType', 'Region', 'OndemandPrice']]
        spot_price_df = spot_price_df[['InstanceType', 'AZ', 'SpotPrice']]

        # ------ Formatting Data ------
        spot_price_df['SpotPrice'] = spot_price_df['SpotPrice'].astype('float').round(5)
        ondemand_price_df['OndemandPrice'] = ondemand_price_df['OndemandPrice'].astype('float').round(5)

        # ------ Need to Change to Outer Join ------
        print("Merging dataframes...")
        merge_df = pd.merge(sps_df, spotinfo_df, how="outer")
        merge_df = pd.merge(merge_df, ondemand_price_df, how="outer")
        merge_df = pd.merge(merge_df, spot_price_df, how="outer")

        merge_df['Savings'] = 100.0 - (merge_df['SpotPrice'] * 100 / merge_df['OndemandPrice'])
        merge_df['Savings'] = merge_df['Savings'].fillna(-1)
        merge_df['SPS'] = merge_df['SPS'].fillna(-1)
        merge_df['SpotPrice'] = merge_df['SpotPrice'].fillna(-1)
        merge_df['OndemandPrice'] = merge_df['OndemandPrice'].fillna(-1)
        merge_df['IF'] = merge_df['IF'].fillna(-1)

        merge_df['Savings'] = merge_df['Savings'].astype('int')
        merge_df['SPS'] = merge_df['SPS'].astype('int')
        merge_df['T3'] = merge_df['T3'].fillna(0).astype('int')
        merge_df['T2'] = merge_df['T2'].fillna(0).astype('int')

        merge_df = merge_df.drop(merge_df[(merge_df['AZ'].isna()) | (merge_df['Region'].isna()) | (merge_df['InstanceType'].isna())].index)

        merge_df.reset_index(drop=True, inplace=True)
        merge_df['Time'] = time_value

        end_time = datetime.now(timezone.utc)
        print(f"Merging time is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")

        # ------ Check The Previous DF File in S3 and Local ------
        previous_df = None
        start_time = datetime.now(timezone.utc)
        filename = 'latest_aws.json'
        LATEST_PATH = f'latest_data/{filename}'
        try:
            previous_df = pd.DataFrame(json.load(s3.Object(BUCKET_NAME, LATEST_PATH).get()['Body']))
            # Verify that the data is in the old format
            columns_to_check = ["T3", "T2"]
            existing_columns = [col for col in columns_to_check if col in previous_df.columns]

            if len(existing_columns) == 0:
                raise FirstRunError("Can't load the previous df from s3 bucket or First run since changing the collector")
            else:
                previous_df = previous_df.drop(columns=['id'])
        except Exception as e: # Catching generic exception to handle NoSuchKey or FirstRunError
            print(f"First run or error loading previous data: {e}")
            # If system is first time uploading data, make a new one and upload it to TSDB
            update_latest(merge_df, TIMESTAMP)
            save_raw(merge_df, TIMESTAMP)
            upload_timestream(merge_df, TIMESTAMP)
            end_time = datetime.now(timezone.utc)
            print(f"Checking time of previous json file is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")
            return

        end_time = datetime.now(timezone.utc)
        print(f"Checking time of previous json file is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")

        start_time = datetime.now(timezone.utc)

        # ------ Compare T3 and T2 Data ------
        print("Comparing with previous data...")
        current_df = compare_max_instance(previous_df, merge_df, target_capacity)

        # ------ Upload Merge DF to s3 Bucket ------
        update_latest(current_df, TIMESTAMP)
        save_raw(current_df, TIMESTAMP)

        # ------ Compare All Data ------
        workload_cols = ['InstanceType', 'Region', 'AZ']
        feature_cols = ['SPS', 'T3', 'T2', 'IF', 'SpotPrice', 'OndemandPrice']

        changed_df, removed_df = compare(previous_df, current_df, workload_cols, feature_cols)  # compare previous_df and current_df to extract changed rows)
        end_time = datetime.now(timezone.utc)
        print(f"Compare time is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")

        # ------ Upload TSDB ------
        start_time = datetime.now(timezone.utc)
        print(f"Uploading {len(changed_df)} changed rows and {len(removed_df)} removed rows to Timestream...")
        upload_timestream(changed_df, TIMESTAMP)
        upload_timestream(removed_df, TIMESTAMP)
        end_time = datetime.now(timezone.utc)
        print(f"Uploading time to TSDB is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")

        # ------ Upload Spotlake Query Selector to S3 ------
        start_time = datetime.now(timezone.utc)
        update_query_selector(changed_df)
        end_time = datetime.now(timezone.utc)
        print(f"Uploading time of query selector data is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")
    except Exception as e:
        send_slack_message(e)
        print(e)
        raise

if __name__ == "__main__":
    main()
