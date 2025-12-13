import sys
import os
import argparse
import boto3
import pickle
import gzip
import pandas as pd
from datetime import datetime, timezone, timedelta

# Add parent directory to path to import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.common import S3, AZURE_CONST, STORAGE_CONST, Logger
from utils.slack_msg_sender import send_slack_message
from merge import upload_data, compare_data

def merge_if_saving_price_sps_df(price_saving_if_df, sps_df, az=True):
    # Ensure join keys are present and types match
    join_df = pd.merge(price_saving_if_df, sps_df, on=['InstanceTier', 'InstanceType', 'Region'], how='outer')
    
    # Handle column renaming from merge collisions or source names
    if 'time_x' in join_df.columns:
        join_df.rename(columns={'time_x': 'PriceEviction_Update_Time'}, inplace=True)
    if 'time_y' in join_df.columns:
        join_df.rename(columns={'time_y': 'SPS_Update_Time'}, inplace=True)
        
    join_df.drop(columns=['id', 'InstanceTypeSPS', 'RegionCodeSPS'], inplace=True, errors='ignore')

    if 'SPS_Update_Time' in join_df.columns and 'PriceEviction_Update_Time' in join_df.columns:
        join_df['SPS_Update_Time'].fillna(join_df['PriceEviction_Update_Time'], inplace=True)
    
    # Define expected columns
    columns = ["InstanceTier", "InstanceType", "Region", "OndemandPrice", "SpotPrice", "Savings", "IF",
        "DesiredCount", "Score", "SPS_Update_Time", "T2", "T3"]

    if az:
        columns.insert(8, "AvailabilityZone") # Insert before Score

    # Ensure all columns exist
    for col in columns:
        if col not in join_df.columns:
            join_df[col] = None 
            
    join_df = join_df[columns]

    join_df.fillna({
        "InstanceTier": "N/A",
        "InstanceType": "N/A",
        "Region": "N/A",
        "OndemandPrice": -1,
        "SpotPrice": -1,
        "Savings": -1,
        "IF": -1,
        "DesiredCount": -1,
        "Score": "N/A",
        "AvailabilityZone": "N/A",
        "SPS_Update_Time": "N/A",
        "T2": 0,
        "T3": 0
    }, inplace=True)

    join_df = join_df[
        ~((join_df["OndemandPrice"] == -1) &
          (join_df["SpotPrice"] == -1) &
          (join_df["Savings"] == -1) &
          (join_df["IF"] == -1))
    ]

    return join_df

def main():
    Logger.info("Start Merge Data Script")
    start_time = datetime.now(timezone.utc)

    parser = argparse.ArgumentParser()
    parser.add_argument('--sps_key', dest='sps_key', action='store', help='S3 Key of the SPS file')
    parser.add_argument('--timestamp', dest='timestamp', action='store')
    args = parser.parse_args()

    s3_client = boto3.client('s3')
    BUCKET_NAME = STORAGE_CONST.BUCKET_NAME

    if args.sps_key:
        sps_key = args.sps_key
        # Parse timestamp from key: .../2025/12/13/13-10_sps_1.pkl.gz
        try:
            path_parts = sps_key.split('/')
            time_part = path_parts[-1].split('_')[0] # 13-10
            date_str = f"{path_parts[-4]}/{path_parts[-3]}/{path_parts[-2]}" # 2025/12/13
            
            datetime_str = f"{date_str} {time_part}"
            timestamp_utc = datetime.strptime(datetime_str, "%Y/%m/%d %H-%M").replace(tzinfo=timezone.utc)
            
            # Extract Desired Count
            desired_count = int(path_parts[-1].split('_')[-1].split('.')[0])
            
        except Exception as e:
            Logger.error(f"Failed to parse SPS key {sps_key}: {e}")
            raise e
            
    elif args.timestamp:
        if args.timestamp.endswith('Z'):
            timestamp_utc = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        else:
            try:
                timestamp_utc = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
            except ValueError:
                timestamp_utc = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M").replace(tzinfo=timezone.utc)
        
        # Try to find SPS key for this timestamp (Defaulting to desired count 1 or finding any?)
        # For now, let's assume if timestamp is given, we fail if we can't find a key, or user must provide key.
        # But for robustness, let's try to list objects.
        date_path = timestamp_utc.strftime("%Y/%m/%d")
        time_str = timestamp_utc.strftime("%H-%M")
        
        # Try to find any sps file for this time
        prefix = f"{AZURE_CONST.S3_RAW_DATA_PATH}/sps/{date_path}/{time_str}_sps_"
        objs = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
        if 'Contents' in objs:
            sps_key = objs['Contents'][0]['Key']
            desired_count = int(sps_key.split('_')[-1].split('.')[0])
            Logger.info(f"Found SPS Key from timestamp: {sps_key}")
        else:
            Logger.error("No SPS file found for timestamp.")
            return
    else:
        Logger.error("Must provide --sps_key or --timestamp")
        return

    Logger.info(f"Processing Timestamp: {timestamp_utc}")
    Logger.info(f"Desired Count: {desired_count}")

    # Construct other keys
    date_path = timestamp_utc.strftime("%Y/%m/%d")
    time_str = timestamp_utc.strftime("%H-%M")
    
    if_key = f"{AZURE_CONST.S3_RAW_DATA_PATH}/spot_if/{date_path}/{time_str}_spot_if.pkl.gz"
    price_key = f"{AZURE_CONST.S3_RAW_DATA_PATH}/spot_price/{date_path}/{time_str}_spot_price.pkl.gz"

    try:
        # Load Data
        sps_df = S3.read_file(sps_key, 'pkl.gz')
        if sps_df is None:
             raise ValueError(f"SPS data missing at {sps_key}")
             
        if_df = S3.read_file(if_key, 'pkl.gz')
        price_df = S3.read_file(price_key, 'pkl.gz')
        
        # If IF/Price missing, we can still proceed with SPS but fields will be empty/default?
        # Legacy Azure `load_sps.py` expects `price_saving_if_df` to be present.
        # `collect_sps.py` (legacy) had data collection integrated.
        # Here we depend on parallel jobs. If they failed, we might have partial data.
        # If price/if missing, we assume empty or fail. Let's assume empty to robustly handle partial failures or retries.
        if if_df is None:
             Logger.warning("IF data missing. Proceeding with empty IF columns.")
             if_df = pd.DataFrame() 
        if price_df is None:
             Logger.warning("Price data missing. Proceeding with empty Price columns.")
             price_df = pd.DataFrame()

        # Pre-merge Price and IF
        # Note: collect_price returns Savings, Price. collect_if returns IF.
        # We need to join them first into `price_saving_if_df` format expected by merge logic.
        # Or just merge three of them.
        
        # Legacy `merge_df.py` has `merge_price_saving_if_df`.
        # collect_price.py returns: ['InstanceTier', 'InstanceType', 'Region', 'OndemandPrice', 'SpotPrice', 'Savings']
        # collect_if.py returns: ['InstanceTier', 'InstanceType', 'Region', 'OndemandPrice', 'SpotPrice', 'Savings', 'IF'] (It actually has default -1s)
        
        # Actually `collect_if` returns a dataframe that has `IF` and dummy price cols.
        # `collect_price` returns `Savings`, `SpotPrice`, `OndemandPrice`.
        
        # We should merge them on InstanceType, InstanceTier, Region.
        # `collect_if` output has dummy price cols, we should likely drop them before merging or use them if price data is missing?
        # Valid `collect_price` data is better.
        
        if not price_df.empty and not if_df.empty:
            # Drop dummy cols from IF before merge to avoid suffixes
            if_df_clean = if_df[['InstanceTier', 'InstanceType', 'Region', 'IF']]
            price_saving_if_df = pd.merge(price_df, if_df_clean, on=['InstanceTier', 'InstanceType', 'Region'], how='outer')
        elif not price_df.empty:
            price_saving_if_df = price_df
            price_saving_if_df['IF'] = -1
        elif not if_df.empty:
            price_saving_if_df = if_df
        else:
            price_saving_if_df = pd.DataFrame(columns=['InstanceTier', 'InstanceType', 'Region', 'OndemandPrice', 'SpotPrice', 'Savings', 'IF'])

        # Merge with SPS
        sps_merged_df = merge_if_saving_price_sps_df(price_saving_if_df, sps_df, az=True)

        # Load Previous Data
        prev_all_data = S3.read_file(AZURE_CONST.S3_LATEST_ALL_DATA_AVAILABILITY_ZONE_TRUE_PKL_GZIP_SAVE_PATH, 'pkl.gz')
        
        query_success = timestream_success = cloudwatch_success = update_latest_success = save_raw_success = False
        
        # Compare and Process
        if prev_all_data is not None and not prev_all_data.empty:
            prev_all_data.drop(columns=['id'], inplace=True, errors='ignore')
            
            # T2/T3 Calculation
            sps_merged_df = compare_data.compare_max_instance(prev_all_data, sps_merged_df, desired_count)
            
            # Detect Changes
            workload_cols = ['InstanceTier', 'InstanceType', 'Region', 'AvailabilityZone', 'DesiredCount']
            feature_cols = ['OndemandPrice', 'SpotPrice', 'IF', 'Score', 'SPS_Update_Time', 'T2', 'T3']
            
            changed_df = compare_data.compare_sps(prev_all_data, sps_merged_df, workload_cols, feature_cols)
            
            if changed_df is not None and not changed_df.empty:
                query_success = upload_data.query_selector(changed_df)
                timestream_success = upload_data.upload_timestream(changed_df, timestamp_utc)
            else:
                Logger.info("No changes detections.")
                query_success = True
                timestream_success = True
                
            cloudwatch_success = upload_data.upload_cloudwatch(sps_merged_df, timestamp_utc)
        else:
            Logger.info("First run or no previous data. Skipping comparison.")
            # Treat all as new?
            update_latest_success = upload_data.update_latest(sps_merged_df)
            save_raw_success = upload_data.save_raw(sps_merged_df, timestamp_utc, az=True, data_type='desired_count_1' if desired_count==1 else 'multi')
            return

        # Upload Results
        update_latest_success = upload_data.update_latest(sps_merged_df)
        
        data_type = 'desired_count_1' if desired_count == 1 else 'multi'
        save_raw_success = upload_data.save_raw(sps_merged_df, timestamp_utc, az=True, data_type=data_type)
        
        Logger.info(f"Merge Execution Completed. UpdateLatest:{update_latest_success}, SaveRaw:{save_raw_success}, Timestream:{timestream_success}, CloudWatch:{cloudwatch_success}")

    except Exception as e:
        Logger.error(f"Merge failed: {e}")
        send_slack_message(f"Azure Merge Failed: {e}")
        raise e

if __name__ == "__main__":
    main()
