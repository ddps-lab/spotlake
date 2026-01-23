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

from utils.common import S3, Logger
from utils.constants import AZURE_CONST, STORAGE_CONST
from utils.slack_msg_sender import send_slack_message
from merge import upload_data, compare_data

def merge_if_saving_price_sps_df(price_saving_if_df, sps_df, az=True):
    join_df = pd.merge(price_saving_if_df, sps_df, on=['InstanceTier', 'InstanceType', 'Region'], how='outer')
    
    # Rename time columns - handle both suffix and no-suffix cases
    rename_map = {}
    if 'time_x' in join_df.columns:
        rename_map['time_x'] = 'PriceEviction_Update_Time'
    if 'time_y' in join_df.columns:
        rename_map['time_y'] = 'Time'
    if 'time' in join_df.columns and 'time_y' not in join_df.columns:
        # Only SPS has time column, so no suffix
        rename_map['time'] = 'Time'
    
    join_df.rename(columns=rename_map, inplace=True)
    join_df.drop(columns=['id', 'InstanceTypeSPS', 'RegionCodeSPS'], inplace=True, errors='ignore')

    # Only fillna if both columns exist
    if 'Time' in join_df.columns and 'PriceEviction_Update_Time' in join_df.columns:
        join_df['Time'].fillna(join_df['PriceEviction_Update_Time'], inplace=True)

    columns = ["InstanceTier", "InstanceType", "Region", "OndemandPrice", "SpotPrice", "Savings", "IF",
        "DesiredCount", "Score", "Time", "T2", "T3"]

    if az:
        columns.insert(-4, "AvailabilityZone")

    # Ensure all columns exist before selecting them
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
        "Score": -1,  # Changed from "N/A" to -1 for Int64 compatibility
        "AvailabilityZone": "N/A",
        "Time": "N/A",
        "T2": 0,
        "T3": 0
    }, inplace=True)
    
    # Convert Score to integer (0-10 range, no decimals needed)
    join_df["Score"] = join_df["Score"].astype("int")

    join_df = join_df[
        ~((join_df["OndemandPrice"] == -1) &
          (join_df["SpotPrice"] == -1) &
          (join_df["Savings"] == -1) &
          (join_df["IF"] == -1))
    ]
    
    # Remove Gov regions (additional safety layer)
    join_df = join_df[
        ~join_df['Region'].astype(str).str.contains('gov', case=False, na=False)
    ]
    
    # Remove rows without valid SPS data (IF/Price only combinations)
    # Score=-1 AND AvailabilityZone=N/A means no SPS placement data
    join_df = join_df[
        ~((join_df["Score"] == -1) & (join_df["AvailabilityZone"] == "N/A"))
    ]

    return join_df

def merge_price_saving_if_df(price_df, if_df):
    # Lambda Logic: Join on armRegionName (Price Code) == Region (IF Code)
    join_df = pd.merge(price_df, if_df,
                    left_on=['InstanceType', 'InstanceTier', 'armRegionName'],
                    right_on=['InstanceType', 'InstanceTier', 'Region'],
                    how='outer')
    
    # Select columns and rename
    # Note: Region_x is Price Region Name ("East US"), Region_y is IF Region Code ("eastus")
    join_df = join_df[['InstanceTier', 'InstanceType', 'Region_x', 'armRegionName', 'OndemandPrice_x', 'SpotPrice_x', 'Savings_x', 'IF']]
    
    # Filter rows where SpotPrice is NaN (Lambda logic: join_df[~join_df['SpotPrice_x'].isna()])
    join_df = join_df[~join_df['SpotPrice_x'].isna()]

    join_df.rename(columns={'Region_x' : 'Region', 'OndemandPrice_x' : 'OndemandPrice', 'SpotPrice_x' : 'SpotPrice', 'Savings_x' : 'Savings'}, inplace=True)
    return join_df

def main():
    Logger.info("Start Merge Data Script")
    start_time = datetime.now(timezone.utc)

    parser = argparse.ArgumentParser()
    parser.add_argument('--sps_key', dest='sps_key', action='store', help='S3 Key of the SPS file')
    parser.add_argument('--timestamp', dest='timestamp', action='store')
    args = parser.parse_args()

    s3_client = boto3.client('s3')
    # Use WRITE_BUCKET_NAME (Test) for listing/reading raw data
    BUCKET_NAME = STORAGE_CONST.WRITE_BUCKET_NAME

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
        
        date_path = timestamp_utc.strftime("%Y/%m/%d")
        time_str = timestamp_utc.strftime("%H-%M")
        
        # Try to find any sps file for this time in Test Bucket
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
        # Load Data from WRITE_BUCKET (Test)
        sps_df = S3.read_file(sps_key, 'pkl.gz')
        if sps_df is None:
             raise ValueError(f"SPS data missing at {sps_key}")
        
        Logger.info(f"Loaded SPS: {len(sps_df)} rows")
        
        # CRITICAL: Filter to latest timestamp only
        # SPS files may contain historical data causing massive row explosion
        if 'time' in sps_df.columns:
            latest_time = sps_df['time'].max()
            time_range = f"{sps_df['time'].min()} to {latest_time}"
            Logger.info(f"SPS time range: {time_range}")
            
            sps_df = sps_df[sps_df['time'] == latest_time].copy()
            Logger.info(f"Filtered SPS to latest timestamp. Rows: {len(sps_df)}")
        
        # NOTE: SPS has both 'Region' (region name) and 'RegionCodeSPS' (region code)
        # Lambda keeps Region as region NAME for merging
        # Do NOT replace Region - it must stay as name to match price_saving_if_df
        
        # Strip potential whitespace and lower case keys
        for col in ['InstanceTier', 'InstanceType', 'Region']:
             if col in sps_df.columns:
                 sps_df[col] = sps_df[col].astype(str).str.strip().str.lower()

        print("DEBUG: SPS DF Head (Normalized):") 
        print(sps_df[['InstanceTier', 'InstanceType', 'Region']].head())
        print("DEBUG: SPS Unique Regions (Top 5):", sps_df['Region'].unique()[:5])
        print("DEBUG: SPS Unique InstanceTypes (Top 5):", sps_df['InstanceType'].unique()[:5])
             
        if_df = S3.read_file(if_key, 'pkl.gz', bucket_name=STORAGE_CONST.WRITE_BUCKET_NAME)
        if if_df is not None:
            # Strip potential whitespace and lower case keys
            for col in ['InstanceTier', 'InstanceType', 'Region']:
                 if col in if_df.columns:
                     if_df[col] = if_df[col].astype(str).str.strip().str.lower()

            print("DEBUG: IF DF Head (Normalized):")
            print(if_df[['InstanceTier', 'InstanceType', 'Region']].head())
            print("DEBUG: IF Unique Regions (Top 5):", if_df['Region'].unique()[:5])
            print("DEBUG: IF Unique InstanceTypes (Top 5):", if_df['InstanceType'].unique()[:5])

        price_df = S3.read_file(price_key, 'pkl.gz', bucket_name=STORAGE_CONST.WRITE_BUCKET_NAME)
        if price_df is not None:
             # Strip potential whitespace and lower case keys
            for col in ['InstanceTier', 'InstanceType', 'Region', 'armRegionName']:
                 if col in price_df.columns:
                     price_df[col] = price_df[col].astype(str).str.strip().str.lower()

            print("DEBUG: Price DF Head (Normalized):")
            print(price_df[['InstanceTier', 'InstanceType', 'Region']].head())
            print("DEBUG: Price Unique Regions (Top 5):", price_df['Region'].unique()[:5])
            print("DEBUG: Price Unique InstanceTypes (Top 5):", price_df['InstanceType'].unique()[:5])
        
        if if_df is None:
             Logger.warning("IF data missing. Proceeding with empty IF columns.")
             if_df = pd.DataFrame() 
        if price_df is None:
             Logger.warning("Price data missing. Proceeding with empty Price columns.")
             price_df = pd.DataFrame()

        if not price_df.empty and not if_df.empty:
            print("\nDEBUG: Before Price+IF Merge:")
            print(f"  Price sample: {price_df[['InstanceType', 'Region', 'armRegionName']].head(2)}")
            print(f"  IF sample: {if_df[['InstanceType', 'Region']].head(2)}")
            # Drop dummy cols from IF is not needed if we use merge_price_saving_if_df
            # Use Lambda-aligned logic
            price_saving_if_df = merge_price_saving_if_df(price_df, if_df)
            print(f"  Merged sample: {price_saving_if_df[['InstanceType', 'Region']].head(2)}")
            
        elif not price_df.empty:
            price_saving_if_df = price_df
            price_saving_if_df['IF'] = -1
        elif not if_df.empty:
            price_saving_if_df = if_df
            # Missing Price means no Ondemand/Spot price info
        else:
            price_saving_if_df = pd.DataFrame(columns=['InstanceTier', 'InstanceType', 'Region', 'OndemandPrice', 'SpotPrice', 'Savings', 'IF'])

        # Merge with SPS
        print("\nDEBUG: Before SPS Merge:")
        print(f"  price_saving_if sample: {price_saving_if_df[['InstanceType', 'Region']].head(2)}")
        print(f"  SPS sample: {sps_df[['InstanceType', 'Region']].head(2)}")
        sps_merged_df = merge_if_saving_price_sps_df(price_saving_if_df, sps_df, az=True)
        print(f"\nDEBUG: After Merge - Result shape: {sps_merged_df.shape}")
        print(f"  Sample with IF/Score: {sps_merged_df[['InstanceType', 'Region', 'IF', 'Score', 'DesiredCount']].head(3)}")

        # Load Previous Data from WRITE_BUCKET (Test)
        prev_all_data = S3.read_file(AZURE_CONST.S3_LATEST_ALL_DATA_AVAILABILITY_ZONE_TRUE_PKL_GZIP_SAVE_PATH, 'pkl.gz', bucket_name=STORAGE_CONST.WRITE_BUCKET_NAME)
        
        # CRITICAL: Filter prev_all_data to latest timestamp
        # Multiple timestamps in prev_all_data cause Cartesian product in merge
        if prev_all_data is not None and not prev_all_data.empty:
            Logger.info(f"Loaded prev_all_data: {len(prev_all_data)} rows")
            
            # Check for Time column (current) or SPS_Update_Time (legacy)
            time_col = None
            if 'Time' in prev_all_data.columns:
                time_col = 'Time'
            elif 'SPS_Update_Time' in prev_all_data.columns:
                time_col = 'SPS_Update_Time'
                prev_all_data.rename(columns={'SPS_Update_Time': 'Time'}, inplace=True)
            
            if time_col or 'Time' in prev_all_data.columns:
                latest_prev_time = prev_all_data['Time'].max()
                time_range = f"{prev_all_data['Time'].min()} to {latest_prev_time}"
                Logger.info(f"prev_all_data time range: {time_range}")
                
                prev_all_data = prev_all_data[prev_all_data['Time'] == latest_prev_time].copy()
                Logger.info(f"Filtered prev_all_data to latest timestamp. Rows: {len(prev_all_data)}")
            
            # Remove Gov regions from prev_all_data
            if 'Region' in prev_all_data.columns:
                gov_count = prev_all_data['Region'].astype(str).str.contains('gov', case=False, na=False).sum()
                if gov_count > 0:
                    Logger.info(f"Removing {gov_count} Gov region rows from prev_all_data")
                    prev_all_data = prev_all_data[
                        ~prev_all_data['Region'].astype(str).str.contains('gov', case=False, na=False)
                    ]
                    Logger.info(f"After Gov filter: {len(prev_all_data)} rows")
        
        query_success = timestream_success = cloudwatch_success = update_latest_success = save_raw_success = False
        
        # Compare and Process
        if prev_all_data is not None and not prev_all_data.empty:
            prev_all_data.drop(columns=['id'], inplace=True, errors='ignore')
            
            # Check merge key dtypes
            Logger.info(f"[MERGE DEBUG] Before compare_max_instance:")
            Logger.info(f"  sps_merged_df: {len(sps_merged_df)} rows")
            Logger.info(f"  prev_all_data: {len(prev_all_data)} rows")
            Logger.info(f"  sps_merged_df key dtypes: InstanceType={sps_merged_df['InstanceType'].dtype}, Region={sps_merged_df['Region'].dtype}, AZ={sps_merged_df['AvailabilityZone'].dtype}, DC={sps_merged_df['DesiredCount'].dtype}")
            Logger.info(f"  prev_all_data key dtypes: InstanceType={prev_all_data['InstanceType'].dtype}, Region={prev_all_data['Region'].dtype}, AZ={prev_all_data['AvailabilityZone'].dtype}, DC={prev_all_data['DesiredCount'].dtype}")
            
            # Check for duplicates in merge keys
            sps_dup_count = sps_merged_df.duplicated(subset=['InstanceType', 'Region', 'AvailabilityZone', 'DesiredCount']).sum()
            prev_dup_count = prev_all_data.duplicated(subset=['InstanceType', 'Region', 'AvailabilityZone', 'DesiredCount']).sum()
            Logger.info(f"  sps_merged_df duplicate keys: {sps_dup_count}")
            Logger.info(f"  prev_all_data duplicate keys: {prev_dup_count}")
            
            # T2/T3 Calculation
            sps_merged_df = compare_data.compare_max_instance(prev_all_data, sps_merged_df, desired_count)
            Logger.info(f"[MERGE DEBUG] After compare_max_instance: {len(sps_merged_df)} rows")
            Logger.info(f"T2/T3 calculation complete. Result rows: {len(sps_merged_df)}")
            
            # Detect Changes
            workload_cols = ['InstanceTier', 'InstanceType', 'Region', 'AvailabilityZone', 'DesiredCount']
            feature_cols = ['OndemandPrice', 'SpotPrice', 'IF', 'Score', 'Time', 'T2', 'T3']
            
            changed_df = compare_data.compare_sps(prev_all_data, sps_merged_df, workload_cols, feature_cols)
            
            if changed_df is not None and not changed_df.empty:
                query_success = upload_data.query_selector(changed_df)
                timestream_success = upload_data.upload_timestream(changed_df, timestamp_utc)
            else:
                Logger.info("No changes detections.")
                query_success = True
                timestream_success = True
                
            cloudwatch_success = upload_data.upload_cloudwatch(sps_merged_df, timestamp_utc)
            
            # Free prev_all_data memory explicitly
            del prev_all_data
            import gc
            gc.collect()
            Logger.info("Memory cleanup complete")
        else:
            Logger.info("First run or no previous data. Skipping comparison.")
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
