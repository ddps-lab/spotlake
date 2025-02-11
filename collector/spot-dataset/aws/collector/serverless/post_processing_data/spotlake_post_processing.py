# ------ import module ------
from datetime import datetime, timezone, timedelta
import boto3
import pickle, gzip, json
import pandas as pd
import numpy as np
import os

# ------ import user module ------
from slack_msg_sender import send_slack_message
from upload_data import upload_timestream, update_latest, save_raw, update_query_selector, update_config
from compare_data import compare, compare_max_instance

def main():
    # ------ Set time data ------
    start_time = datetime.now(timezone.utc)
    timestamp = start_time.replace(minute=((start_time.minute // 10) * 10), second=0) - timedelta(minutes=10)
    S3_DIR_NAME = timestamp.strftime('%Y/%m/%d')
    S3_OBJECT_PREFIX = timestamp.strftime('%H-%M')
    time_value = timestamp.strftime("%Y-%m-%d %H:%M:%S")

    # ------ Create Boto3 Session ------
    s3 = boto3.resource("s3")

    BUCKET_NAME = os.environ.get('S3_BUCKET')
    BUCKET_FILE_PATH = os.environ.get('PARENT_PATH')
    target_capacities = [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50]

    # ------ Load Data from PKL File in S3 ------
    config_path = "config.txt"
    text = s3.Object(BUCKET_NAME, f"config/{config_path}").get()["Body"].read().decode('utf-8').split("\n")

    target_capacity = int(text[0].strip())

    keys = [line.format(
                        BUCKET_FILE_PATH=BUCKET_FILE_PATH, 
                        S3_DIR_NAME=S3_DIR_NAME, 
                        S3_OBJECT_PREFIX=S3_OBJECT_PREFIX, 
                        target_capacity=target_capacities[target_capacity]
                    ) for line in text]

    try:
        sps_df = pickle.load(gzip.open(s3.Object(BUCKET_NAME, keys[1].strip()).get()["Body"]))
        spotinfo_df = pickle.load(gzip.open(s3.Object(BUCKET_NAME, keys[2].strip()).get()["Body"]))
        ondemand_price_df = pickle.load(gzip.open(s3.Object(BUCKET_NAME, keys[3].strip()).get()["Body"]))
        spot_price_df = pickle.load(gzip.open(s3.Object(BUCKET_NAME, keys[4].strip()).get()["Body"]))
    except Exception as e:
        send_slack_message(e)
        print(e)
    
    # ------ Create a DF by Selecting Only The Columns Required ------
    try:
        sps_df = sps_df[['InstanceType', 'Region', 'AZ', 'SPS', 'T3', 'T2']]
        spotinfo_df = spotinfo_df[['InstanceType', 'Region', 'IF']]
        ondemand_price_df = ondemand_price_df[['InstanceType', 'Region', 'OndemandPrice']]
        spot_price_df = spot_price_df[['InstanceType', 'AZ', 'SpotPrice']]
    except Exception as e:
        send_slack_message(e)
        print(e)

    # ------ Formatting Data ------
    spot_price_df['SpotPrice'] = spot_price_df['SpotPrice'].astype('float').round(5)
    ondemand_price_df['OndemandPrice'] = ondemand_price_df['OndemandPrice'].astype('float').round(5)

    # ------ Need to Change to Outer Join ------
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
    LATEST_PATH = f'{BUCKET_FILE_PATH}/latest_data/{filename}'
    try:
        previous_df = pd.DataFrame(json.load(s3.Object(BUCKET_NAME, LATEST_PATH).get()['Body']))
        
        # Verify that the data is in the old format
        columns_to_check = ["T3", "T2"]
        existing_columns = [col for col in columns_to_check if col in previous_df.columns]
        
        if len(existing_columns) == 0:
            raise
        else:
            previous_df = previous_df.drop(columns=['Id'])
    except:
        # If system is first time uploading data, make a new one and upload it to TSDB
        try:
            update_latest(merge_df)
            save_raw(merge_df, timestamp)
            upload_timestream(merge_df, timestamp)
            update_config(config_path, text, target_capacity, target_capacities)
        except Exception as e:
            send_slack_message(e)
            print(e)
        end_time = datetime.now(timezone.utc)
        print(f"Checking time of previous json file is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")
        return print("Can't load the previous df from s3 bucket or First run since changing the collector")

    end_time = datetime.now(timezone.utc)
    print(f"Checking time of previous json file is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")
    
    start_time = datetime.now(timezone.utc)
    # ------ Compare T3 and T2 Data ------
    current_df = compare_max_instance(merge_df, previous_df, target_capacities, target_capacity)

    # ------ Upload Merge DF to s3 Bucket ------
    try:
        update_latest(current_df)
        save_raw(current_df, timestamp)
    except Exception as e:
        send_slack_message(e)
        print(e)
        
    # ------ Compare All Data ------
    workload_cols = ['InstanceType', 'Region', 'AZ']
    feature_cols = ['SPS', 'T3', 'T2', 'IF', 'SpotPrice', 'OndemandPrice']

    changed_df, removed_df = compare(previous_df, current_df, workload_cols, feature_cols) # compare previous_df and current_df to extract changed rows)
    end_time = datetime.now(timezone.utc)
    print(f"Compare time is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")

    start_time = datetime.now(timezone.utc)
    # ------ Upload TSDB ------
    try:
        upload_timestream(changed_df, timestamp)
        upload_timestream(removed_df, timestamp)
    except Exception as e:
        send_slack_message(e)
        print(e)
    end_time = datetime.now(timezone.utc)
    print(f"Uploading time to TSDB is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")

    start_time = datetime.now(timezone.utc)
    # ------ Upload Spotlake Query Selector to S3 ------
    try:
        update_query_selector(changed_df)
    except Exception as e:
        send_slack_message(e)
        print(e)
    end_time = datetime.now(timezone.utc)
    print(f"Uploading time of query selector data is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")

    # ------ Write Target Capacity Value in Text File ------
    update_config(config_path, text, target_capacity, target_capacities)

def lambda_handler(event, context):
    start_time = datetime.now(timezone.utc)
    main()
    end_time = datetime.now(timezone.utc)
    print(f"Running time is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")
    return "Process completed successfully"