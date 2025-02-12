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
    print("Start Lambda Function")
    start_time = datetime.now(timezone.utc)

    # ------ Set Constants ------
    BUCKET_NAME = os.environ.get('S3_BUCKET')
    BUCKET_FILE_PATH = os.environ.get('PARENT_PATH')

    TIMESTAMP = start_time.replace(minute=((start_time.minute // 10) * 10), second=0) - timedelta(minutes=10)
    S3_DIR_NAME = TIMESTAMP.strftime('%Y/%m/%d')
    S3_OBJECT_PREFIX = TIMESTAMP.strftime('%H-%M')
    
    SPS_FILE_PREFIX = f"{BUCKET_FILE_PATH}/sps/{S3_DIR_NAME}"
    SPOTIF_FILE_NAME = f"{BUCKET_FILE_PATH}/spot_if/{S3_DIR_NAME}/{S3_OBJECT_PREFIX}_spot_if.pkl.gz"
    ONDEMAND_PRICE_FILE_NAME = f"{BUCKET_FILE_PATH}/ondemand_price/{S3_DIR_NAME}/ondemand_price.pkl.gz"
    SPOTPRICE_FILE_NAME = f"{BUCKET_FILE_PATH}/spot_price/{S3_DIR_NAME}/{S3_OBJECT_PREFIX}_spot_price.pkl.gz"

    # ------ Set time data ------
    time_value = TIMESTAMP.strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        # ------ Create Boto3 Session ------
        s3 = boto3.resource("s3")
        s3_client = boto3.client('s3')

        # ------ Find Sps File in S3 ------
        sps_file_list = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=SPS_FILE_PREFIX)
        sps_files = []
        for obj in sps_file_list['Contents']:
            if obj['Key'].startswith(f"{SPS_FILE_PREFIX}/{S3_OBJECT_PREFIX}"):
                sps_files.append(obj['Key'])

        sps_file_name = sps_files[0]
        print(sps_file_name)
        target_capacity = int(sps_file_name.split('/')[-1].split('_')[2].split('.')[0])

        # ------ Load Data from PKL File in S3 ------
        sps_df = pickle.load(gzip.open(s3.Object(BUCKET_NAME, sps_file_name).get()["Body"]))
        spotinfo_df = pickle.load(gzip.open(s3.Object(BUCKET_NAME, SPOTIF_FILE_NAME.strip()).get()["Body"]))
        ondemand_price_df = pickle.load(gzip.open(s3.Object(BUCKET_NAME, ONDEMAND_PRICE_FILE_NAME.strip()).get()["Body"]))
        spot_price_df = pickle.load(gzip.open(s3.Object(BUCKET_NAME, SPOTPRICE_FILE_NAME.strip()).get()["Body"]))

        # ------ Create a DF by Selecting Only The Columns Required ------
        sps_df = sps_df[['InstanceType', 'Region', 'AZ', 'SPS', 'T3', 'T2']]
        spotinfo_df = spotinfo_df[['InstanceType', 'Region', 'IF']]
        ondemand_price_df = ondemand_price_df[['InstanceType', 'Region', 'OndemandPrice']]
        spot_price_df = spot_price_df[['InstanceType', 'AZ', 'SpotPrice']]
        
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
        LATEST_PATH = f'latest_data/{filename}'
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
            update_latest(merge_df)
            save_raw(merge_df, TIMESTAMP)
            upload_timestream(merge_df, TIMESTAMP)
            end_time = datetime.now(timezone.utc)
            print(f"Checking time of previous json file is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")
            return print("Can't load the previous df from s3 bucket or First run since changing the collector")

        end_time = datetime.now(timezone.utc)
        print(f"Checking time of previous json file is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")
        
        start_time = datetime.now(timezone.utc)
    
        # ------ Compare T3 and T2 Data ------
        current_df = compare_max_instance(merge_df, previous_df, target_capacity)

        # ------ Upload Merge DF to s3 Bucket ------
        update_latest(current_df)
        save_raw(current_df, TIMESTAMP)
            
        # ------ Compare All Data ------
        workload_cols = ['InstanceType', 'Region', 'AZ']
        feature_cols = ['SPS', 'T3', 'T2', 'IF', 'SpotPrice', 'OndemandPrice']

        changed_df, removed_df = compare(previous_df, current_df, workload_cols, feature_cols) # compare previous_df and current_df to extract changed rows)
        end_time = datetime.now(timezone.utc)
        print(f"Compare time is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")

        # ------ Upload TSDB ------
        start_time = datetime.now(timezone.utc)
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

def lambda_handler(event, context):
    start_time = datetime.now(timezone.utc)
    main()
    end_time = datetime.now(timezone.utc)
    print(f"Running time is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")
    return "Process completed successfully"