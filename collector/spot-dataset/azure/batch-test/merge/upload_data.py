import sys
import os
import pandas as pd
import boto3
import time
import json
import pickle
import gzip
from datetime import datetime
import concurrent.futures

# Add parent directory to path to import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.common import CW, S3, TimestreamWrite, Logger
from utils.constants import AZURE_CONST, STORAGE_CONST

session = boto3.session.Session(region_name='us-west-2')

# Update latest_azure.json in S3
def update_latest_price_saving_if(data, time_datetime):
    try:
        # Copy once at start to avoid mutating caller's DataFrame
        data = data.copy()
        data['id'] = data.index + 1
        data = data[['id', 'InstanceTier', 'InstanceType', 'Region', 'OndemandPrice', 'SpotPrice', 'Savings', 'IF']]

        data['OndemandPrice'] = data['OndemandPrice'].fillna(-1)
        data['Savings'] = data['Savings'].fillna(-1)
        data['IF'] = data['IF'].fillna(-1)

        data['time'] = datetime.strftime(time_datetime, '%Y-%m-%d %H:%M:%S')

        local_json_path = f"{AZURE_CONST.SERVER_SAVE_DIR}/{AZURE_CONST.LATEST_PRICE_SAVING_IF_FILENAME}"
        local_pkl_gz_path = f"{AZURE_CONST.SERVER_SAVE_DIR}/{AZURE_CONST.LATEST_PRICE_SAVING_IF_PKL_GZIP_FILENAME}"
        local_pkl_path = f"{AZURE_CONST.SERVER_SAVE_DIR}/{AZURE_CONST.SERVER_SAVE_FILENAME}"

        data.to_json(local_json_path, orient='records')
        data.to_pickle(local_pkl_gz_path, compression="gzip")
        pickle.dump(data, open(local_pkl_path, "wb"))

        S3.upload_file(data.to_dict(orient='records'), AZURE_CONST.S3_LATEST_PRICE_SAVING_IF_DATA_SAVE_PATH, "json", set_public_read=True)
        S3.upload_file(data, AZURE_CONST.S3_LATEST_PRICE_SAVING_IF_GZIP_SAVE_PATH, "pkl.gz", set_public_read=True)

        return True
    except Exception as e:
        print(f"update_latest_price_saving_if failed: {e}")
        return False

# Save raw data in S3
def save_raw_price_saving_if(data, time_datetime):
    try:
        data['Time'] = time_datetime.strftime("%Y-%m-%d %H:%M:%S")
        data = data[['Time','InstanceTier','InstanceType', 'Region', 'OndemandPrice','SpotPrice', 'IF', 'Savings']]

        s3_dir_name = time_datetime.strftime("%Y/%m/%d")
        s3_obj_name = time_datetime.strftime("%H-%M-%S")
        
        s3_key = f"{AZURE_CONST.S3_RAW_DATA_PATH}/if_saving_price/{s3_dir_name}/{s3_obj_name}.csv.gz"
        
        S3.upload_file(data, s3_key, "df_to_csv.gz", set_public_read=True)
        return True
    except Exception as e:
        print(f"save_raw_price_saving_if failed: {e}")
        return False

def upload_cloudwatch(data, time_datetime):
    Logger.info("Executing upload_cloudwatch!")
    try:
        ondemand_count = len(data.drop(columns=['IF', 'SpotPrice', 'Savings', 'Score']).dropna())
        spot_count = len(data.drop(columns=['IF', 'OndemandPrice', 'Savings', 'Score']).dropna())
        if_count = len(data.drop(columns=['OndemandPrice', 'SpotPrice', 'Savings', 'Score']).dropna())
        sps_count = len(data.drop(columns=['IF', 'OndemandPrice', 'SpotPrice', 'Savings']).dropna())

        log_event = [{
            'timestamp': int(time_datetime.timestamp()) * 1000,
            'message': f'AZUREONDEMAND: {ondemand_count} AZURESPOT: {spot_count} AZUREIF: {if_count} AZURESPS: {sps_count}'
        }]

        CW.put_log_events(
            log_events=log_event,
            log_group_name=STORAGE_CONST.SPOT_DATA_COLLECTION_LOG_GROUP_NAME,
            log_stream_name=STORAGE_CONST.LOG_STREAM_NAME
        )
        return True

    except Exception as e:
        print(f"upload_cloudwatch failed. error: {e}")
        return False

def query_selector(data):
    Logger.info("Executing query_selector!")
    try:
        # Read from WRITE_BUCKET (Test) because we are updating the selector based on new test data
        prev_query_selector_data = S3.read_file(AZURE_CONST.S3_QUERY_SELECTOR_SAVE_PATH, 'json', bucket_name=STORAGE_CONST.WRITE_BUCKET_NAME)
        if prev_query_selector_data:
            prev_selector_df = pd.DataFrame(prev_query_selector_data)
            selector_df = pd.concat([
                prev_selector_df[['InstanceTier', 'InstanceType', 'Region']],
                data[['InstanceTier', 'InstanceType', 'Region']]
            ], ignore_index=True).dropna().drop_duplicates().reset_index(drop=True)
        else:
            selector_df = data[['InstanceTier', 'InstanceType', 'Region']].dropna().drop_duplicates().reset_index(drop=True)

        S3.upload_file(
            selector_df.to_dict(orient="records"),
            AZURE_CONST.S3_QUERY_SELECTOR_SAVE_PATH,
            'json',
            set_public_read=True
        )
        return True

    except Exception as e:
        print(f"query_selector failed. error: {e}")
        return False

# Submit Batch To Timestream
def submit_batch(records, counter, recursive):
    try:
        common_attrs = {'MeasureName': 'azure_values','MeasureValueType': 'MULTI'}
        TimestreamWrite.write_records(
            records=records,
            common_attrs=common_attrs,
            database_name=STORAGE_CONST.DATABASE_NAME,
            table_name=STORAGE_CONST.TABLE_NAME
        )

    except TimestreamWrite.client.exceptions.RejectedRecordsException as err:
        print(f"RejectedRecords Details: {err.response['RejectedRecords']}")
        re_records = []
        for rr in err.response["RejectedRecords"]:
            re_records.append(records[rr["RecordIndex"]])
        if recursive == 10:
            raise
        else:
            submit_batch(re_records, counter, recursive + 1)
    except Exception as err:
        raise


# Check Database And Table Are Exist and Upload Data to Timestream
def upload_timestream(data, time_datetime):
    Logger.info("Executing upload_timestream!")
    try:
        # Copy only selected columns to reduce memory
        data = data[["InstanceTier", "InstanceType", "Region", "OndemandPrice", "SpotPrice", "Savings", "IF",
            "DesiredCount", "AvailabilityZone", "Score", "Time", "T2", "T3"]].copy()

        fill_values = {
            "InstanceTier": 'N/A',
            "InstanceType": 'N/A',
            "Region": 'N/A',
            'OndemandPrice': -1,
            'Savings': -1,
            'SpotPrice': -1,
            'IF': -1,
            'DesiredCount': -1,
            'AvailabilityZone': 'N/A',
            'Score': 'N/A',
            'Time': 'N/A',
            'T2': 0,
            'T3': 0
        }
        data = data.fillna(fill_values)

        time_value = str(int(round(time_datetime.timestamp() * 1000)))

        # Prepare all records first
        all_records = []
        for idx, row in data.iterrows():
            dimensions = []
            for column in ['InstanceTier', 'InstanceType', 'Region', 'AvailabilityZone']:
                dimensions.append({'Name': column, 'Value': str(row[column])})

            submit_data = {
                'Dimensions': dimensions,
                'MeasureValues': [],
                'Time': time_value,
                'Version': int(time.time() * 1000)
            }

            measure_columns = [
                ('DesiredCount', 'DOUBLE'),
                ('OndemandPrice', 'DOUBLE'),
                ('SpotPrice', 'DOUBLE'),
                ('IF', 'DOUBLE'),
                ('Score', 'VARCHAR'),
                ('Time', 'VARCHAR'),
                ('T2', 'DOUBLE'),
                ('T3', 'DOUBLE')
            ]

            for column, value_type in measure_columns:
                submit_data['MeasureValues'].append({
                    'Name': column,
                    'Value': str(row[column]),
                    'Type': value_type
                })

            all_records.append(submit_data)

        # Split into batches of 100
        all_batches = []
        batch = []
        for record in all_records:
            batch.append(record)
            if len(batch) == 100:
                all_batches.append(batch)
                batch = []
        
        if batch:  # Add remaining records
            all_batches.append(batch)
        
        Logger.info(f"Uploading {len(all_records)} records in {len(all_batches)} batches using 10 threads")
        
        # Submit batches in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(submit_batch, batch, i, 0) for i, batch in enumerate(all_batches)]
            
            # Wait for all to complete and check for errors
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    Logger.error(f"Error submitting batch: {e}")
                    # Continue with other batches even if one fails
        
        Logger.info("Timestream upload completed")
        return True

    except Exception as e:
        Logger.error(f"upload_timestream failed. error: {e}")
        return False


def update_latest(all_data_dataframe):
    try:
        all_data_dataframe['id'] = all_data_dataframe.index + 1

        json_data = all_data_dataframe.to_dict(orient="records")

        json_path = f"{AZURE_CONST.S3_LATEST_JSON_SAVE_PATH}"
        pkl_gzip_path = f"{AZURE_CONST.S3_LATEST_ALL_DATA_AVAILABILITY_ZONE_TRUE_PKL_GZIP_SAVE_PATH}"

        # Parallel upload: json and pkl.gz
        def upload_json():
            S3.upload_file(json_data, json_path, "json", set_public_read=True)

        def upload_pkl_gz():
            S3.upload_file(all_data_dataframe, pkl_gzip_path, "pkl.gz", set_public_read=True)

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            futures = [
                executor.submit(upload_json),
                executor.submit(upload_pkl_gz)
            ]
            for future in concurrent.futures.as_completed(futures):
                future.result()  # Raise exception if any

        return True

    except Exception as e:
        print(f"update_latest failed. error: {e}")
        return False


def save_raw(all_data_dataframe, time_utc, az, data_type=None):
    try:
        s3_dir_name = time_utc.strftime("%Y/%m/%d")
        s3_obj_name = time_utc.strftime("%H-%M-%S")

        base_path = f"{AZURE_CONST.S3_RAW_DATA_PATH}"

        if data_type in ["desired_count_1", "multi", "specific"]:
            data_path = f"{base_path}/{s3_dir_name}/{s3_obj_name}.csv.gz"

        else:
            print(f"save_raw failed. error: no data_type.")
            return False

        # data 분석용
        S3.upload_file(all_data_dataframe, data_path, "df_to_csv.gz", set_public_read=True)

        return True

    except Exception as e:
        print(f"save_raw failed. error: {e}")
        return False
