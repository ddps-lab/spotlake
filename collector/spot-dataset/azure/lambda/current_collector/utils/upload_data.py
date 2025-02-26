# Upload collected data to Timestream or S3
import os
import json
import time
import boto3
import pickle
import pandas as pd
from datetime import datetime
from utils.pub_service import AZURE_CONST, STORAGE_CONST, CW, S3, TimestreamWrite

session = boto3.session.Session(region_name='us-west-2')

# Update latest_azure.json in S3
def update_latest_price_saving_if(data, time_datetime):
    data['id'] = data.index + 1
    data = data[['id', 'InstanceTier', 'InstanceType', 'Region', 'OndemandPrice', 'SpotPrice', 'Savings', 'IF']]
    data = data.copy()

    data['OndemandPrice'] = data['OndemandPrice'].fillna(-1)
    data['Savings'] = data['Savings'].fillna(-1)
    data['IF'] = data['IF'].fillna(-1)

    data['time'] = datetime.strftime(time_datetime, '%Y-%m-%d %H:%M:%S')

    data.to_json(f"{AZURE_CONST.SERVER_SAVE_DIR}/{AZURE_CONST.LATEST_PRICE_SAVING_IF_FILENAME}", orient='records')
    data.to_pickle(f"{AZURE_CONST.SERVER_SAVE_DIR}/{AZURE_CONST.LATEST_PRICE_SAVING_IF_PKL_GZIP_FILENAME}", compression="gzip")

    session = boto3.Session()
    s3 = session.client('s3')

    with open(f"{AZURE_CONST.SERVER_SAVE_DIR}/{AZURE_CONST.LATEST_PRICE_SAVING_IF_FILENAME}", 'rb') as f:
        s3.upload_fileobj(f, STORAGE_CONST.BUCKET_NAME, AZURE_CONST.S3_LATEST_PRICE_SAVING_IF_DATA_SAVE_PATH)

    with open(f"{AZURE_CONST.SERVER_SAVE_DIR}/{AZURE_CONST.LATEST_PRICE_SAVING_IF_PKL_GZIP_FILENAME}", 'rb') as f:
        s3.upload_fileobj(f, STORAGE_CONST.BUCKET_NAME, AZURE_CONST.S3_LATEST_PRICE_SAVING_IF_GZIP_SAVE_PATH)

    s3 = boto3.resource('s3')
    object_acl = s3.ObjectAcl(STORAGE_CONST.BUCKET_NAME, AZURE_CONST.S3_LATEST_PRICE_SAVING_IF_DATA_SAVE_PATH)
    response = object_acl.put(ACL='public-read')

    object_acl = s3.ObjectAcl(STORAGE_CONST.BUCKET_NAME, AZURE_CONST.S3_LATEST_PRICE_SAVING_IF_GZIP_SAVE_PATH)
    response = object_acl.put(ACL='public-read')

    pickle.dump(data, open(f"{AZURE_CONST.SERVER_SAVE_DIR}/{AZURE_CONST.SERVER_SAVE_FILENAME}", "wb"))


# Save raw data in S3
def save_raw_price_saving_if(data, time_datetime):
    data['Time'] = time_datetime.strftime("%Y-%m-%d %H:%M:%S")
    time_str = datetime.strftime(time_datetime, '%Y-%m-%d_%H-%M-%S')
    data = data[['Time','InstanceTier','InstanceType', 'Region', 'OndemandPrice','SpotPrice', 'IF', 'Savings']]

    data.to_csv(f"{AZURE_CONST.SERVER_SAVE_DIR}/{time_str}.csv.gz", index=False, compression="gzip")

    session = boto3.Session()
    s3 = session.client('s3')

    s3_dir_name = time_datetime.strftime("%Y/%m/%d")
    s3_obj_name = time_datetime.strftime("%H-%M-%S")

    with open(f"{AZURE_CONST.SERVER_SAVE_DIR}/{time_str}.csv.gz", 'rb') as f:
        s3.upload_fileobj(f, STORAGE_CONST.BUCKET_NAME, f"""{AZURE_CONST.S3_RAW_DATA_PATH}/if_saving_price/{s3_dir_name}/{s3_obj_name}.csv.gz""")
    os.remove(f"{AZURE_CONST.SERVER_SAVE_DIR}/{time_str}.csv.gz")

def upload_cloudwatch(data, time_datetime):
    try:
        ondemand_count = len(data.drop(columns=['IF', 'SpotPrice', 'Savings', 'Score']).dropna())
        spot_count = len(data.drop(columns=['IF', 'OndemandPrice', 'Savings', 'Score']).dropna())
        if_count = len(data.drop(columns=['OndemandPrice', 'SpotPrice', 'Savings', 'Score']).dropna())
        sps_count = len(data.drop(columns=['IF', 'OndemandPrice', 'SpotPrice', 'Savings']).dropna())

        log_event = [{
            'timestamp': int(time_datetime.timestamp()) * 1000,
            'message': f'AZUREONDEMAND: {ondemand_count} AZURESPOT: {spot_count} AZUREIF: {if_count} AZURESPS: {sps_count}'
        }]

        CW.client.put_log_events(
            log_group=AZURE_CONST.SPOT_DATA_COLLECTION_LOG_GROUP_NAME,
            log_stream=AZURE_CONST.LOG_STREAM_NAME,
            log_event=log_event
        )
        return True

    except Exception as e:
        print(f"upload_cloudwatch failed. error: {e}")
        return False

def query_selector(data):
    try:
        prev_query_selector_data = S3.read_file(AZURE_CONST.S3_QUERY_SELECTOR_SAVE_PATH, 'json')
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
    if recursive == 10:
        return
    try:
        common_attrs = {'MeasureName': 'azure_values','MeasureValueType': 'MULTI'}
        TimestreamWrite.client.write_records(
            DatabaseName='spotlake',
            TableName='azure',
            Records=records,
            CommonAttributes=common_attrs
        )

    except TimestreamWrite.client.exceptions.RejectedRecordsException as err:
        print(err)
        re_records = []
        for rr in err.response["RejectedRecords"]:
            re_records.append(records[rr["RecordIndex"]])
        submit_batch(re_records, counter, recursive + 1)
    except Exception as err:
        print(err)


# Check Database And Table Are Exist and Upload Data to Timestream
def upload_timestream(data, time_datetime):
    try:
        data = data.copy()
        data = data[["InstanceTier", "InstanceType", "Region", "OndemandPrice", "SpotPrice", "Savings", "IF",
            "DesiredCount", "AvailabilityZone", "Score", "SPS_Update_Time"]]

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
            'SPS_Update_Time': 'N/A'
        }
        data = data.fillna(fill_values)

        time_value = str(int(round(time_datetime.timestamp() * 1000)))

        records = []
        counter = 0
        for idx, row in data.iterrows():

            dimensions = []
            for column in ['InstanceTier', 'InstanceType', 'Region', 'AvailabilityZone']:
                dimensions.append({'Name': column, 'Value': str(row[column])})

            submit_data = {
                'Dimensions': dimensions,
                'MeasureValues': [],
                'Time': time_value
            }

            measure_columns = [
                ('DesiredCount', 'DOUBLE'),
                ('OndemandPrice', 'DOUBLE'),
                ('SpotPrice', 'DOUBLE'),
                ('IF', 'DOUBLE'),
                ('Score', 'VARCHAR'),
                ('SPS_Update_Time', 'VARCHAR')
            ]

            for column, value_type in measure_columns:
                submit_data['MeasureValues'].append({
                    'Name': column,
                    'Value': str(row[column]),
                    'Type': value_type
                })

            records.append(submit_data)
            counter += 1
            if len(records) == 100:
                submit_batch(records, counter, 0)
                records = []

        if len(records) != 0:
            submit_batch(records, counter, 0)
        return True

    except Exception as e:
        print(f"upload_timestream failed. error: {e}")
        return False


def update_latest(all_data_dataframe, availability_zones=True):
    try:
        all_data_dataframe['id'] = all_data_dataframe.index + 1

        dataframe_desired_count_1_df = all_data_dataframe[all_data_dataframe["DesiredCount"].isin([1, -1])].copy()
        dataframe_desired_count_1_df['id'] = dataframe_desired_count_1_df.index + 1
        desired_count_1_json_data = dataframe_desired_count_1_df.to_dict(orient="records")

        if availability_zones:
            desired_count_1_json_path = f"{AZURE_CONST.S3_LATEST_DESIRED_COUNT_1_DATA_AVAILABILITYZONE_TRUE_SAVE_PATH}"
            pkl_gzip_path = f"{AZURE_CONST.S3_LATEST_ALL_DATA_AVAILABILITY_ZONE_TRUE_PKL_GZIP_SAVE_PATH}"

            # FE 노출용 json, ["DesiredCount"].isin([1, -1])
            S3.upload_file(desired_count_1_json_data, desired_count_1_json_path, "json", set_public_read=True)
            # Full data pkl.gz, data 비교용
            S3.upload_file(all_data_dataframe, pkl_gzip_path, "pkl.gz", set_public_read=True)

        return True

    except Exception as e:
        print(f"update_latest failed. error: {e}")
        return False


def save_raw(all_data_dataframe, time_utc, availability_zones=True):
    try:
        dataframe_desired_count_1_df = all_data_dataframe[all_data_dataframe["DesiredCount"].isin([1, -1])].copy()

        s3_dir_name = time_utc.strftime("%Y/%m/%d")
        s3_obj_name = time_utc.strftime("%H-%M-%S")

        if availability_zones:
            all_data_path = f"{AZURE_CONST.S3_RAW_DATA_PATH}/all_data/availability-zones-true/{s3_dir_name}/{s3_obj_name}.csv.gz"
            # 기존 경로 유지
            desired_count_1_path = f"{AZURE_CONST.S3_RAW_DATA_PATH}/{s3_dir_name}/{s3_obj_name}.csv.gz"
            S3.upload_file(dataframe_desired_count_1_df, desired_count_1_path, "df_to_csv.gz", set_public_read=True)
        else:
            all_data_path = f"{AZURE_CONST.S3_RAW_DATA_PATH}/all_data/availability-zones-false/{s3_dir_name}/{s3_obj_name}.csv.gz"

        # data 분석용
        S3.upload_file(all_data_dataframe, all_data_path, "df_to_csv.gz", set_public_read=True)

        return True

    except Exception as e:
        print(f"save_raw failed. error: {e}")
        return False