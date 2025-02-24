# Upload collected data to Timestream or S3
import os
import json
import time
import boto3
import pickle
import pandas as pd
from datetime import datetime
from botocore.config import Config
from utils.pub_service import send_slack_message, S3, AZURE_CONST, STORAGE_CONST

session = boto3.session.Session(region_name='us-west-2')
write_client = session.client('timestream-write',
                              config=Config(read_timeout=20,
                                max_pool_connections=5000,
                                retries={'max_attempts': 10})
                              )

# Submit Batch To Timestream
def submit_batch(records, counter, recursive):
    if recursive == 10:
        return
    try:
        result = write_client.write_records(DatabaseName=STORAGE_CONST.BUCKET_NAME, TableName=STORAGE_CONST.AZURE_TABLE_NAME, Records=records,CommonAttributes={})

    except write_client.exceptions.RejectedRecordsException as err:
        send_slack_message(err)
        print(err)
        re_records = []
        for rr in err.response["RejectedRecords"]:
            re_records.append(records[rr["RecordIndex"]])
        submit_batch(re_records, counter, recursive + 1)
        exit()
    except Exception as err:
        send_slack_message(err)
        print(err)
        exit()


# Check Database And Table Are Exist and Upload Data to Timestream
def upload_timestream(data, time_datetime):
    data = data[['InstanceTier', 'InstanceType', 'Region', 'OndemandPrice', 'SpotPrice', 'IF']]
    data = data.copy()

    data['OndemandPrice'] = data['OndemandPrice'].fillna(-1)
    data['SpotPrice'] = data['SpotPrice'].fillna(-1)
    data['IF'] = data['IF'].fillna(-1)

    time_value = time.mktime(time_datetime.timetuple())
    time_value = str(int(round(time_value * 1000)))

    records = []
    counter = 0
    for idx, row in data.iterrows():

        dimensions = []
        for column in ['InstanceTier', 'InstanceType', 'Region']:
            dimensions.append({'Name': column, 'Value': str(row[column])})

        submit_data = {
            'Dimensions': dimensions,
            'MeasureName': 'azure_values',
            'MeasureValues': [],
            'MeasureValueType': 'MULTI',
            'Time': time_value
        }

        for column, types in [('OndemandPrice', 'DOUBLE'), ('SpotPrice', 'DOUBLE'), ('IF', 'DOUBLE')]:
            submit_data['MeasureValues'].append({'Name': column, 'Value': str(row[column]), 'Type': types})

        records.append(submit_data)
        counter += 1
        if len(records) == 100:
            submit_batch(records, counter, 0)
            records = []

    if len(records) != 0:
        submit_batch(records, counter, 0)


# Update latest_azure.json in S3
def update_latest(data, time_datetime):
    data['id'] = data.index + 1
    data = data[['id', 'InstanceTier', 'InstanceType', 'Region', 'OndemandPrice', 'SpotPrice', 'Savings', 'IF']]
    data = data.copy()

    data['OndemandPrice'] = data['OndemandPrice'].fillna(-1)
    data['Savings'] = data['Savings'].fillna(-1)
    data['IF'] = data['IF'].fillna(-1)

    data['time'] = datetime.strftime(time_datetime, '%Y-%m-%d %H:%M:%S')

    data.to_json(f"{AZURE_CONST.SERVER_SAVE_DIR}/{AZURE_CONST.LATEST_FILENAME}", orient='records')
    data.to_pickle(f"{AZURE_CONST.SERVER_SAVE_DIR}/{AZURE_CONST.LATEST_PRICE_IF_PKL_GZIP_FILENAME}", compression="gzip")

    session = boto3.Session()
    s3 = session.client('s3')

    with open(f"{AZURE_CONST.SERVER_SAVE_DIR}/{AZURE_CONST.LATEST_FILENAME}", 'rb') as f:
        s3.upload_fileobj(f, STORAGE_CONST.BUCKET_NAME, AZURE_CONST.S3_LATEST_DATA_SAVE_PATH)

    with open(f"{AZURE_CONST.SERVER_SAVE_DIR}/{AZURE_CONST.LATEST_PRICE_IF_PKL_GZIP_FILENAME}", 'rb') as f:
        s3.upload_fileobj(f, STORAGE_CONST.BUCKET_NAME, AZURE_CONST.S3_LATEST_PRICE_IF_GZIP_SAVE_PATH)

    s3 = boto3.resource('s3')
    object_acl = s3.ObjectAcl(STORAGE_CONST.BUCKET_NAME, AZURE_CONST.S3_LATEST_DATA_SAVE_PATH)
    response = object_acl.put(ACL='public-read')

    object_acl = s3.ObjectAcl(STORAGE_CONST.BUCKET_NAME, AZURE_CONST.S3_LATEST_PRICE_IF_GZIP_SAVE_PATH)
    response = object_acl.put(ACL='public-read')

    pickle.dump(data, open(f"{AZURE_CONST.SERVER_SAVE_DIR}/{AZURE_CONST.SERVER_SAVE_FILENAME}", "wb"))


# Save raw data in S3
def save_raw(data, time_datetime):
    data['Time'] = time_datetime.strftime("%Y-%m-%d %H:%M:%S")
    time_str = datetime.strftime(time_datetime, '%Y-%m-%d_%H-%M-%S')
    data = data[['Time','InstanceTier','InstanceType', 'Region', 'OndemandPrice','SpotPrice', 'IF', 'Savings']]

    data.to_csv(f"{AZURE_CONST.SERVER_SAVE_DIR}/{time_str}.csv.gz", index=False, compression="gzip")

    session = boto3.Session()
    s3 = session.client('s3')

    s3_dir_name = time_datetime.strftime("%Y/%m/%d")
    s3_obj_name = time_datetime.strftime("%H-%M-%S")

    with open(f"{AZURE_CONST.SERVER_SAVE_DIR}/{time_str}.csv.gz", 'rb') as f:
        s3.upload_fileobj(f, STORAGE_CONST.BUCKET_NAME, f"""rawdata/azure/{s3_dir_name}/{s3_obj_name}.csv.gz""")
    os.remove(f"{AZURE_CONST.SERVER_SAVE_DIR}/{time_str}.csv.gz")


# Update query-selector-azure.json in S3
def query_selector(data):
    s3 = session.resource('s3')
    prev_selector_df = pd.DataFrame(json.loads(s3.Object(STORAGE_CONST.BUCKET_NAME, AZURE_CONST.S3_QUERY_SELECTOR_SAVE_PATH).get()['Body'].read()))
    selector_df = pd.concat([prev_selector_df[['InstanceTier', 'InstanceType', 'Region']], data[['InstanceTier', 'InstanceType', 'Region']]], axis=0, ignore_index=True).dropna().drop_duplicates(['InstanceTier', 'InstanceType', 'Region']).reset_index(drop=True)
    result = selector_df.to_json(f"{AZURE_CONST.SERVER_SAVE_DIR}/{AZURE_CONST.QUERY_SELECTOR_FILENAME}", orient='records')
    s3 = session.client('s3')
    with open(f"{AZURE_CONST.SERVER_SAVE_DIR}/{AZURE_CONST.QUERY_SELECTOR_FILENAME}", "rb") as f:
        s3.upload_fileobj(f, STORAGE_CONST.BUCKET_NAME, AZURE_CONST.S3_QUERY_SELECTOR_SAVE_PATH)
    os.remove(f"{AZURE_CONST.SERVER_SAVE_DIR}/{AZURE_CONST.QUERY_SELECTOR_FILENAME}")
    s3 = session.resource('s3')
    object_acl = s3.ObjectAcl(STORAGE_CONST.BUCKET_NAME, AZURE_CONST.S3_QUERY_SELECTOR_SAVE_PATH)
    response = object_acl.put(ACL='public-read')


def upload_cloudwatch(data, time_datetime):
    ondemand_count = len(data.drop(columns=['IF', 'SpotPrice', 'Savings']).dropna())
    spot_count = len(data.drop(columns=['IF', 'OndemandPrice', 'Savings']).dropna())
    if_count = len(data.drop(columns=['OndemandPrice', 'SpotPrice', 'Savings']).dropna())

    cw_client = boto3.client('logs')

    log_event = {
        'timestamp': int(time_datetime.timestamp()) * 1000,
        'message': f'AZUREONDEMAND: {ondemand_count} AZURESPOT: {spot_count} AZUREIF: {if_count}'
    }

    cw_client.put_log_events(
        logGroupName=AZURE_CONST.SPOT_DATA_COLLECTION_LOG_GROUP_NAME,
        logStreamName=AZURE_CONST.LOG_STREAM_NAME,
        logEvents=[log_event]
    )


def update_latest_sps(dataframe, availability_zones=True):
    try:
        if availability_zones:
            path = f"{AZURE_CONST.LATEST_SPS_FILENAME}"
        else:
            path = f"{AZURE_CONST.LATEST_SPS_AVAILABILITY_ZONE_FALSE_FILENAME}"

        json_data = dataframe.to_dict(orient="records")
        S3.upload_file(json_data, path, "json", set_public_read=True)
        return True

    except Exception as e:
        print(f"update_latest_sps failed. error: {e}")
        return False


def save_raw_sps(dataframe, time_utc, availability_zones=True):
    try:
        s3_dir_name = time_utc.strftime("%Y/%m/%d")
        s3_obj_name = time_utc.strftime("%H-%M-%S")

        if availability_zones:
            path = f"{AZURE_CONST.LATEST_SPS_RAW_DATA_PATH}/availability-zones-true/{s3_dir_name}/{s3_obj_name}.csv.gz"
        else:
            path = f"{AZURE_CONST.LATEST_SPS_RAW_DATA_PATH}/availability-zones-false/{s3_dir_name}/{s3_obj_name}.csv.gz"

        S3.upload_file(dataframe, path, "df_to_csv.gz", set_public_read=True)
        return True

    except Exception as e:
        print(f"save_raw_sps failed. error: {e}")
        return False