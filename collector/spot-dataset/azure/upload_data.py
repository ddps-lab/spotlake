import os
import time
import boto3
import pickle
import pandas as pd
from datetime import datetime
from botocore.config import Config
from botocore.exceptions import ClientError
from utility import slack_msg_sender


session = boto3.session.Session(region_name='us-west-2')
write_client = session.client('timestream-write',config=Config(read_timeout=20, max_pool_connections=5000, retries={'max_attempts': 10}))
SAVE_DIR = '/home/ubuntu/spot-score/collection/azure'
BUCKET_NAME = 'spotlake'
DATABASE_NAME = 'spotlake'
TABLE_NAME = 'azure'
FILENAME = 'latest_azure.json'
S3_PATH = f'latest_data/{FILENAME}'


# Submit Batch To Timestream
def submit_batch(records, counter, recursive):
    if recursive == 10:
        return
    try:
        result = write_client.write_records(DatabaseName=DATABASE_NAME, TableName=TABLE_NAME, Records=records,CommonAttributes={})

    except write_client.exceptions.RejectedRecordsException as err:
        slack_msg_sender.send_slack_message(err)
        print(err)
        re_records = []
        for rr in err.response["RejectedRecords"]:
            re_records.append(records[rr["RecordIndex"]])
        submit_batch(re_records, counter, recursive + 1)
        exit()
    except Exception as err:
        slack_msg_sender.send_slack_message(err)
        print(err)
        exit()


# Check Database And Table Are Exist and Upload Data to Timestream
def upload_timestream(data, timestamp):
    data = data[['InstanceTier', 'InstanceType', 'Region', 'OndemandPrice', 'SpotPrice']]

    time_value = time.strptime(timestamp.strftime("%Y-%m-%d %H:%M"), '%Y-%m-%d %H:%M')
    time_value = time.mktime(time_value)
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

        for column, types in [('OndemandPrice', 'DOUBLE'), ('SpotPrice', 'DOUBLE')]:
            submit_data['MeasureValues'].append({'Name': column, 'Value': str(row[column]), 'Type': types})

        records.append(submit_data)
        counter += 1
        if len(records) == 100:
            submit_batch(records, counter, 0)
            records = []

    if len(records) != 0:
        submit_batch(records, counter, 0)


def update_latest(data, timestamp):
    data['id'] = data.index + 1
    data = data[['id', 'InstanceTier', 'InstanceType', 'Region', 'OndemandPrice', 'SpotPrice', 'Savings']]
    data['OndemandPrice'] = data['OndemandPrice'].fillna(-1)
    data['Savings'] = data['Savings'].fillna(-1)
    data['time'] = datetime.strftime(timestamp, '%Y-%m-%d %H:%M:%S')

    result = data.to_json(f"{SAVE_DIR}/{FILENAME}", orient='records')

    session = boto3.Session()
    s3 = session.client('s3')

    with open(f"{SAVE_DIR}/{FILENAME}", 'rb') as f:
        s3.upload_fileobj(f, BUCKET_NAME, S3_PATH)

    s3 = boto3.resource('s3')
    object_acl = s3.ObjectAcl(BUCKET_NAME, S3_PATH)
    response = object_acl.put(ACL='public-read')

    pickle.dump(data, open(f"{SAVE_DIR}/latest_azure_df.pkl", "wb"))


def save_raw(data, timestamp):
    data.to_csv(f"{SAVE_DIR}/{timestamp}.csv.gz", index=False, compression="gzip")

    session = boto3.Session()
    s3 = session.client('s3')

    s3_dir_name = timestamp.strftime("%Y/%m/%d")
    s3_obj_name = timestamp.strftime("%H:%M:%S")

    with open(f"{SAVE_DIR}/{timestamp}.csv.gz", 'rb') as f:
        s3.upload_fileobj(f, BUCKET_NAME, f"rawdata/{s3_dir_name}/{s3_obj_name}.csv.gz")
    os.remove(f"{SAVE_DIR}/{timestamp}.csv.gz")
