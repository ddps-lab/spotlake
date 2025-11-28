# ------ import module ------
import boto3
import pandas as pd
import os
import json
from botocore.config import Config

# ------ import user module ------
import sys
# sys.path.append("/home/ubuntu/spotlake")
# from const_config import AwsCollector, Storage
from utility.utils import get_region

BUCKET_NAME = "spotlake"
S3_PATH_PREFIX = "rawdata/aws"

DATABASE_NAME = "spotlake"
AWS_TABLE_NAME = "aws"

write_client = boto3.client('timestream-write', region_name=get_region(),
                            config=Config(read_timeout=20, max_pool_connections=5000, retries={'max_attempts': 10}))

# Submit Batch To Timestream


def submit_batch(records, counter, recursive):
    if recursive == 10:
        return
    try:
        result = write_client.write_records(DatabaseName=DATABASE_NAME, TableName=AWS_TABLE_NAME,
                                            Records=records, CommonAttributes={})
    except write_client.exceptions.RejectedRecordsException as err:
        re_records = []
        for rr in err.response["RejectedRecords"]:
            send_slack_message(rr['Reason'])
            print(rr['Reason'])
            re_records.append(records[rr["RecordIndex"]])
        submit_batch(re_records, counter, recursive + 1)
    except Exception as err:
        send_slack_message(err)
        print(err)
        exit()


# Check Database And Table Are Exist and Upload Data to Timestream
def upload_timestream(data, timestamp):
    data = data.dropna(axis=0)
    time_value = str(int(timestamp.timestamp() * 1000))

    records = []
    counter = 0
    for idx, row in data.iterrows():
        dimensions = []
        for column in data.columns:
            if column in ['InstanceType', 'Region', 'AZ', 'OndemandPrice', 'Ceased']:
                dimensions.append({'Name': column, 'Value': str(row[column])})
        submit_data = {
            'Dimensions': dimensions,
            'MeasureName': 'aws_values',
            'MeasureValues': [],
            'MeasureValueType': 'MULTI',
            'Time': time_value
        }
        for column, types in [('SPS', 'BIGINT'), ('T3', 'BIGINT'), ('T2', 'BIGINT'), ('IF', 'DOUBLE'), ('SpotPrice', 'DOUBLE')]:
            submit_data['MeasureValues'].append({'Name': column, 'Value': str(row[column]), 'Type': types})

        records.append(submit_data)
        counter += 1
        if len(records) == 100:
            submit_batch(records, counter, 0)
            records = []

    if len(records) != 0:
        submit_batch(records, counter, 0)


def update_latest(data, timestamp):
    # Upload file to use as previous collection data
    filename = 'latest_aws.json'
    LATEST_PATH = f'latest_data/{filename}'

    data['id'] = data.index+1
    data['time'] = timestamp.strftime("%Y-%m-%d %H:%M:%S")
    result = data.to_json(f"/tmp/{filename}", orient="records")

    s3 = boto3.resource('s3')
    s3_client = boto3.client('s3')

    with open(f"/tmp/{filename}", 'rb') as f:
        s3_client.upload_fileobj(f, BUCKET_NAME, LATEST_PATH, ExtraArgs={'ContentType': 'application/json'})
    object_acl = s3.ObjectAcl(BUCKET_NAME, LATEST_PATH)
    response = object_acl.put(ACL='public-read')

    data.drop(['id'], axis=1, inplace=True)


def update_query_selector(changed_df):
    filename = 'query-selector-aws.json'
    s3_path = f'query-selector/{filename}'
    s3 = boto3.resource('s3')
    try:
        query_selector_aws = pd.DataFrame(json.loads(s3.Object(BUCKET_NAME, s3_path).get()['Body'].read()))
        query_selector_aws = pd.concat([query_selector_aws[['InstanceType', 'Region', 'AZ']], changed_df[['InstanceType', 'Region', 'AZ']]],
                                       axis=0, ignore_index=True).dropna().drop_duplicates(['InstanceType', 'Region', 'AZ']).reset_index(drop=True)
    except Exception as e:
        print(f"Failed to load existing query selector, creating new one: {e}")
        query_selector_aws = changed_df[['InstanceType', 'Region', 'AZ']].dropna().drop_duplicates().reset_index(drop=True)

    result = query_selector_aws.to_json(f"/tmp/{filename}", orient="records")
    s3_client = boto3.client('s3')
    with open(f"/tmp/{filename}", 'rb') as f:
        s3_client.upload_fileobj(f, BUCKET_NAME, s3_path, ExtraArgs={'ContentType': 'application/json'})
    s3 = boto3.resource('s3')
    object_acl = s3.ObjectAcl(BUCKET_NAME, s3_path)
    response = object_acl.put(ACL='public-read')


def save_raw(data, timestamp):
    s3_dir_name = timestamp.strftime("%Y/%m/%d")
    s3_obj_name = timestamp.strftime("%H-%M-%S")

    rawdata = data[['Time', 'InstanceType', 'Region', 'AZ', 'SPS', 'T3', 'T2', 'IF', 'OndemandPrice', 'SpotPrice', 'Savings']]
    SAVE_FILENAME = f"/tmp/{s3_obj_name}.csv.gz"
    rawdata.to_csv(SAVE_FILENAME, index=False, compression="gzip")

    s3 = boto3.client('s3')

    with open(SAVE_FILENAME, 'rb') as f:
        s3.upload_fileobj(f, BUCKET_NAME, f"{S3_PATH_PREFIX}/{s3_dir_name}/{s3_obj_name}.csv.gz")


def update_config(config_path, text, target_capacity, target_capacities):
    s3_client = boto3.client('s3')
    with open(f"/tmp/{config_path}", "w") as file:
        for i, line in enumerate(text):
            if i == 0:
                file.write(f"{(target_capacity + 1) % len(target_capacities)}\n")
            else:
                file.write(f"{line}\n")
    s3_client.upload_file(f"/tmp/{config_path}", BUCKET_NAME, f"config/{config_path}")
