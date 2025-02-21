# ------ import module ------
import boto3
import pandas as pd
import os
import json
from botocore.config import Config

# ------ import user module ------
from slack_msg_sender import send_slack_message

BUCKET_NAME = "spotlake"
BUCKET_FILE_PATH = "rawdata/aws"

DATABASE_NAME = "spotlake"
AWS_TABLE_NAME = "aws"
write_client = boto3.client('timestream-write', config=Config(read_timeout=20, max_pool_connections=5000, retries={'max_attempts':10}))

# Submit Batch To Timestream
def submit_batch(records, counter, recursive):
    if recursive == 10:
        return
    try:
        result = write_client.write_records(DatabaseName=DATABASE_NAME, TableName = AWS_TABLE_NAME, Records=records, CommonAttributes={})
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
    time_value = str(int(timestamp.timestamp() * 1000))

    records = []
    counter = 0
    for idx, row in data.iterrows():
        dimensions = []
        for column in data.columns:
            if column in ['InstanceType', 'Region', 'AZ', 'OndemandPrice', 'Ceased']:
                dimensions.append({'Name':column, 'Value': str(row[column])})
        submit_data = {
                'Dimensions': dimensions,
                'MeasureName': 'aws_values',
                'MeasureValues': [],
                'MeasureValueType': 'MULTI',
                'Time': time_value,
                'version': 2
        }
        for column, types in [('SPS', 'BIGINT'), ('IF', 'DOUBLE'), ('SpotPrice', 'DOUBLE')]:
            submit_data['MeasureValues'].append({'Name': column, 'Value': str(row[column]), 'Type' : types})
        
        records.append(submit_data)
        counter += 1
        if len(records) == 100:
            submit_batch(records, counter, 0)
            records = []

    if len(records) != 0:
        submit_batch(records, counter, 0)


def update_latest(data, timestamp):
    # Upload file to use as previous collection data
    filename = './utility/manual_merge_aws_rawdata/latest_aws.json'

    data['id'] = data.index+1
    data['time'] = timestamp.strftime("%Y-%m-%d %H:%M:%S")
    result = data.to_json(f"filename", orient="records")
    
    data.drop(['id'], axis=1, inplace=True)


def update_query_selector(changed_df):
    filename = 'query-selector-aws.json'
    s3_path = f'query-selector/{filename}'
    s3 = boto3.resource('s3')
    query_selector_aws = pd.DataFrame(json.loads(s3.Object(BUCKET_NAME, s3_path).get()['Body'].read()))
    query_selector_aws = pd.concat([query_selector_aws[['InstanceType', 'Region', 'AZ']], changed_df[['InstanceType', 'Region', 'AZ']]], axis=0, ignore_index=True).dropna().drop_duplicates(['InstanceType', 'Region', 'AZ']).reset_index(drop=True)
    result = query_selector_aws.to_json(f"/tmp/{filename}", orient="records")
    s3 = boto3.client('s3')
    with open(f"/tmp/{filename}", 'rb') as f:
        s3.upload_fileobj(f, BUCKET_NAME, s3_path)
    s3 = boto3.resource('s3')
    object_acl = s3.ObjectAcl(BUCKET_NAME, s3_path)
    response = object_acl.put(ACL='public-read')


def save_raw(data, timestamp):
    s3_dir_name = timestamp.strftime("%Y/%m/%d")
    s3_obj_name = timestamp.strftime("%H-%M-%S")

    rawdata = data[['Time', 'InstanceType', 'Region', 'AZ', 'SPS', 'IF', 'OndemandPrice', 'SpotPrice', 'Savings']]
    SAVE_FILENAME = f"/tmp/{s3_obj_name}.csv.gz"
    rawdata.to_csv(SAVE_FILENAME, index=False, compression="gzip")
    
    s3 = boto3.client('s3')

    with open(SAVE_FILENAME, 'rb') as f:
        s3.upload_fileobj(f, BUCKET_NAME, f"{BUCKET_FILE_PATH}/{s3_dir_name}/{s3_obj_name}.csv.gz")


def update_config(config_path, text, target_capacity, target_capacities):
    s3_client = boto3.client('s3')
    with open(f"/tmp/{config_path}", "w") as file:
        for i, line in enumerate(text):
            if i == 0:
                file.write(f"{(target_capacity + 1) % len(target_capacities)}\n")
            else:
                file.write(f"{line}\n")
    s3_client.upload_file(f"/tmp/{config_path}", BUCKET_NAME, f"config/{config_path}")
