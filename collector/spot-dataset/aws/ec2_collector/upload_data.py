import boto3
import time
import pandas as pd
import pickle
import sys
import os
import json
from botocore.config import Config
from botocore.exceptions import ClientError
from const_config import AwsCollector, Storage

sys.path.append('/home/ubuntu/spotlake/utility')

from slack_msg_sender import send_slack_message

STORAGE_CONST = Storage()
AWS_CONST = AwsCollector()

session = boto3.session.Session(region_name='us-west-2')
write_client = session.client('timestream-write', config=Config(read_timeout=20, max_pool_connections=5000, retries={'max_attempts':10}))

# Submit Batch To Timestream
def submit_batch(records, counter, recursive):
    if recursive == 10:
        return
    try:
        result = write_client.write_records(DatabaseName=STORAGE_CONST.DATABASE_NAME, TableName = STORAGE_CONST.AWS_TABLE_NAME, Records=records, CommonAttributes={})
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
    time_value = time.strptime(timestamp.strftime("%Y-%m-%d %H:%M"), '%Y-%m-%d %H:%M')
    time_value = time.mktime(time_value)
    time_value = str(int(round(time_value * 1000)))

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
                'Time': time_value
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
    filename = 'latest_aws.json'
    data['time'] = timestamp.strftime("%Y-%m-%d %H:%M:%S")
    data['id'] = data.index+1
    result = data.to_json(f"{AWS_CONST.LOCAL_PATH}/{filename}", orient="records")
    s3_path = f'latest_data/{filename}'
    session = boto3.Session()
    s3 = session.client('s3')
    with open(f"{AWS_CONST.LOCAL_PATH}/{filename}", 'rb') as f:
        s3.upload_fileobj(f, STORAGE_CONST.BUCKET_NAME, s3_path)
    s3 = session.resource('s3')
    object_acl = s3.ObjectAcl(STORAGE_CONST.BUCKET_NAME, s3_path)
    response = object_acl.put(ACL='public-read')


def update_query_selector(changed_df):
    filename = 'query-selector-aws.json'
    s3_path = f'query-selector/{filename}'
    session = boto3.Session()
    s3 = session.resource('s3')
    query_selector_aws = pd.DataFrame(json.loads(s3.Object(STORAGE_CONST.BUCKET_NAME, s3_path).get()['Body'].read()))
    query_selector_aws = pd.concat([query_selector_aws[['InstanceType', 'Region', 'AZ']], changed_df[['InstanceType', 'Region', 'AZ']]], axis=0, ignore_index=True).dropna().drop_duplicates(['InstanceType', 'Region', 'AZ']).reset_index(drop=True)
    result = query_selector_aws.to_json(f"{AWS_CONST.LOCAL_PATH}/{filename}", orient="records")
    s3 = session.client('s3')
    with open(f"{AWS_CONST.LOCAL_PATH}/{filename}", 'rb') as f:
        s3.upload_fileobj(f, STORAGE_CONST.BUCKET_NAME, s3_path)
    s3 = session.resource('s3')
    object_acl = s3.ObjectAcl(STORAGE_CONST.BUCKET_NAME, s3_path)
    response = object_acl.put(ACL='public-read')


def save_raw(data, timestamp):
    SAVE_FILENAME = f"{AWS_CONST.LOCAL_PATH}/spotlake_"+f"{timestamp}.csv.gz"
    data.to_csv(SAVE_FILENAME, index=False, compression="gzip")
    session = boto3.Session()
    s3 = session.client('s3')
    s3_dir_name = timestamp.strftime("%Y/%m/%d")
    s3_obj_name = timestamp.strftime("%H-%M-%S")

    with open(SAVE_FILENAME, 'rb') as f:
        s3.upload_fileobj(f, STORAGE_CONST.BUCKET_NAME, f"rawdata/aws/{s3_dir_name}/{s3_obj_name}.csv.gz")
    
    for filename in os.listdir(f"{AWS_CONST.LOCAL_PATH}/"):
        if "spotlake_" in filename:
            os.remove(f"{AWS_CONST.LOCAL_PATH}/{filename}")

