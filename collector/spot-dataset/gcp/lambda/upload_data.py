import boto3
import os
from botocore.config import Config
import time
from datetime import datetime
from botocore.exceptions import ClientError
import pandas as pd
import json
from const_config import Storage, GcpCollector
from utility import slack_msg_sender

session = boto3.session.Session(region_name='us-west-2')
write_client = session.client('timestream-write',
                              config=Config(read_timeout=20, max_pool_connections=5000, retries={'max_attempts': 10}))

STORAGE_CONST = Storage()
LOCAL_PATH = GcpCollector.LOCAL_PATH

# Submit Batch To Timestream
def submit_batch(records, counter, recursive):
    if recursive == 10:
        return
    try:
        result = write_client.write_records(DatabaseName=STORAGE_CONST.DATABASE_NAME,
                                            TableName=STORAGE_CONST.GCP_TABLE_NAME, Records=records,
                                            CommonAttributes={})
    except write_client.exceptions.RejectedRecordsException as err:
        re_records = []
        for rr in err.response["RejectedRecords"]:
            slack_msg_sender.send_slack_message({rr['Reason']})
            print(rr['Reason'])
            re_records.append(records[rr["RecordIndex"]])
        submit_batch(re_records, counter, recursive + 1)
    except Exception as err:
        slack_msg_sender.send_slack_message(err)
        print(err)
        exit()


# Check Database And Table Are Exist and Upload Data to Timestream
def upload_timestream(data, timestamp):
    print(len(data))

    time_value = time.strptime(timestamp.strftime("%Y-%m-%d %H:%M"), '%Y-%m-%d %H:%M')
    time_value = time.mktime(time_value)
    time_value = str(int(round(time_value * 1000)))

    records = []
    counter = 0
    for idx, row in data.iterrows():

        dimensions = []
        for column in data.columns:
            if column in ['InstanceType', 'Region', 'Ceased']:
                dimensions.append({'Name': column, 'Value': str(row[column])})

        submit_data = {
            'Dimensions': dimensions,
            'MeasureName': 'gcp_values',
            'MeasureValues': [],
            'MeasureValueType': 'MULTI',
            'Time': time_value
        }
        for column, types in [('OnDemand Price', 'DOUBLE'), ('Spot Price', 'DOUBLE')]:
            submit_data['MeasureValues'].append({'Name': column, 'Value': str(row[column]), 'Type': types})
        records.append(submit_data)
        counter += 1
        if len(records) == 100:
            submit_batch(records, counter, 0)
            records = []

    if len(records) != 0:
        submit_batch(records, counter, 0)

    print(f"end : {counter}")


def update_latest(data, timestamp):
    filename = 'latest_gcp.json'
    data.reset_index(drop=True, inplace=True)
    data = data.replace(-0, -1)
    data['id'] = data.index + 1
    data = pd.DataFrame(data, columns=['id', 'InstanceType', 'Region', 'OnDemand Price',
                                       'Spot Price', 'Savings'])
    data['time'] = datetime.strftime(timestamp, '%Y-%m-%d %H:%M:%S')

    data_dict = data.to_dict(orient='records')
    with open(f'{LOCAL_PATH}/{filename}', 'w') as f:
        json.dump(data_dict, f)

    s3_path = f'latest_data/{filename}'
    session = boto3.Session()
    s3 = session.client('s3')
    with open(f"{LOCAL_PATH}/{filename}", 'rb') as f:
        s3.upload_fileobj(f, STORAGE_CONST.BUCKET_NAME, s3_path)

    ## temporary blocking of access
    s3 = session.resource('s3')
    object_acl = s3.ObjectAcl(STORAGE_CONST.BUCKET_NAME, s3_path)
    response = object_acl.put(ACL='public-read')


def save_raw(data, timestamp):
    SAVE_FILENAME = f"{LOCAL_PATH}/spotlake_" + f"{timestamp}.csv.gz"
    data['Savings'] = round(
        (data['OnDemand Price'] - data['Spot Price']) / data[
            'OnDemand Price'] * 100)
    data.to_csv(SAVE_FILENAME, index=False, compression='gzip')
    session = boto3.Session()
    s3 = session.client('s3')
    s3_dir_name = timestamp.strftime("%Y/%m/%d")
    s3_obj_name = timestamp.strftime("%H-%M-%S")
    with open(SAVE_FILENAME, 'rb') as f:
        s3.upload_fileobj(
            f, STORAGE_CONST.BUCKET_NAME, f"rawdata/gcp/{s3_dir_name}/{s3_obj_name}.csv.gz")

    for filename in os.listdir(f"{LOCAL_PATH}/"):
        if "spotlake_" in filename:
            os.remove(f"{LOCAL_PATH}/{filename}")


def upload_metadata(filename):
    session = boto3.Session()
    s3 = session.client('s3')
    with open(f'{LOCAL_PATH}/{filename}.json', 'rb') as f:
        s3.upload_fileobj(
            f, STORAGE_CONST.BUCKET_NAME, f'gcp_metadata/{filename}.json')


def load_metadata(filename):
    obj_file = f'gcp_metadata/{filename}.json'
    save_file = f'tmp/{filename}.json'
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(STORAGE_CONST.BUCKET_NAME)
    bucket.download_file(obj_file, save_file)
    
    data = None
    with open(save_file, 'r') as f:
        data = json.load(f)
        
    return data
