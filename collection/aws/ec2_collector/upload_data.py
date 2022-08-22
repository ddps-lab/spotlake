import boto3
import time
import pandas as pd
import pickle
from botocore.config import Config
from botocore.exceptions import ClientError


session = boto3.session.Session(region_name='us-west-2')
write_client = session.client('timestream-write', config=Config(read_timeout=20, max_pool_connections=5000, retries={'max_attempts':10}))

BUCKET_NAME = 'spotlake-test'
DATABASE_NAME = 'spotlake-timestream'
TABLE_NAME = 'aws-table'


# Submit Batch To Timestream
def submit_batch(records, counter, recursive):
    if recursive == 10:
        return
    try:
        result = write_client.write_records(DatabaseName=DATABASE_NAME, TableName = TABLE_NAME, Records=records, CommonAttributes={})
    except write_client.exceptions.RejectedRecordsException as err:
        print(err)
        re_records = []
        for rr in err.response["RejectedRecords"]:
            re_records.append(records[rr["RecordIndex"]])
        submit_batch(re_records, counter, recursive + 1)
    except Exception as err:
        print(err)
        exit()


# Check Database And Table Are Exist and Upload Data to Timestream
def upload_timestream(data, timestamp):
    data = data[['InstanceType', 'Region', 'AvailabilityZoneId', 'SPS', 'IF', 'SpotPrice', 'Savings', 'OndemandPrice']]
    data = data.rename({'AvailabilityZoneId': 'AZ'}, axis=1)

    time_value = time.strptime(timestamp.strftime("%Y-%m-%d %H:%M"), '%Y-%m-%d %H:%M')
    time_value = time.mktime(time_value)
    time_value = str(int(round(time_value * 1000)))

    records = []
    counter = 0
    for idx, row in data.iterrows():

        dimensions = []
        for column in ['InstanceType', 'Region', 'AZ']:
            dimensions.append({'Name':column, 'Value': str(row[column])})

        submit_data = {
                'Dimensions': dimensions,
                'MeasureName': 'aws_values',
                'MeasureValues': [],
                'MeasureValueType': 'MULTI',
                'Time': time_value
        }
        for column, types in [('SPS', 'BIGINT'), ('IF', 'DOUBLE'), ('SpotPrice', 'DOUBLE'), ('OndemandPrice', 'DOUBLE')]:
            submit_data['MeasureValues'].append({'Name': column, 'Value': str(row[column]), 'Type' : types})
        records.append(submit_data)
        counter += 1
        if len(records) == 100:
            submit_batch(records, counter, 0)
            records = []

    if len(records) != 0:
        submit_batch(records, counter, 0)


def update_latest(data):
    filename = '/home/ubuntu/spot-score/collection/aws/ec2_collector/latest_spot_data.json'
    result = data.to_json(filename)
    s3_path = f'latest_data/{filename}'
    session = boto3.Session()
    s3 = session.client('s3')
    with open(filename, 'rb') as f:
        s3.upload_fileobj(f, BUCKET_NAME, s3_path)
    pickle.dump(data, open("/home/ubuntu/spot-score/collection/aws/ec2_collector/latest_df.pkl", "wb"))

def save_raw(data, timestamp):
    SAVE_FILENAME = "/home/ubuntu/spot-score/collection/aws/ec2_collector/spotlake_"+"timestamp"
    data.to_csv(SAVE_FILENAME, index=False, compression="gzip")
    session = boto3.Session()
    s3 = session.client('s3')
    s3_dir_name = timestamp.strftime("%Y/%m/%d")
    s3_obj_name = timestamp.strftime("%H:%M:%S")

    with open(SAVE_FILENAME, 'rb') as f:
        s3.upload_fileobj(f, BUCKET_NAME, f"rawdata/{s3_dir_name}/{s3_obj_name}.csv.gz")
