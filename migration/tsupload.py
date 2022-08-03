import boto3
import time
import pandas as pd
from botocore.config import Config
from botocore.exceptions import ClientError


DATABASE_NAME = 'spotlake'
TABLE_NAME = 'aws'


# Submit Batch To Timestream
def submit_batch(records, counter, recursive, write_client):
    if recursive == 10:
        return
    try:
        result = write_client.write_records(DatabaseName=DATABASE_NAME, TableName = TABLE_NAME, Records=records, CommonAttributes={})
    except write_client.exceptions.RejectedRecordsException as err:
        re_records = []
        for rr in err.response["RejectedRecords"]:
            re_records.append(records[rr["RecordIndex"]])
        submit_batch(re_records, counter, recursive + 1)
    except Exception as err:
        exit()


# Check Database And Table Are Exist and Upload Data to Timestream
def upload_timestream(data, profile_name):
    session = boto3.Session(profile_name=profile_name, region_name='us-west-2')
    write_client = session.client('timestream-write', config=Config(read_timeout=20, max_pool_connections=5000, retries={'max_attempts':10}))

    data = data[['InstanceType', 'Region', 'AZ', 'SPS', 'IF', 'SpotPrice', 'Savings', 'time']]

    records = []
    counter = 0
    for idx, row in data.iterrows():
        time_value = str(row['time']).split('+')[0]
        time_value = time.strptime(time_value, '%Y-%m-%d %H:%M:%S')
        time_value = time.mktime(time_value)
        time_value = str(int(round(time_value * 1000)))

        dimensions = []
        for column in ['InstanceType', 'Region', 'AZ', 'SPS', 'IF', 'SpotPrice', 'Savings']:
            dimensions.append({'Name':column, 'Value': str(row[column])})

        submit_data = {
                'Dimensions': dimensions,
                'MeasureName': 'SpotPrice',
                'MeasureValue': str(row['SpotPrice']),
                'MeasureValueType': 'DOUBLE',
                'Time': time_value
        }
        records.append(submit_data)
        counter += 1
        if len(records) == 100:
            submit_batch(records, counter, 0, write_client)
            records = []

    if len(records) != 0:
        submit_batch(records, counter, 0, write_client)
