import time
import boto3
import pandas as pd
from botocore.config import Config
from botocore.exceptions import ClientError


PROFILE_NAME = 'default'
REGION_NAME = 'us-east-2'
DATABASE_NAME = 'dbname'
TABLE_NAME = 'tablename'


# Submit Batch To Timestream
def submit_batch(records, counter, recursive, write_client):
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
def upload_timestream(data):
    session = boto3.Session(profile_name=PROFILE_NAME, region_name=REGION_NAME)
    write_client = session.client('timestream-write', config=Config(read_timeout=20, max_pool_connections=5000, retries={'max_attempts':10}))

    records = []
    counter = 0
    for idx, row in data.iterrows():
        time_value = str(row['time']).split('+')[0]
        time_value = time.strptime(time_value, '%Y-%m-%d %H:%M:%S')
        time_value = time.mktime(time_value)
        time_value = str(int(round(time_value * 1000)))

        dimensions = []
        for column in data.columns:
            if column in ['InstanceType', 'Region', 'AZ', 'Ceased']:
                dimensions.append({'Name':column, 'Value': str(row[column])})

        measures = []
        for column, types in [('SPS', 'BIGINT'), ('IF', 'DOUBLE'), ('SpotPrice', 'DOUBLE')]:
            measures.append({'Name': column, 'Value': str(row[column]), 'Type': types})
            
        submit_data = {
                'Dimensions': dimensions,
                'MeasureName': 'aws_values',
                'MeasureValues': measures,
                'MeasureValueType': 'MULTI',
                'Time': time_value
        }
        
        records.append(submit_data)
        counter += 1
        if len(records) == 100:
            submit_batch(records, counter, 0, write_client)
            records = []

    if len(records) != 0:
        submit_batch(records, counter, 0, write_client)
