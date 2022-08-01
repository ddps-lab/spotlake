import boto3
import time
import pandas as pd
from botocore.config import Config
from botocore.exceptions import ClientError


session = boto3.session.Session(region_name='us-west-2')
write_client = session.client('timestream-write', config=Config(read_timeout=20, max_pool_connections=5000, retries={'max_attempts':10}))

DATABASE_NAME = 'spotlake'
TABLE_NAME = 'aws'


# Submit Batch To Timestream
def submit_batch(records, counter, recursive):
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
def upload_timestream(data):
    data = data[['InstanceType', 'Region', 'AvailabilityZoneId', 'SPS', 'IF', 'SpotPrice', 'Savings', 'TimeStamp_spotinfo']]
    data = data.rename({'AvailabilityZoneId': 'AZ', 'TimeStamp_spotinfo': 'TimeStamp'}, axis=1)

    records = []
    counter = 0
    for idx, row in data.iterrows():
        time_value = str(row['TimeStamp']).split('+')[0]
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
            submit_batch(records, counter, 0)
            records = []

    if len(records) != 0:
        submit_batch(records, counter, 0)


def update_latest(data):
    data_to_json = "["
    id_count = 0
    for idx, row in data.iterrows():
        id_count += 1
        data_to_json += '{'
        data_to_json += '\"id\":\"'+str(id_count)+'\",'
        data_to_json += '\"SpotPrice\":\"'+str(row['SpotPrice'])+'\",'
        data_to_json += '\"Savings\":\"'+str(row['Savings'])+'\",'
        data_to_json += '\"SPS\":\"'+str(row['SPS'])+'\",'
        data_to_json += '\"AZ\":\"'+str(row['AvailabilityZoneId'].split('-az')[1])+'\",'
        data_to_json += '\"Region\":\"'+str(row['Region'])+'\",'
        data_to_json += '\"InstanceType\":\"'+str(row['InstanceType'])+'\",'
        save_latest_if = 0
        if row['IF'] == '<5%':
            save_latest_if = 3.0
        elif row['IF'] == '5-10%':
            save_latest_if = 2.5
        elif row['IF'] == '10-15%':
            save_latest_if = 2.0
        elif row['IF'] == '15-20%':
            save_latest_if = 1.5
        else:
            save_latest_if = 1.0
        data_to_json += '\"IF\":\"'+str(save_latest_if)+'\",'
        data_to_json += '\"time\":\"'+str(row['TimeStamp_spotinfo'].split('+')[0])+'\"}'
        data_to_json += ','

    if data_to_json[-1] == ',':
        data_to_json = data_to_json[:len(data_to_json)-1] + ']'
    elif data_to_json[-1] == '[':
        data_to_json += ']'
    result = json.dumps(data_to_json)
    filename = 'latest_spot_data.json'
    s3_path = f'latest_data/{filename}'
    s3.Object(SAVE_BUCKET_NAME, s3_path).put(Body=result)
    object_acl = s3.ObjectAcl(SAVE_BUCKET_NAME, s3_path)
    response = object_acl.put(ACL='public-read')
