import boto3
import os
from botocore.config import Config
import time
from botocore.exceptions import ClientError

session = boto3.session.Session(region_name='us-west-2')
write_client = session.client('timestream-write', config=Config(read_timeout=20, max_pool_connections=5000, retries={'max_attempts':10}))


BUCKET_NAME = 'spotlake'
DATABASE_NAME = 'spotlake'
TABLE_NAME = 'gcp'
LOCAL_PATH = '/tmp'


# Submit Batch To Timestream
def submit_batch(records, counter, recursive):
    if recursive == 10:
        return
    try:
        result = write_client.write_records(DatabaseName=DATABASE_NAME, TableName = TABLE_NAME, Records=records, CommonAttributes={})
    except write_client.exceptions.RejectedRecordsException as err:
        re_records = []
        for rr in err.response["RejectedRecords"]:
            print(rr['Reason'])
            re_records.append(records[rr["RecordIndex"]])
        submit_batch(re_records, counter, recursive + 1)
    except Exception as err:
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
                dimensions.append({'Name':column, 'Value': str(row[column])})

        submit_data = {
                'Dimensions': dimensions,
                'MeasureName': 'gcp_values',
                'MeasureValues': [],
                'MeasureValueType': 'MULTI',
                'Time': time_value
        }
        for column, types in [('Calculator OnDemand Price', 'DOUBLE'), ('Calculator Preemptible Price', 'DOUBLE'), ('VM Instance OnDemand Price', 'DOUBLE'), ('VM Instance Preemptible Price', 'DOUBLE')]:
            submit_data['MeasureValues'].append({'Name': column, 'Value': str(row[column]), 'Type' : types})
        records.append(submit_data)
        counter += 1
        if len(records) == 100:
            submit_batch(records, counter, 0)
            records = []

    if len(records) != 0:
        submit_batch(records, counter, 0)
    
    print(f"end : {counter}")


def update_latest(data):
    filename = 'latest_gcp.json'
    data.to_json(f"{LOCAL_PATH}/{filename}")
    s3_path = f'latest_data/{filename}'
    session = boto3.Session()
    s3 = session.client('s3')
    with open(f"{LOCAL_PATH}/{filename}", 'rb') as f:
        s3.upload_fileobj(f, BUCKET_NAME, s3_path)


def save_raw(data, timestamp):
    SAVE_FILENAME = f"{LOCAL_PATH}/spotlake_"+f"{timestamp}.csv.gz"
    data.to_csv(SAVE_FILENAME, index=False, compression='gzip')
    session = boto3.Session()
    s3 = session.client('s3')
    s3_dir_name = timestamp.strftime("%Y/%m/%d")
    s3_obj_name = timestamp.strftime("%H:%M:%S")
    with open(SAVE_FILENAME, 'rb') as f:
        s3.upload_fileobj(
            f, BUCKET_NAME, f"rawdata/gcp/{s3_dir_name}/{s3_obj_name}.csv.gz")

    for filename in os.listdir(f"{LOCAL_PATH}/"):
        if "spotlake_" in filename:
            os.remove(f"{LOCAL_PATH}/{filename}")

