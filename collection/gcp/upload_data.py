import boto3
import os
from botocore.config import Config
from botocore.exceptions import ClientError

# session = boto3.session.Session(region_name='us-west-2')
# write_client = session.client('timestream-write', config=Config(read_timeout=20, max_pool_connections=5000, retries={'max_attempts':10}))


BUCKET_NAME = 'tmp-gcp'
# DATABASE_NAME = 'spotlake'
# TABLE_NAME = 'gcp'
LOCAL_PATH = '/home/ubuntu/spot-score/collection/gcp'

# need to implement timestream upload

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
            f, BUCKET_NAME, f"rawdata/{s3_dir_name}/{s3_obj_name}.csv.gz")

    for filename in os.listdir(f"{LOCAL_PATH}/"):
        if "spotlake_" in filename:
            os.remove(f"{LOCAL_PATH}/{filename}")

