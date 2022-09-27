import boto3
from botocore.config import Config
import os
import pandas as pd
import gzip
from datetime import datetime, timezone

### preprocess rawdata from s3 temp bucket and save 'spotlake' bucket
### before this, have to aws sync from s3 temp bucket 
### plz check NEW_BUCKET_NAME, file paths before running this

session = boto3.session.Session(region_name='us-west-2')
write_client = session.client('timestream-write', config=Config(read_timeout=20, max_pool_connections=5000, retries={'max_attempts':10}))
client = session.client('timestream-query')

NEW_BUCKET_NAME = 'spotlake'

FILE_PATH = '/home/ubuntu/gcp_rawdata'          # 2022/MM/dd
TEMP_FILE_PATH = './'
NEW_FILE_PATH = '/home/ubuntu/gcp_newrawdata'  
workload_cols = ['InstanceType', 'Region']
feature_cols = ['Calculator OnDemand Price', 'Calculator Preemptible Price', 'VM Instance OnDemand Price', 'VM Instance Preemptible Price']

def save_raw(data, timestamp):
    SAVE_FILENAME = f"{TEMP_FILE_PATH}/spotlake_"+f"{timestamp}.csv.gz"
    data.to_csv(SAVE_FILENAME, index=False, compression='gzip')
    session = boto3.Session()
    s3 = session.client('s3')
    s3_dir_name = timestamp.strftime("%Y/%m/%d")
    s3_obj_name = timestamp.strftime("%H:%M:%S")
    with open(SAVE_FILENAME, 'rb') as f:
        s3.upload_fileobj(
            f, NEW_BUCKET_NAME, f"rawdata/gcp/{s3_dir_name}/{s3_obj_name}.csv.gz")

    for filename in os.listdir(f"{TEMP_FILE_PATH}/"):
        if "spotlake_" in filename:
            os.remove(f"{TEMP_FILE_PATH}/{filename}")


# sort gcp_rawdata folder paths

paths = []
for (path, dir, files) in os.walk(FILE_PATH):
    for filename in files:
        final_path = path + '/' + filename
        paths.append(final_path)
paths.sort()

# remove unnecessary vendor, change nan into -1 and save into tmp-change-bucket
for path in paths:
    changed_time = path.split('gcp_rawdata/')[1].split('.csv.gz')[0]
    timestamp = datetime.strptime(changed_time, '%Y/%m/%d/%H:%M:%S')

    df_old = pd.DataFrame()

    with gzip.open(path, 'rb') as f:
        df_old = pd.read_csv(f)

    # remove Vendor, Calculator Savings, VM Instance Savings
    df_new = pd.DataFrame()
    try :
        df_new = df_old.drop(['Vendor', 'Calculator Savings', 'VM Instance Savings'], axis=1)
    except:
        df_new = df_old

    # # have to change nan into -1    
    df_new = df_new.replace(float('nan'), -1)

    # # write to tmp-changed-gcp
    save_raw(df_new, timestamp)
    print(timestamp)

