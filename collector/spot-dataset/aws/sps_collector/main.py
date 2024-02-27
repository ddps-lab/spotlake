import boto3
import botocore
import pickle
import pandas as pd
import argparse
import sys
import os
import gzip
from datetime import datetime
from sps_query_api import query_sps
from sps_utils import *
from time import time

from concurrent.futures import ThreadPoolExecutor
from io import StringIO

sys.path.append("/home/ubuntu/spotlake/utility")
from slack_msg_sender import send_slack_message

s3 = boto3.resource("s3")
s3_client = boto3.client('s3', region_name='us-west-2')
log_client = boto3.client('logs', region_name='us-west-2')

NUM_WORKER = 26
CURRENT_PATH = "/home/ubuntu/spotlake/collector/spot-dataset/aws/sps_collector/"
WORKLOAD_FILE_PATH = "rawdata/aws/workloads"
CREDENTIAL_FILE_PATH = "credential/credential_3699.csv"
BUCKET_NAME = "sps-query-test"
WORKLOAD_BUCKET_NAME = "spotlake"
CREDENTIAL_START_INDEX_FILE_NAME = f"{CURRENT_PATH}start_index.txt"


parser = argparse.ArgumentParser()
parser.add_argument('--timestamp', dest='timestamp', action='store')
args = parser.parse_args()
timestamp_utc = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M")
date = args.timestamp.split("T")[0]
rounded_minute = (timestamp_utc.minute // 10) * 10 # 분을 10분단위로 내림합니다.
timestamp_utc = timestamp_utc.replace(minute=rounded_minute, second=0)
S3_DIR_NAME = timestamp_utc.strftime("%Y/%m/%d")
S3_OBJECT_PREFIX = timestamp_utc.strftime("%H-%M")
execution_time_start = time()

with open(CREDENTIAL_START_INDEX_FILE_NAME, 'r') as f:
    current_credential_index = int(f.read().strip())

# ------ Load Workload File -------
workload = None
try:
    key = f"{WORKLOAD_FILE_PATH}/{'/'.join(date.split('-'))}/binpacked_workloads.pkl.gz"
    workload = pickle.load(gzip.open(s3.Object(WORKLOAD_BUCKET_NAME, key).get()["Body"]))
    local_workload_path = f"{CURRENT_PATH}{date}_binpacked_workloads.pkl.gz"
    # workload파일을 새로 받았다면 다운로드
    if not os.path.exists(local_workload_path):
        for filename in os.listdir(f"{CURRENT_PATH}"):
            if "_binpacked_workloads.pkl.gz" in filename:
                os.remove(f"{CURRENT_PATH}{filename}")
        s3_client.download_file(WORKLOAD_BUCKET_NAME, key, local_workload_path)
        # workload파일이 바뀌었으므로 계정 묶음 change
        current_credential_index = 1800 if current_credential_index == 0 else 0
        with open(CREDENTIAL_START_INDEX_FILE_NAME, 'w') as f:
            f.write(str(current_credential_index))
except Exception as e:
    message = f"bucket : {WORKLOAD_BUCKET_NAME}, object : {key} 가 수집되지 않았습니다.\n 서버에 있는 로컬 workload파일을 불러옵니다."
    send_slack_message(message)
    try:
        is_local = False
        for filename in os.listdir(f"{CURRENT_PATH}"):
            if "_binpacked_workloads.pkl.gz" in filename:
                print(f"로컬 워크로드 파일 {filename} 사용")
                with open(f"{CURRENT_PATH}{filename}", 'rb') as f:
                    workload = pickle.load(f)
                is_local = True
                break
        if not is_local:
            raise Exception("does not exist local workloads file")
    except Exception as another_exception:
        message = f"로컬파일에 workload파일이 존재하지 않습니다."
        send_slack_message(message)
        raise e

# ------ Load Credential File ------
credentials = None
try:
    csv_content = s3.Object(BUCKET_NAME, CREDENTIAL_FILE_PATH).get()["Body"].read().decode('utf-8')
    credentials = pd.read_csv(StringIO(csv_content))
except Exception as e:
    send_slack_message(e)
    raise e

target_capacities = [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50]
sps_df_list = []
for target_capacity in target_capacities:
    # ------ Start Query Per Target Capacity ------
    start_time = time()
    while True:
        try:
            print(f"Target Capacity {target_capacity} Query Start")
            works = []
            start_credential_index = current_credential_index
            for scenarios in workload:
                works.append((credentials.iloc[current_credential_index], scenarios, target_capacity))
                current_credential_index += 1
            end_credential_index = current_credential_index
            with ThreadPoolExecutor(max_workers=NUM_WORKER) as executor:
                df_list = list(executor.map(query_sps, works))
            df_combined = pd.concat(df_list, axis=0, ignore_index=True)
            sps_df_list.append(df_combined)
            end_time = time()
            message = f"사용한 credential range : ({start_credential_index}, {end_credential_index})"
            print(message)
            log_execution_time(log_client, start_time, end_time, f"QUERY_{target_capacity}")
            break
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'MaxConfigLimitExceeded':
                continue
            else:
                send_slack_message(e)
                raise e
        except Exception as e:
            send_slack_message(e)
            raise e
        
# ------ Merge Horizontally Collected DataFrame ------
try:
    key = ['InstanceType', 'Region', 'AZ']
    merged_df = pd.DataFrame(columns=key)
    for df in sps_df_list:
        merged_df = pd.merge(merged_df, df, on=key, how='outer')
    object_name = f"{S3_OBJECT_PREFIX}_sps_1_to_50.csv.gz"
    saved_filename = f"{CURRENT_PATH}"+f"{object_name}"
    merged_df.to_csv(saved_filename, index=False, compression="gzip")
    upload_data_to_s3(s3_client, saved_filename, S3_DIR_NAME, object_name, BUCKET_NAME)
except Exception as e:
    send_slack_message(e)
    raise e

execution_time_end = time()
total_execution_time = calculate_execution_ms(execution_time_start, execution_time_end)
if total_execution_time >= 600000:
    send_slack_message(f"sps 쿼리 시간이 10분을 초과하였습니다 : {total_execution_time} ms")
log_execution_time(log_client, execution_time_start, execution_time_end, "TOTAL_EXECUTION_TIME")
log_amount(log_client, merged_df.shape[0])