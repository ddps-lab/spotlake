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
from sps_utils import upload_data_to_s3, log_execution_time, calculate_execution_ms, upload_log_event
from time import time

from concurrent.futures import ThreadPoolExecutor
from io import StringIO

sys.path.append("/home/ubuntu/spotlake/utility")
from slack_msg_sender import send_slack_message

s3 = boto3.resource("s3")
log_client = boto3.client('logs')
NUM_WORKER = 26
CURRENT_PATH = "/home/ubuntu/spotlake/collector/spot-dataset/aws/sps_collector/"
WORKLOAD_FILE_PATH = "rawdata/aws/workloads"
CREDENTIAL_FILE_PATH = "credential/credential_3699.csv"
START_CREDENTIAL_INDEX = 100
LOG_GROUP_NAME = "SPS_Collected-Data"
LOG_STREAM_NAME_EXECUTION_TIME = "execution-time"
LOG_STREAM_NAME_AMOUNT_DATA = "amount-data"

# test
bucket_name = "sps-query-test"
workload_bucket_name = "spotlake"

parser = argparse.ArgumentParser()
parser.add_argument('--timestamp', dest='timestamp', action='store')
args = parser.parse_args()
timestamp_utc = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M")
date = args.timestamp.split("T")[0]

rounded_minute = (timestamp_utc.minute // 10) * 10 # 분을 10분단위로 내림합니다.
timestamp_utc = timestamp_utc.replace(minute=rounded_minute, second=0)
S3_DIR_NAME = timestamp_utc.strftime("%Y/%m/%d/%H/%M")

total_execution_time_ms = 0
target_capacities = [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50]

idx_credential = START_CREDENTIAL_INDEX
def get_work_per_thread():
    global idx_credential
    work_per_thread = []
    for target_capacity in target_capacities:
        work_per_target_capacity = []
        for scenarios in workload:
            credential = credentials.iloc[idx_credential]
            idx_credential += 1
            work_per_target_capacity.append((credential, scenarios, target_capacity))
        work_per_thread.append(work_per_target_capacity)
    return work_per_thread

start_time = time()
workload = None
try:
    key = f"{WORKLOAD_FILE_PATH}/{'/'.join(date.split('-'))}/binpacked_workloads.pkl.gz"
    workload = pickle.load(gzip.open(s3.Object(workload_bucket_name, key).get()["Body"]))
except Exception as e:
    send_slack_message(e)
    raise e
end_time = time()
total_execution_time_ms += calculate_execution_ms(start_time, end_time)

start_time = time()
credentials = None
try:
    csv_content = s3.Object(bucket_name, CREDENTIAL_FILE_PATH).get()["Body"].read().decode('utf-8')
    credentials = pd.read_csv(StringIO(csv_content))
except Exception as e:
    send_slack_message(e)
    raise e
end_time = time()
total_execution_time_ms += calculate_execution_ms(start_time, end_time)
    
start_time = time()
work_per_thread = get_work_per_thread()
end_time = time()
total_execution_time_ms += calculate_execution_ms(start_time, end_time)

while True:
    try:
        sps_df_per_target_capacity = []
        idx_target_capacity = 0 # 출력용 변수입니다.
        for work_per_target_capacity in work_per_thread:
            target_capacity = target_capacities[idx_target_capacity]
            start_time = time()
            with ThreadPoolExecutor(max_workers=NUM_WORKER) as executor:
                sps_df_list = list(executor.map(query_sps, work_per_target_capacity))
            df_combined = pd.concat(sps_df_list, axis=0, ignore_index=True)
            sps_df_per_target_capacity.append(df_combined)
            end_time = time()
            total_execution_time_ms += calculate_execution_ms(start_time, end_time)
            response = log_execution_time(log_client, start_time, end_time, 
                                            LOG_GROUP_NAME, LOG_STREAM_NAME_EXECUTION_TIME, 
                                            f"QUERY_TIME_{target_capacity}")
            idx_target_capacity += 1
        break
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'MaxConfigLimitExceeded':
            work_per_thread = get_work_per_thread()
        else:
            send_slack_message(e)
            raise e
    except Exception as e:
        send_slack_message(e)
        raise e

start_time = time()
try:
    key = ['InstanceType', 'Region', 'AZ']
    merged_df = pd.DataFrame(columns=key)
    for df in sps_df_per_target_capacity:
        merged_df = pd.merge(merged_df, df, on=key, how='outer')
    
    csv_object_name = "sps_1_to_50.csv.gz"
    SAVED_FILENAME = f"{CURRENT_PATH}"+f"{csv_object_name}"
    merged_df.to_csv(SAVED_FILENAME, index=False, compression="gzip")
    upload_data_to_s3(SAVED_FILENAME, S3_DIR_NAME, csv_object_name, bucket_name)
except Exception as e:
    send_slack_message(e)
    raise e
end_time = time()
total_execution_time_ms += calculate_execution_ms(start_time, end_time)
response = log_execution_time(log_client, start_time, end_time, 
                                LOG_GROUP_NAME, LOG_STREAM_NAME_EXECUTION_TIME, 
                                f"TOTAL_EXECUTION_TIME")
response = upload_log_event(log_client, LOG_GROUP_NAME, LOG_STREAM_NAME_AMOUNT_DATA,
                            f"NUMBER_ROWS", merged_df.shape[0])