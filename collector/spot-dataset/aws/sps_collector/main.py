import boto3
import pickle
import pandas as pd
import argparse
import sys
import os
import gzip
from datetime import datetime
from sps_query_api import query_sps
from time import time

from concurrent.futures import ThreadPoolExecutor
from io import StringIO

sys.path.append("/home/ubuntu/spotlake/utility")
from slack_msg_sender import send_slack_message

s3 = boto3.resource("s3")
NUM_THREAD = 26
CURRENT_PATH = "/home/ubuntu/spotlake/collector/spot-dataset/aws/sps_collector/"
WORKLOAD_FILE_PATH = "rawdata/aws/workloads"
CREDENTIAL_FILE_PATH = "credential/credential_3699.csv"

bucket_name = "sps-query-data"
workload_bucket_name = "splotlake"

parser = argparse.ArgumentParser()
parser.add_argument('--timestamp', dest='timestamp', action='store')
args = parser.parse_args()
timestamp = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M")
date = args.timestamp.split("T")[0]

def save_data(df, timestamp, target_capacity):
    SAVE_FILENAME = f"{CURRENT_PATH}/"+f"{timestamp}_target_capacity_{target_capacity}.csv.gz"
    df.to_csv(SAVE_FILENAME, index=False, compression="gzip")
    session = boto3.Session()
    s3 = session.client('s3')
    s3_dir_name = timestamp.strftime("%Y/%m/%d")
    s3_obj_name = timestamp.strftime("%H-%M-%S")

    with open(SAVE_FILENAME, 'rb') as f:
        s3.upload_fileobj(f, bucket_name, f"aws/{s3_dir_name}/{s3_obj_name}_sps_{target_capacity}.csv.gz")
    
    for filename in os.listdir(f"{CURRENT_PATH}/"):
        if "_sps_" in filename:
            os.remove(f"{CURRENT_PATH}/{filename}")

def print_ms(start_time, end_time, message):
    delta_time_ms = (end_time - start_time) * 1000
    print(f"{message} : {delta_time_ms} ms")


start_time = time()
workload = None
try:
    key = f"{WORKLOAD_FILE_PATH}/{'-'.join(date.split('-'))}/binpacked_workloads.pkl.gz"
    workload = pickle.load(gzip.open(s3.Object(workload_bucket_name, key).get()["Body"]))
except Exception as e:
    print("Exception at load workloads")
    send_slack_message(e)
    print(e)
    exit(1)
end_time = time()
print_ms(start_time, end_time, "workload 파일을 s3에서 load하는 데 걸린 시간")

start_time = time()
credentials = None
try:
    csv_content = s3.Object(bucket_name, CREDENTIAL_FILE_PATH).get()["Body"].read().decode('utf-8')
    credentials = pd.read_csv(StringIO(csv_content))
except Exception as e:
    print("Exception at load credentials")
    send_slack_message(e)
    print(e)
    exit(1)
end_time = time()
print_ms(start_time, end_time, "credential 파일을 s3에서 load하는 데 걸린 시간")
    
start_time = time()
idx_credential = 0
# 나중에 data frame 합병을 쉽게 하기 위해서 work_per_thread를 이중 리스트로 만들어 놓았습니다.
# target_capacity별로 리스트가 생성됩니다.
# 이중 리스트 크기는 11 x (scenario 개수) 입니다.
work_per_thread = []
target_capacities = [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50]
for target_capacity in target_capacities:
    work_per_target_capacity = []
    for scenarios in workload:
        for i in range(len(scenarios)):
            credential = credentials.iloc[idx_credential]
            idx_credential += 1
            
            work_per_target_capacity.append((credential, scenarios, target_capacity))
    work_per_thread.append(work_per_target_capacity)
end_time = time()
print_ms(start_time, end_time, "workload 내용을 멀티 프로세싱을 할 수 있게 분할하는 데 걸린 시간")

try:
    sps_df_per_target_capacity = []
    # 출력용 변수입니다.
    idx_target_capacity = 0
    for work_per_target_capacity in work_per_thread:
        start_time = time()
        with ThreadPoolExecutor(max_workers=NUM_THREAD) as executor:
            sps_df_list = list(executor.map(query_sps, work_per_target_capacity))
        df_combined = pd.concat(sps_df_list, axis=0, ignore_index=True)
        sps_df_per_target_capacity.append(df_combined)
        end_time = time()
        print_ms(start_time, end_time, f"Target Capacity {target_capacities[idx_target_capacity]} 작업 완료 시간")
except Exception as e:
    print("Exception at query and combine")
    send_slack_message(e)
    print(e)
    exit(1)


start_time = time()
try:
    for i in range(len(sps_df_per_target_capacity)):
        sps_df = sps_df_per_target_capacity[i]
        target_capacity = target_capacities[i]
        save_data(sps_df, timestamp, target_capacity)
except Exception as e:
    print("Exception at save data")
    send_slack_message(e)
    print(e)
    exit(1)
end_time = time()
print_ms(start_time, end_time, "DataFrame 수직적 병합 완료 시간")