import boto3
import botocore
import pickle
import pandas as pd
import argparse
import sys
import os
import gzip
import logging
from datetime import datetime
from sps_query_api import query_sps
from time import time

from concurrent.futures import ThreadPoolExecutor
from io import StringIO

sys.path.append("/home/ubuntu/spotlake/utility")
from slack_msg_sender import send_slack_message

s3 = boto3.resource("s3")
NUM_WORKER = 26
CURRENT_PATH = "/home/ubuntu/spotlake/collector/spot-dataset/aws/sps_collector/"
WORKLOAD_FILE_PATH = "rawdata/aws/workloads"
CREDENTIAL_FILE_PATH = "credential/credential_3699.csv"
START_CREDENTIAL_INDEX = 100
LOG_FILENAME = f"{CURRENT_PATH}server.log"

logging.basicConfig(filename=LOG_FILENAME, level=logging.INFO)

bucket_name = "sps-query-data"
workload_bucket_name = "spotlake"

parser = argparse.ArgumentParser()
parser.add_argument('--timestamp', dest='timestamp', action='store')
args = parser.parse_args()
timestamp_utc = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M")
date = args.timestamp.split("T")[0]

logging.info(f"실행 시작 시간 (UTC) : {timestamp_utc}")

rounded_minute = (timestamp_utc.minute // 10) * 10 # 분을 10분단위로 내림합니다.
timestamp_utc = timestamp_utc.replace(minute=rounded_minute, second=0)
S3_DIR_NAME = timestamp_utc.strftime("%Y/%m/%d/%H/%M")

total_execution_time_ms = 0
target_capacities = [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50]

def upload_data_to_s3(saved_filename, s3_dir_name, s3_obj_name):
    session = boto3.Session()
    s3 = session.client('s3')

    with open(saved_filename, 'rb') as f:
        s3.upload_fileobj(f, bucket_name, f"aws/{s3_dir_name}/{s3_obj_name}")
    
    os.remove(saved_filename)

def log_ms(start_time, end_time, message):
    delta_time_ms = (end_time - start_time) * 1000
    global total_execution_time_ms
    total_execution_time_ms += delta_time_ms
    logging.info(f"{message} : {delta_time_ms} ms")

idx_credential = START_CREDENTIAL_INDEX
def get_work_per_thread():
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
    logging.error("Exception at load workloads")
    send_slack_message(e)
    logging.error(e)
    exit(1)
end_time = time()
log_ms(start_time, end_time, "workload 파일을 s3에서 load하는 데 걸린 시간")

start_time = time()
credentials = None
try:
    csv_content = s3.Object(bucket_name, CREDENTIAL_FILE_PATH).get()["Body"].read().decode('utf-8')
    credentials = pd.read_csv(StringIO(csv_content))
except Exception as e:
    logging.error("Exception at load credentials")
    send_slack_message(e)
    logging.error(e)
    exit(1)
end_time = time()
log_ms(start_time, end_time, "credential 파일을 s3에서 load하는 데 걸린 시간")
    
start_time = time()
work_per_thread = get_work_per_thread()
end_time = time()
logging.info(f"사용한 계정 개수 : {idx_credential - START_CREDENTIAL_INDEX}")
log_ms(start_time, end_time, "workload 내용을 멀티 프로세싱을 할 수 있게 분할하는 데 걸린 시간")

while True:
    try:
        sps_df_per_target_capacity = []
        idx_target_capacity = 0 # 출력용 변수입니다.
        for work_per_target_capacity in work_per_thread:
            start_time = time()
            with ThreadPoolExecutor(max_workers=NUM_WORKER) as executor:
                sps_df_list = list(executor.map(query_sps, work_per_target_capacity))
            df_combined = pd.concat(sps_df_list, axis=0, ignore_index=True)
            sps_df_per_target_capacity.append(df_combined)
            end_time = time()
            log_ms(start_time, end_time, f"Target Capacity {target_capacities[idx_target_capacity]} 작업 완료 시간")
            idx_target_capacity += 1
        break
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'MaxConfigLimitExceeded':
            logging.error(f"계정당 쿼리 가능한 숫자가 넘었습니다. Target Capacity : {target_capacities[idx_target_capacity]}")
            logging.error(f"workload의 계정을 재분배합니다.")
            logging.error(f"재분배 시작 계정 인덱스 : {idx_credential}")
            work_per_thread = get_work_per_thread()
            logging.error(f"재분배 완료 계정 인덱스 : {idx_credential}")
        else:
            send_slack_message(e)
            exit(1)
    except Exception as e:
        logging.error("Exception at query and combine")
        send_slack_message(e)
        logging.error(e)
        exit(1)

start_time = time()
try:
    key = ['InstanceType', 'Region', 'AZ']
    merged_df = pd.DataFrame(columns=key)
    for df in sps_df_per_target_capacity:
        merged_df = pd.merge(merged_df, df, on=key, how='outer')
    
    csv_object_name = "sps_1_to_50.csv.gz"
    SAVED_FILENAME = f"{CURRENT_PATH}"+f"{csv_object_name}"
    merged_df.to_csv(SAVED_FILENAME, index=False, compression="gzip")
    upload_data_to_s3(SAVED_FILENAME, S3_DIR_NAME, csv_object_name)
except Exception as e:
    logging.error("Exception at horizontal merge")
    send_slack_message(e)
    logging.error(e)
    exit(1)
end_time = time()
log_ms(start_time, end_time, "DataFrame 수평적 병합 완료 시간")

logging.info(f"총 실행 시간 합 : {total_execution_time_ms} ms")
upload_data_to_s3(LOG_FILENAME, S3_DIR_NAME, "server.log")