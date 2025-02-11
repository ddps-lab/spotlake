# ------ import module ------
from datetime import datetime, timezone
import boto3.session, botocore
import sys, os, argparse
import pickle, gzip, json
import pandas as pd
from io import StringIO

# ------ import user module ------
# memo: change the path
sys.path.append("/home/ubuntu/spotlake/utility")
from slack_msg_sender import send_slack_message
from sps_query_api import query_sps

def main():
    # ------ Setting Client ------
    session = boto3.session.Session(profile_name="spotlake")
    s3 = session.resource("s3")
    s3_client = session.client("s3", region_name="us-west-2")

    # ------ Create Index Files ------
    # memo: change the cloud path
    CURRENT_PATH = "/home/ubuntu/spotlake/collector/spot-dataset/aws/collector/"

    CREDENTIAL_START_INDEX_FILE_NAME = f"{CURRENT_PATH}/credential_index.txt"
    if not os.path.exists(CREDENTIAL_START_INDEX_FILE_NAME):
        with open(CREDENTIAL_START_INDEX_FILE_NAME, 'w') as file:
            file.write('0\n0')
    TARGET_CAPACITY_INDEX_FILE_NAME = f"{CURRENT_PATH}/target_capacity_index.txt"
    if not os.path.exists(TARGET_CAPACITY_INDEX_FILE_NAME):
        with open(TARGET_CAPACITY_INDEX_FILE_NAME, 'w') as file:
            file.write('0\n0')

    # ------ Receive UTC Time Data ------
    parser = argparse.ArgumentParser()
    parser.add_argument('--timestamp', dest='timestamp', action='store')
    args = parser.parse_args()
    timestamp_utc = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M")
    
    print(f"스크립트 실행 시작 시간 (UTC) : {timestamp_utc}")

    # ------ Modify Date Data Format ------
    date = args.timestamp.split("T")[0]
    timestamp_utc = timestamp_utc.replace(minute=((timestamp_utc.minute // 10) * 10), second=0)
    S3_DIR_NAME = timestamp_utc.strftime("%Y/%m/%d")
    S3_OBJECT_PREFIX = timestamp_utc.strftime("%H-%M")
    execution_time_start = datetime.now(timezone.utc)

    # ------ Save Value of Credential Start Index ------
    with open(CREDENTIAL_START_INDEX_FILE_NAME, 'r') as f:
        init_credential_index, current_credential_index = map(int, f.readlines())
    
    # ------ Set Target Capacities ------
    target_capacities = [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50]
    with open(TARGET_CAPACITY_INDEX_FILE_NAME, 'r') as f:
        init_target_capacity_index, target_capacity_index = map(int, f.readlines())
    target_capacity_index = target_capacity_index % len(target_capacities)
    target_capacity = target_capacities[target_capacity_index]

    # ------ Load Workload File -------
    BUCKET_NAME = "spotlake"
    BUCKET_FILE_PATH = "rawdata/aws/workloads"

    start_time = datetime.now(timezone.utc)
    workload = None
    try:
        key = f"{BUCKET_FILE_PATH}/{S3_DIR_NAME}/binpacked_workloads.pkl.gz"
        workload = pickle.load(gzip.open(s3.Object(BUCKET_NAME, key).get()["Body"]))

        local_workload_path = f"{CURRENT_PATH}/{date}_binpacked_workloads.pkl.gz"
        
        # workload파일을 새로 받았다면 다운로드
        if not os.path.exists(local_workload_path):
            for filename in os.listdir(f"{CURRENT_PATH}"):
                if "_binpacked_workloads.pkl.gz" in filename:
                    os.remove(f"{CURRENT_PATH}/{filename}")
            
            s3_client.download_file(BUCKET_NAME, key, local_workload_path)
            # workload 파일이 바뀌었으므로 계정 묶음 change
            init_credential_index = 1800 if init_credential_index == 0 else 0
            with open(CREDENTIAL_START_INDEX_FILE_NAME, 'w') as f:
                f.write(f"{str(init_credential_index)}\n{str(init_credential_index)}")
            # workload 파일이 바뀌었으므로 index location save
            init_target_capacity_index = target_capacity_index
            with open(TARGET_CAPACITY_INDEX_FILE_NAME, 'w') as f:
                f.write(f"{str(init_target_capacity_index)}\n{str(init_target_capacity_index)}")
    except Exception as e:
        message = f"bucket : {BUCKET_NAME}, object : {key} 가 수집되지 않았습니다.\n서버에 있는 로컬 workload파일을 불러옵니다."
        send_slack_message(message)
        print(message)
        is_local = False
        for filename in os.listdir(f"{CURRENT_PATH}"):
            if "_binpacked_workloads.pkl.gz" in filename:
                print(f"로컬 워크로드 파일 {CURRENT_PATH}/{filename} 사용")
                with open(f"{CURRENT_PATH}/{filename}", 'rb') as f:
                    workload = pickle.load(gzip.open(f))
                is_local = True
                break
        if not is_local:
            message = f"로컬파일에 workload파일이 존재하지 않습니다."
            send_slack_message(message)
            print(message)
            raise Exception("does not exist local workloads file")
    print(f"계정 시작 인덱스 : {current_credential_index}")

    # ------ Load Credential File ------
    CREDENTIAL_FILE_PATH = "aws/credentials/credential_3699.csv"
    credentials = None
    try:
        csv_content = s3.Object(BUCKET_NAME, CREDENTIAL_FILE_PATH).get()["Body"].read().decode('utf-8')
        credentials = pd.read_csv(StringIO(csv_content))
    except Exception as e:
        send_slack_message(e)
        print(e)
        raise e
    
    end_time = datetime.now(timezone.utc)
    print(f"Load credential and workload time : {(end_time - start_time).total_seconds():.4f} ms")

    # ------ Start Query Per Target Capacity ------
    start_time = datetime.now(timezone.utc)
    start_credential_index = current_credential_index

    try:
        df_list = []
        for scenarios in workload:
            while True:
                try:
                    args = (credentials.iloc[current_credential_index], scenarios, target_capacity)
                    current_credential_index += 1
                    df = query_sps(args)
                    df_list.append(df)
                    break
                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == 'MaxConfigLimitExceeded':
                        continue
                    else:
                        send_slack_message(e)
                        print(e)
                        raise e
                except Exception as e:
                    send_slack_message(e)
                    print(e)
                    raise e
            break
        sps_df = pd.concat(df_list, axis=0, ignore_index=True)
    except Exception as e:
        message = f"error at query_sps\nerror : {e}"
        send_slack_message(message)
        print(message)
        raise e

    # ------ Update config files ------
    next_target_capacity_index = (target_capacity_index + 1) % len(target_capacities)
    print(next_target_capacity_index)
    if next_target_capacity_index == init_target_capacity_index:
        with open(CREDENTIAL_START_INDEX_FILE_NAME, "w") as f:
            f.write(f"{str(init_credential_index)}\n{str(init_credential_index)}")
    else:
        with open(CREDENTIAL_START_INDEX_FILE_NAME, "w") as f:
            f.write(f"{str(init_credential_index)}\n{str(current_credential_index)}")
    with open(TARGET_CAPACITY_INDEX_FILE_NAME, "w") as f:
        f.write(f"{str(init_target_capacity_index)}\n{str(next_target_capacity_index)}")
    
    end_time = datetime.now(timezone.utc)
    print(f"Target Capacity {target_capacity} query time is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")
    print(f"사용한 credential range : {(start_credential_index, current_credential_index)}")

    start_time = datetime.now(timezone.utc)
    # ------ Save Dataframe File ------
    try:
        object_name = f"{S3_OBJECT_PREFIX}_sps_{target_capacity}.pkl"
        saved_filename = f"{CURRENT_PATH}/" + f"{object_name}"
        try:
            pickle.dump(sps_df, open(saved_filename, "wb"))
            gzip.open(f"{saved_filename}.gz", "wb").writelines(open(f"{saved_filename}", "rb"))
        except Exception as e:
            send_slack_message(e)
            print(e)
        # memo: change the saving cloud path
        s3_client.upload_fileobj(open(f"{saved_filename}.gz", "rb"), BUCKET_NAME, f"rawdata/aws/sps/{S3_DIR_NAME}/{S3_OBJECT_PREFIX}_sps_{target_capacity}.pkl.gz")
        os.remove(f"{saved_filename}")
        os.remove(f"{saved_filename}.gz")
    except Exception as e:
        send_slack_message(e)
        print(e)
        raise e
    end_time = datetime.now(timezone.utc)
    print(f"Saving time of DF File is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")

    # ------ Monitoring for total execution time ------
    execution_time_end = datetime.now(timezone.utc)
    total_execution_time = (execution_time_end - execution_time_start).total_seconds()
    if total_execution_time >= 600000:
        message = f"sps 쿼리 시간이 10분을 초과하였습니다 : {total_execution_time} ms"
        message += f"\n실행 시작 시간 (UTC) : {timestamp_utc}"
        send_slack_message(message)
        print(message)

    # ------ Upload Collecting Data Number at Cloud Logs ------
    log_client = session.client('logs', 'us-west-2')
    # memo: change the log group name
    log_group_name = "SPS-Server-Data-Count"
    log_stream_name = "aws"
    
    try:
        message = json.dumps({"MUMBER_ROWS" : sps_df.shape[0]})
        timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
        try:
            response = log_client.put_log_events(
                logGroupName = log_group_name,
                logStreamName = log_stream_name,
                logEvents = [
                    {
                        'timestamp' : timestamp,
                        'message' : message
                    },
                ],
            )
        except Exception as e:
            print(e)
            raise e
    except Exception as e:
        print(e)
        raise e
    print(f"수집된 DataFrame 행 수 : {sps_df.shape[0]}")

if __name__ == "__main__":
    start_time = datetime.now(timezone.utc)
    main()
    end_time = datetime.now(timezone.utc)
    print(f"Running time is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")