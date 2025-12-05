# ------ import module ------
from datetime import datetime, timezone
import boto3.session, botocore
import sys, os, argparse
import pickle, gzip, json, yaml
import pandas as pd
from io import StringIO

# ------ import user module ------
# Assuming utility modules are in PYTHONPATH
# sys.path.append("/home/ubuntu/spotlake")
# from const_config import AwsCollector, Storage
from utility.slack_msg_sender import send_slack_message

from sps_query_api import query_sps

# ------ S3 File Helper Functions ------
def read_metadata(s3_client, bucket_name, s3_key, default_value=None):
    """
    S3에서 YAML 메타데이터를 읽어 내용을 반환합니다.
    """
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        content = response['Body'].read().decode('utf-8')
        print(f"S3에서 메타데이터 읽기 성공: {s3_key}")
        return yaml.safe_load(content)
    except s3_client.exceptions.NoSuchKey:
        print(f"S3에 파일이 없습니다 ({s3_key}).")
        # fallback도 실패하면 기본값 반환
        if default_value is not None:
            print(f"기본값을 사용합니다: {default_value}")
            return default_value
        else:
            print(f"S3에 파일이 없고 기본값도 없습니다 ({s3_key})")
            return {}
    except Exception as e:
        print(f"S3에서 파일 읽기 실패 ({s3_key}): {e}")
        raise e

def write_metadata(s3_client, bucket_name, s3_key, metadata):
    """
    S3에 YAML 메타데이터를 저장합니다.
    """
    try:
        content = yaml.dump(metadata)
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=content.encode('utf-8')
        )
        print(f"S3에 메타데이터 저장 성공: {s3_key}")
    except Exception as e:
        print(f"S3에 파일 쓰기 실패 ({s3_key}): {e}")
        raise e

def main():
    # ------ Setting Constants ------
    # ------ Setting Constants ------
    BUCKET_NAME = "spotlake"
    READ_BUCKET_NAME = "spotlake"
    WRITE_BUCKET_NAME = "spotlake-test"
    S3_PATH_PREFIX = "rawdata/aws"
    CREDENTIAL_FILE_PATH = "credential/credential_3699.csv"
    LOG_GROUP_NAME = "Collection-Data-Count"
    LOG_STREAM_NAME = "AWS-Count"
    
    # ------ Setting Client ------
    session = boto3.session.Session()
    s3 = session.resource("s3")
    s3_client = session.client("s3", region_name="us-west-2")

    # ------ Receive UTC Time Data ------
    parser = argparse.ArgumentParser()
    parser.add_argument('--timestamp', dest='timestamp', action='store', help='Timestamp in format YYYY-MM-DDTHH:MM')
    args = parser.parse_args()
    
    if args.timestamp:
        # Handle EventBridge timestamp format (YYYY-MM-DDTHH:MM:SSZ)
        if args.timestamp.endswith('Z'):
            timestamp_utc = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M:%SZ")
        else:
            try:
                timestamp_utc = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                timestamp_utc = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M")
    else:
        timestamp_utc = datetime.now(timezone.utc)
        # Round down to nearest 10 minutes
        timestamp_utc = timestamp_utc.replace(minute=((timestamp_utc.minute // 10) * 10), second=0, microsecond=0)

    print(f"스크립트 실행 시작 시간 (UTC) : {timestamp_utc}")

    # ------ Modify Date Data Format ------
    date = timestamp_utc.strftime("%Y-%m-%d")
    S3_DIR_NAME = timestamp_utc.strftime("%Y/%m/%d")
    S3_OBJECT_PREFIX = timestamp_utc.strftime("%H-%M")
    execution_time_start = datetime.now(timezone.utc)

    # ------ Save Value of Credential Start Index ------
    SPS_METADATA_S3_KEY = f"{S3_PATH_PREFIX}/localfile/sps_metadata.yaml"

    metadata = read_metadata(
        s3_client, WRITE_BUCKET_NAME, SPS_METADATA_S3_KEY,
        default_value={
            "credential_index": {"init": 0, "current": 0},
            "target_capacity_index": {"init": 0, "current": 0},
            "workload_date": ""
        }
    )

    init_credential_index = metadata["credential_index"]["init"]
    current_credential_index = metadata["credential_index"]["current"]

    # ------ Set Target Capacities ------
    target_capacities = [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50]
    
    init_target_capacity_index = metadata["target_capacity_index"]["init"]
    target_capacity_index = metadata["target_capacity_index"]["current"]
    
    target_capacity_index = target_capacity_index % len(target_capacities)
    target_capacity = target_capacities[target_capacity_index]

    # ------ Load Workload File -------
    start_time = datetime.now(timezone.utc)
    workload = None
    try:
        key = f"{S3_PATH_PREFIX}/workloads/{S3_DIR_NAME}/binpacked_workloads.pkl.gz"
        print(f"Loading workload from S3: {key}")
        workload = pickle.load(gzip.open(s3.Object(READ_BUCKET_NAME, key).get()["Body"]))

        # ------ Check Workload Date Change (S3 방식, Spot Batch 호환) ------
        # S3에서 저장된 workload 날짜 읽기
        saved_workload_date = metadata["workload_date"]

        print(f"저장된 workload 날짜: '{saved_workload_date}', 현재 날짜: '{date}'")

        # workload 날짜가 변경되었는지 확인
        if saved_workload_date != date:
            print(f"workload 날짜가 변경되었습니다: {saved_workload_date} -> {date}")

            # workload 파일이 바뀌었으므로 계정 묶음 change
            init_credential_index = 1800 if init_credential_index == 0 else 0
            current_credential_index = init_credential_index
            metadata["credential_index"]["init"] = init_credential_index
            metadata["credential_index"]["current"] = init_credential_index

            # workload 파일이 바뀌었으므로 target capacity index 초기화
            init_target_capacity_index = target_capacity_index
            metadata["target_capacity_index"]["init"] = init_target_capacity_index
            metadata["target_capacity_index"]["current"] = init_target_capacity_index

            # 새로운 workload 날짜 저장
            metadata["workload_date"] = date
            
            write_metadata(s3_client, WRITE_BUCKET_NAME, SPS_METADATA_S3_KEY, metadata)
        else:
            print("workload 날짜가 동일합니다. index를 유지합니다.")
    except Exception as e:
        message = f"bucket : {BUCKET_NAME}, object : {key} 가 수집되지 않았습니다."
        send_slack_message(message)
        print(message)
        raise e
        
    print(f"계정 시작 인덱스 : {current_credential_index}")

    # ------ Load Credential File ------
    credentials = None
    try:
        csv_content = s3.Object(READ_BUCKET_NAME, CREDENTIAL_FILE_PATH).get()["Body"].read().decode('utf-8')
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
        metadata["credential_index"]["init"] = init_credential_index
        metadata["credential_index"]["current"] = init_credential_index
    else:
        metadata["credential_index"]["init"] = init_credential_index
        metadata["credential_index"]["current"] = current_credential_index
        
    metadata["target_capacity_index"]["init"] = init_target_capacity_index
    metadata["target_capacity_index"]["current"] = next_target_capacity_index
    
    write_metadata(s3_client, WRITE_BUCKET_NAME, SPS_METADATA_S3_KEY, metadata)
    
    end_time = datetime.now(timezone.utc)
    print(f"Target Capacity {target_capacity} query time is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")
    print(f"사용한 credential range : {(start_credential_index, current_credential_index)}")

    start_time = datetime.now(timezone.utc)
    # ------ Save Dataframe File ------
    try:
        object_name = f"{S3_OBJECT_PREFIX}_sps_{target_capacity}.pkl"
        # Use /tmp for temporary files in Batch/Lambda
        saved_filename = f"/tmp/{object_name}"
        gz_filename = f"{saved_filename}.gz"

        with open(saved_filename, "wb") as f:
            pickle.dump(sps_df, f)

        with open(saved_filename, "rb") as f_in, gzip.open(gz_filename, "wb") as f_out:
            f_out.writelines(f_in)

        with open(gz_filename, "rb") as f:
            s3_client.upload_fileobj(f, WRITE_BUCKET_NAME, f"{S3_PATH_PREFIX}/sps/{S3_DIR_NAME}/{S3_OBJECT_PREFIX}_sps_{target_capacity}.pkl.gz")

        os.remove(saved_filename)
        os.remove(gz_filename)

        # Save S3 key to a file for the merge script
        s3_key = f"{S3_PATH_PREFIX}/sps/{S3_DIR_NAME}/{S3_OBJECT_PREFIX}_sps_{target_capacity}.pkl.gz"
        with open("/tmp/sps_key.txt", "w") as f:
            f.write(s3_key)
        print(f"SPS Key saved to /tmp/sps_key.txt: {s3_key}")

    except Exception as e:
        send_slack_message(e)
        print(f"파일 저장 및 업로드 중 오류 발생: {e}")
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
    try:
        message = json.dumps({"MUMBER_ROWS" : sps_df.shape[0]})
        timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
        try:
            response = log_client.put_log_events(
                logGroupName=LOG_GROUP_NAME,
                logStreamName=LOG_STREAM_NAME,
                logEvents=[
                    {
                        'timestamp' : timestamp,
                        'message' : message
                    },
                ],
            )
        except Exception as e:
            print(e)
            # Don't raise here, just log
    except Exception as e:
        print(e)
        # Don't raise here
    print(f"수집된 DataFrame 행 수 : {sps_df.shape[0]}")

if __name__ == "__main__":
    start_time = datetime.now(timezone.utc)
    main()
    end_time = datetime.now(timezone.utc)
    print(f"Running time is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")
