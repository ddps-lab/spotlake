# ------ import module ------
from datetime import datetime, timezone
import boto3
import os, pickle, gzip
import subprocess
import pandas as pd
import io

# ------ import user module ------
from slack_msg_sender import send_slack_message

def main():
    # ------ Set time data ------
    start_time = datetime.now(timezone.utc)
    timestamp = start_time.replace(minute=((start_time.minute // 10) * 10), second=0)
    S3_DIR_NAME = timestamp.strftime('%Y/%m/%d')
    S3_OBJECT_PREFIX = timestamp.strftime('%H-%M')

    try:
        # ------ Load Spot Info System ------
        EXECUTABLE_PATH = "/opt/spotinfo"

        command = [f"{EXECUTABLE_PATH} --output csv --region all"]

        # ------ Execute a Command ------
        result = subprocess.Popen(command[0].split(' '), stdout=subprocess.PIPE)
        stdout, _ = result.communicate()

        # ------ Collect Spot IF ------
        spotinfo_string = stdout.decode('utf-8')
        spotinfo_list = [row.split(',') for row in spotinfo_string.split('\n')]

        spotinfo_dict = {'Region' : [],
                        'InstanceType' : [],
                        'IF' : []}
        
        # remove column name from data using indexing
        for spotinfo in spotinfo_list[2:-1]:
            spotinfo_dict['Region'].append(spotinfo[0])
            spotinfo_dict['InstanceType'].append(spotinfo[1])
            spotinfo_dict['IF'].append(spotinfo[5])
        
        spotinfo_df = pd.DataFrame(spotinfo_dict)

        frequency_map = {'<5%': 3.0, '5-10%': 2.5, '10-15%': 2.0, '15-20%': 1.5, '>20%': 1.0}
        spotinfo_df = spotinfo_df.replace({'IF': frequency_map})
    except Exception as e:
        send_slack_message(e)
    end_time = datetime.now(timezone.utc)
    print(f"Collecting time is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")

    # ------ Save Raw Data in S3 ------
    start_time = datetime.now(timezone.utc)
    try:
        buffer = io.BytesIO()
        pickle.dump(spotinfo_df, buffer)
        buffer.seek(0)

        compressed_buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=compressed_buffer, mode='wb') as gz:
            gz.write(buffer.getvalue())
        compressed_buffer.seek(0)
    except Exception as e:
        send_slack_message(e)

    s3 = boto3.client('s3')
    try:
        s3.upload_fileobj(compressed_buffer, os.environ.get('S3_BUCKET'), f"{os.environ.get('PARENT_PATH')}/spot_if/{S3_DIR_NAME}/{S3_OBJECT_PREFIX}_spot_if.pkl.gz")
    except Exception as e:
        send_slack_message(e)
    end_time = datetime.now(timezone.utc)
    print(f"Upload time is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")    

def lambda_handler(event, context):
    start_time = datetime.now(timezone.utc)
    main()
    end_time = datetime.now(timezone.utc)
    print(f"Running time is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")
    return "Process completed successfully"