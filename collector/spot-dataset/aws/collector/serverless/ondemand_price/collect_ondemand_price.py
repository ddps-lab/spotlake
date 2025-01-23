# ------ import module ------
from datetime import datetime, timezone, timedelta
import boto3
import os, pickle, gzip
import io

# ------ import user module ------
from load_price import get_ondemand_price
from slack_msg_sender import send_slack_message

def main():
    # ------ Set time data ------
    start_time = datetime.now(timezone.utc)
    timestamp = start_time.replace(minute=((start_time.minute // 10) * 10), second=0) + timedelta(days=1)
    S3_DIR_NAME = timestamp.strftime('%Y/%m/%d')

    # ------ Collect Ondemand Price ------
    try:
        ondemand_price_df = get_ondemand_price()
    except Exception as e:
        send_slack_message(f"Error during ondemand price collection\n{e}")

    end_time = datetime.now(timezone.utc)
    print(f"Collecting time is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")

    # ------ Save Raw Data in S3 ------
    start_time = datetime.now(timezone.utc)
    try:
        buffer = io.BytesIO()
        pickle.dump(ondemand_price_df, buffer)
        buffer.seek(0)

        compressed_buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=compressed_buffer, mode='wb') as gz:
            gz.write(buffer.getvalue())
        compressed_buffer.seek(0)
    except Exception as e:
        send_slack_message(e)

    s3 = boto3.client('s3')
    try:
        s3.upload_fileobj(compressed_buffer, os.environ.get('S3_BUCKET'), f"{os.environ.get('PARENT_PATH')}/ondemand_price/{S3_DIR_NAME}/ondemand_price.pkl.gz")
    except Exception as e:
        send_slack_message(f"Error saving ondemand data in s3\n{e}")
    end_time = datetime.now(timezone.utc)
    print(f"Uploaded time is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")

def lambda_handler(event, context):
    start_time = datetime.now(timezone.utc)
    main()
    end_time = datetime.now(timezone.utc)
    print(f"Running time is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")
    return "Process completed successfully"