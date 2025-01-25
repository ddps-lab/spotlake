# ------ import module ------
from datetime import datetime, timezone
import pandas as pd
import boto3.session
import os, pickle, gzip
import io

# ------ import user module ------
from load_price import get_spot_price, get_regions
from slack_msg_sender import send_slack_message

def main():
    # ------ Set time data ------
    start_time = datetime.now(timezone.utc)
    timestamp = start_time.replace(minute=((start_time.minute // 10) * 10), second=0)
    S3_DIR_NAME = timestamp.strftime('%Y/%m/%d')
    S3_OBJECT_PREFIX = timestamp.strftime('%H-%M')

    # ------ Collect Spot Price ------
    try:
        session = boto3.session.Session()
        regions = get_regions(session)
        spot_price_df_list = []
        for region in regions:
            spot_price_df_list.append(get_spot_price(region))
        spot_price_df = pd.concat(spot_price_df_list).reset_index(drop=True)
    except Exception as e:
        send_slack_message(f"Error during spot price collection\n{e}")
    end_time = datetime.now(timezone.utc)
    print(f"Collecting time is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")

    # ------ Save Raw Data in S3 ------
    start_time = datetime.now(timezone.utc)
    try:
        buffer = io.BytesIO()
        pickle.dump(spot_price_df, buffer)
        buffer.seek(0)

        compressed_buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=compressed_buffer, mode='wb') as gz:
            gz.write(buffer.getvalue())
        compressed_buffer.seek(0)
    except Exception as e:
        send_slack_message(f"Store spot price data to be stored in the cloud in memory\n{e}")

    s3 = session.client('s3')
    try:
        s3.upload_fileobj(compressed_buffer, os.environ.get('S3_BUCKET'), f"{os.environ.get('PARENT_PATH')}/spot_price/{S3_DIR_NAME}/{S3_OBJECT_PREFIX}_spot_price.pkl.gz")
    except Exception as e:
        send_slack_message(f"Error saving spot price data in s3\n{e}")
    end_time = datetime.now(timezone.utc)
    print(f"Upload time is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")

def lambda_handler(event, context):
    start_time = datetime.now(timezone.utc)
    main()
    end_time = datetime.now(timezone.utc)
    print(f"Running time is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")
    return "Process completed successfully"