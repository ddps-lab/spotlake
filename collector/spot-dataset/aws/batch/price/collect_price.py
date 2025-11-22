# ------ import module ------
from datetime import datetime, timezone
import pandas as pd
import boto3.session
import os, pickle, gzip
import io
import argparse

# ------ import user module ------
from load_price import get_spot_price, get_regions

try:
    from slack_msg_sender import send_slack_message
except ImportError:
    print("Warning: slack_msg_sender not found. Slack notifications will be disabled.")
    def send_slack_message(msg):
        print(f"[SLACK] {msg}")

def main():
    # ------ Set Constants ------
    BUCKET_NAME = os.environ.get("S3_BUCKET", "spotlake")
    S3_PATH_PREFIX = os.environ.get("S3_PATH_PREFIX", "rawdata/aws")

    # ------ Set time data ------
    parser = argparse.ArgumentParser()
    parser.add_argument('--timestamp', dest='timestamp', action='store', help='Timestamp in format YYYY-MM-DDTHH:MM')
    args = parser.parse_args()
    
    if args.timestamp:
        timestamp_utc = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M")
    else:
        start_time = datetime.now(timezone.utc)
        timestamp_utc = start_time.replace(minute=((start_time.minute // 10) * 10), second=0)

    S3_DIR_NAME = timestamp_utc.strftime('%Y/%m/%d')
    S3_OBJECT_PREFIX = timestamp_utc.strftime('%H-%M')
    
    print(f"Collecting Spot Price for timestamp: {timestamp_utc}")
    start_time = datetime.now(timezone.utc)

    # ------ Collect Spot Price ------
    try:
        session = boto3.session.Session()
        regions = get_regions(session)
        spot_price_df_list = []
        for region in regions:
            print(f"Collecting price for region: {region}")
            try:
                spot_price_df_list.append(get_spot_price(region))
            except Exception as e:
                print(f"Error collecting price for region {region}: {e}")
                # Continue to other regions even if one fails
                continue
                
        if spot_price_df_list:
            spot_price_df = pd.concat(spot_price_df_list).reset_index(drop=True)
            print(f"Collected {len(spot_price_df)} rows of Spot Price data")
        else:
            print("No spot price data collected")
            return

    except Exception as e:
        send_slack_message(f"Error during spot price collection\n{e}")
        raise e
        
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
        
        s3 = session.client('s3')
        key = f"{S3_PATH_PREFIX}/spot_price/{S3_DIR_NAME}/{S3_OBJECT_PREFIX}_spot_price.pkl.gz"
        s3.upload_fileobj(compressed_buffer, BUCKET_NAME, key)
        print(f"Uploaded data to s3://{BUCKET_NAME}/{key}")
        
    except Exception as e:
        send_slack_message(f"Store spot price data to be stored in the cloud in memory\n{e}")
        raise e

    end_time = datetime.now(timezone.utc)
    print(f"Upload time is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")

if __name__ == "__main__":
    start_time = datetime.now(timezone.utc)
    main()
    end_time = datetime.now(timezone.utc)
    print(f"Running time is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")
