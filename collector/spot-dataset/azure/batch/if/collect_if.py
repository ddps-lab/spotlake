import requests
import pandas as pd
import boto3
import argparse
import os
import sys
import pickle
import gzip
from datetime import datetime, timezone
from io import BytesIO

# Add parent directory to path to import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.azure_auth import get_sps_token_and_subscriptions
from utils.slack_msg_sender import send_slack_message

def get_data(sps_token, skip_token, retry=3):
    try:
        headers = {
            "Authorization": f"Bearer {sps_token}",
        }
        data = requests.post(
            "https://management.azure.com/providers/Microsoft.ResourceGraph/resources?api-version=2024-04-01",
            headers=headers,
            json={
                "query": """spotresources\n
            | where type =~ \"microsoft.compute/skuspotevictionrate/location\"\n
            | project location = location, props = parse_json(properties)\n
            | project location = location, skuName = props.skuName, evictionRate = props.evictionRate\n
            | where isnotempty(skuName) and isnotempty(evictionRate) and isnotempty(location)
            """,
                "options": {
                    "resultFormat": "objectArray",
                    "$skipToken": skip_token
                }
            }).json()

        if not "data" in data:
            raise ValueError

        if len(data['data']) > 0:
            return data
        else:
            return None

    except:
        if retry == 1:
            raise
        return get_data(sps_token, skip_token, retry - 1)


def load_if():
    try:
        sps_token, _ = get_sps_token_and_subscriptions()
        datas = []
        skip_token = ""

        while True:
            data = get_data(sps_token, skip_token)
            if not data:
                break

            datas += data["data"]
            skip_token = data.get("$skipToken", None)

            if skip_token is None:
                break

        if not datas:
            return None

        eviction_df = pd.DataFrame(datas)

        eviction_df['InstanceTier'] = eviction_df['skuName'].str.split('_', n=1, expand=True)[0].str.capitalize()
        eviction_df['InstanceType'] = eviction_df['skuName'].str.split('_', n=1, expand=True)[1].str.capitalize()

        frequency_map = {'0-5': 3.0, '5-10': 2.5, '10-15': 2.0, '15-20': 1.5, '20+': 1.0}
        eviction_df = eviction_df.replace({'evictionRate': frequency_map})

        eviction_df.rename(columns={'evictionRate': 'IF'}, inplace=True)
        eviction_df.rename(columns={'location': 'Region'}, inplace=True)

        eviction_df['OndemandPrice'] = -1.0
        eviction_df['SpotPrice'] = -1.0
        eviction_df['Savings'] = 1.0

        eviction_df = eviction_df[
            ['InstanceTier', 'InstanceType', 'Region', 'OndemandPrice', 'SpotPrice', 'Savings', 'IF']]

        return eviction_df

    except Exception as e:
        result_msg = """AZURE Exception when load_if\n %s""" % (e)
        send_slack_message(result_msg)
        raise e

def main():
    # Parse Arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--timestamp', dest='timestamp', action='store')
    args = parser.parse_args()

    if args.timestamp:
        if args.timestamp.endswith('Z'):
            timestamp_utc = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M:%SZ")
        else:
            timestamp_utc = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M")
    else:
        timestamp_utc = datetime.now(timezone.utc)
        timestamp_utc = timestamp_utc.replace(minute=((timestamp_utc.minute // 10) * 10), second=0, microsecond=0)

    print(f"Script execution start time (UTC): {timestamp_utc}")

    # S3 Config
    BUCKET_NAME = "spotlake"
    S3_PATH_PREFIX = "rawdata/azure/spot_if"
    date_path = timestamp_utc.strftime("%Y/%m/%d")
    time_str = timestamp_utc.strftime("%H-%M")
    
    try:
        # Collect Data
        start_time = datetime.now(timezone.utc)
        if_df = load_if()
        end_time = datetime.now(timezone.utc)
        print(f"Collection time: {(end_time - start_time).total_seconds()} seconds")

        if if_df is None or if_df.empty:
            print("No IF data collected.")
            return

        # Save to S3
        s3_client = boto3.client('s3')
        s3_key = f"{S3_PATH_PREFIX}/{date_path}/{time_str}_spot_if.pkl.gz"
        
        # Use /tmp for temp file
        local_path = f"/tmp/{time_str}_spot_if.pkl.gz"
        if_df.to_pickle(local_path, compression='gzip')

        with open(local_path, 'rb') as f:
            s3_client.upload_fileobj(f, BUCKET_NAME, s3_key)
        
        print(f"Uploaded to S3: {s3_key}")
        os.remove(local_path)

    except Exception as e:
        send_slack_message(f"Error in collect_if.py: {e}")
        raise e

if __name__ == "__main__":
    main()
