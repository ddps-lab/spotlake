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
from utils.constants import STORAGE_CONST

def get_data(sps_token, skip_token, retry=3):
    """Old query: uses parse_json(properties).skuName"""
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


def get_data_v2(sps_token, skip_token, retry=3):
    """New query: uses sku.name (MS official docs pattern)"""
    try:
        headers = {
            "Authorization": f"Bearer {sps_token}",
        }
        data = requests.post(
            "https://management.azure.com/providers/Microsoft.ResourceGraph/resources?api-version=2024-04-01",
            headers=headers,
            json={
                "query": """spotresources
            | where type =~ "microsoft.compute/skuspotevictionrate/location"
            | project skuName = tostring(sku.name), location, evictionRate = tostring(properties.evictionRate)
            | where isnotempty(skuName) and isnotempty(evictionRate) and isnotempty(location)
            """,
                "options": {
                    "resultFormat": "objectArray",
                    "$skipToken": skip_token
                }
            }).json()

        if "data" not in data:
            raise ValueError(f"No 'data' key in response: {list(data.keys())}")

        if len(data['data']) > 0:
            return data
        else:
            return None

    except:
        if retry == 1:
            raise
        return get_data_v2(sps_token, skip_token, retry - 1)


def _paginate_query(query_fn, sps_token):
    """Run a paginated Resource Graph query function, return all raw records."""
    datas = []
    skip_token = ""
    while True:
        data = query_fn(sps_token, skip_token)
        if not data:
            break
        datas += data["data"]
        skip_token = data.get("$skipToken", None)
        if skip_token is None:
            break
    return datas


def _process_eviction_df(datas):
    """Convert raw query results into a cleaned eviction DataFrame."""
    if not datas:
        return pd.DataFrame()

    eviction_df = pd.DataFrame(datas)

    sku_split = eviction_df['skuName'].str.split('_', n=1, expand=True)
    eviction_df['InstanceTier'] = sku_split[0]
    eviction_df['InstanceType'] = sku_split[1]

    frequency_map = {'0-5': 3.0, '5-10': 2.5, '10-15': 2.0, '15-20': 1.5, '20+': 1.0}
    eviction_df = eviction_df.replace({'evictionRate': frequency_map})

    eviction_df.rename(columns={'evictionRate': 'IF'}, inplace=True)
    eviction_df.rename(columns={'location': 'Region'}, inplace=True)

    eviction_df['OndemandPrice'] = -1.0
    eviction_df['SpotPrice'] = -1.0
    eviction_df['Savings'] = 1.0

    eviction_df = eviction_df[
        ['InstanceTier', 'InstanceType', 'Region', 'OndemandPrice', 'SpotPrice', 'Savings', 'IF']]

    # Filter out Gov regions (align with Price collection)
    FILTER_LOCATIONS = ['GOV', 'DoD', 'China', 'Germany']
    eviction_df = eviction_df[
        ~eviction_df['Region'].str.split().str[0].str.upper().isin(FILTER_LOCATIONS)
    ]

    eviction_df = eviction_df.drop_duplicates(subset=['InstanceTier', 'InstanceType', 'Region'])

    return eviction_df


def load_if():
    try:
        sps_token, _ = get_sps_token_and_subscriptions()

        # --- Run both queries for comparison ---
        old_datas = _paginate_query(get_data, sps_token)
        new_datas = _paginate_query(get_data_v2, sps_token)

        old_df = _process_eviction_df(old_datas)
        new_df = _process_eviction_df(new_datas)

        old_count_raw = len(old_datas)
        new_count_raw = len(new_datas)
        old_count = len(old_df)
        new_count = len(new_df)

        # --- Comparison logging ---
        print("=" * 60)
        print("[IF QUERY COMPARISON]")
        print(f"  Old query (properties.skuName): {old_count_raw} raw records -> {old_count} after processing")
        print(f"  New query (sku.name):           {new_count_raw} raw records -> {new_count} after processing")
        print(f"  Difference (raw):               {new_count_raw - old_count_raw:+d} records")
        print(f"  Difference (processed):         {new_count - old_count:+d} records")

        if new_count > 0 and old_count > 0:
            # Show overlap analysis
            old_keys = set(zip(old_df['InstanceTier'], old_df['InstanceType'], old_df['Region']))
            new_keys = set(zip(new_df['InstanceTier'], new_df['InstanceType'], new_df['Region']))
            only_old = old_keys - new_keys
            only_new = new_keys - old_keys
            common = old_keys & new_keys
            print(f"  Common keys:    {len(common)}")
            print(f"  Only in old:    {len(only_old)}")
            print(f"  Only in new:    {len(only_new)}")
            if only_old:
                samples = list(only_old)[:3]
                print(f"  Old-only samples: {samples}")
            if only_new:
                samples = list(only_new)[:3]
                print(f"  New-only samples: {samples}")
        print("=" * 60)

        # --- Use NEW query result ---
        if new_df.empty:
            print("[IF QUERY COMPARISON] WARNING: New query returned 0 results. Falling back to old query.")
            return old_df if not old_df.empty else None

        return new_df

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
            s3_client.upload_fileobj(f, STORAGE_CONST.WRITE_BUCKET_NAME, s3_key)
        
        print(f"Uploaded to S3: {s3_key}")
        os.remove(local_path)

    except Exception as e:
        send_slack_message(f"Error in collect_if.py: {e}")
        raise e

if __name__ == "__main__":
    main()
