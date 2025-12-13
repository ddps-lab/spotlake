import requests
import pandas as pd
import numpy as np
import threading
import time
import argparse
import boto3
import os
import sys
import pickle
import gzip
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

# Add parent directory to path to import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.slack_msg_sender import send_slack_message

# Constants
GET_PRICE_URL = "https://prices.azure.com/api/retail/prices?currencyCode='USD'&$filter=serviceName eq 'Virtual Machines' and priceType eq 'Consumption'&$skip="
FILTER_LOCATIONS = ['GOV', 'DoD', 'China', 'Germany']
MAX_SKIP = 2000

# Globals for threading
price_list = []
response_dict = {}
event = threading.Event()
lock = threading.Lock()

def get_instaceTier(armSkuName):
    split_armSkuName = armSkuName.split('_')
    if len(split_armSkuName) == 0:
        return np.nan
    if split_armSkuName[0] == 'Standard' or split_armSkuName[0] == 'Basic':
        InstanceTier = split_armSkuName[0]
    else:
        InstanceTier = np.nan
    return InstanceTier

def get_instaceType(armSkuName):
    split_armSkuName = armSkuName.split('_')
    if len(split_armSkuName) == 0:
        return np.nan
    if split_armSkuName[0] == 'Standard' or split_armSkuName[0] == 'Basic':
        if len(split_armSkuName) == 1:
            return np.nan
        InstanceType = '_'.join(split_armSkuName[1:])
    else:
        InstanceType = split_armSkuName[0]
    return InstanceType

def get_price(skip_num):
    get_link = GET_PRICE_URL + str(skip_num)
    try:
        response = requests.get(get_link)
        for _ in range(5):
            if response.status_code == 200:
                break
            else:
                time.sleep(2)
                response = requests.get(get_link)

        if response.status_code != 200:
            lock.acquire()
            response_dict[response.status_code] = response_dict.get(response.status_code, 0) + 1
            lock.release()
            return

        price_data = list(response.json()['Items'])
        if not price_data:
            event.set()
            return

        lock.acquire()
        price_list.extend(price_data)
        lock.release()

    except Exception as e:
        print(f"Error in get_price for skip_num {skip_num}: {e}")

def preprocessing_price(df):
    df = df[~df['productName'].str.contains('Windows')]
    df = df[~df['meterName'].str.contains('Priority')]
    df = df[~df['meterName'].str.contains('Expired')]
    df = df[~df['location'].str.split().str[0].isin(FILTER_LOCATIONS)]

    ondemand_df = df[~df['meterName'].str.contains('Spot')]
    spot_df = df[df['meterName'].str.contains('Spot')]

    list_meterName = list(spot_df['meterName'].str.split(' ').str[:-1].apply(' '.join))
    spot_df = spot_df.drop(['meterName'], axis=1)
    spot_df['meterName'] = list_meterName

    spot_df = spot_df[['location', 'armRegionName', 'armSkuName', 'retailPrice', 'meterName', 'effectiveStartDate']]
    spot_df.rename(columns={'retailPrice': 'SpotPrice'}, inplace=True)
    ondemand_df = ondemand_df[['location', 'armRegionName', 'armSkuName', 'retailPrice', 'meterName', 'effectiveStartDate']]
    ondemand_df.rename(columns={'retailPrice': 'OndemandPrice'}, inplace=True)

    drop_idx = ondemand_df.loc[(ondemand_df['location'] == '')].index
    ondemand_df.drop(drop_idx, inplace=True)

    join_df = pd.merge(ondemand_df, spot_df,
                       left_on=['location', 'armRegionName', 'armSkuName', 'meterName'],
                       right_on=['location', 'armRegionName', 'armSkuName', 'meterName'],
                       how='outer')

    join_df = join_df.dropna(subset=['SpotPrice'])

    join_df.loc[join_df['OndemandPrice'] == 0, 'OndemandPrice'] = None
    join_df['Savings'] = (join_df['OndemandPrice'] - join_df['SpotPrice']) / join_df['OndemandPrice'] * 100

    join_df['InstanceTier'] = join_df['armSkuName'].apply(lambda armSkuName: get_instaceTier(armSkuName))
    join_df['InstanceType'] = join_df['armSkuName'].apply(lambda armSkuName: get_instaceType(armSkuName))

    join_df = join_df.reindex(columns=['InstanceTier', 'InstanceType', 'location', 'armRegionName', 'OndemandPrice', 'SpotPrice', 'Savings'])
    join_df.rename(columns={'location': 'Region'}, inplace=True)

    return join_df

def collect_price_with_multithreading():
    global price_list, response_dict, event
    price_list = []
    response_dict = {}
    event.clear()
    
    SKIP_NUM_LIST = [i*1000 for i in range(MAX_SKIP)]

    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = []
        for skip_num in SKIP_NUM_LIST:
            if event.is_set():
                break
            futures.append(executor.submit(get_price, skip_num))
        
        while not event.is_set():
            if all(f.done() for f in futures) and len(futures) == len(SKIP_NUM_LIST):
                break
            time.sleep(1)
        
        executor.shutdown(wait=True, cancel_futures=True)

    if response_dict:
        for i in response_dict:
            send_slack_message(f"[Azure Collector]: {i} respones occurred {response_dict[i]} times.")

    if not price_list:
        return pd.DataFrame()

    price_df = pd.DataFrame(price_list)
    savings_df = preprocessing_price(price_df)
    savings_df = savings_df.drop_duplicates(subset=['InstanceTier', 'InstanceType', 'Region'], keep='first')

    return savings_df

def main():
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

    BUCKET_NAME = "spotlake"
    S3_PATH_PREFIX = "rawdata/azure/spot_price"
    date_path = timestamp_utc.strftime("%Y/%m/%d")
    time_str = timestamp_utc.strftime("%H-%M")

    try:
        start_time = datetime.now(timezone.utc)
        
        savings_df = collect_price_with_multithreading()
        
        if savings_df.empty:
            print("No price data collected.")
            return
        
        end_time = datetime.now(timezone.utc)
        print(f"Collection time: {(end_time - start_time).total_seconds()} seconds")

        # Save to S3
        s3_client = boto3.client('s3')
        s3_key = f"{S3_PATH_PREFIX}/{date_path}/{time_str}_spot_price.pkl.gz"
        
        local_path = f"/tmp/{time_str}_spot_price.pkl.gz"
        savings_df.to_pickle(local_path, compression='gzip')

        with open(local_path, 'rb') as f:
            s3_client.upload_fileobj(f, BUCKET_NAME, s3_key)
        
        print(f"Uploaded to S3: {s3_key}")
        os.remove(local_path)

    except Exception as e:
        send_slack_message(f"Error in collect_price.py: {e}")
        raise e

if __name__ == "__main__":
    main()
