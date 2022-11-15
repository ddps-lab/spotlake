import boto3
import pickle
import pandas as pd
import argparse
import os
import time
from datetime import datetime, timedelta
from multiprocessing import Pool

from workload_binpacking import get_binpacked_workload
from load_price import get_spot_price, get_ondemand_price, get_regions
from load_spot_placement_score import get_sps
from load_spotinfo import get_spotinfo
from compare_data import compare
from upload_data import upload_timestream, update_latest, save_raw
from join_data import build_join_df

NUM_CPU = 2
LOCAL_PATH = '/home/ubuntu/spot-score/collection/aws/ec2_collector'

# get timestamp from argument
parser = argparse.ArgumentParser()
parser.add_argument('--timestamp', dest='timestamp', action='store')
args = parser.parse_args()
timestamp = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M")
date = args.timestamp.split("T")[0]

# need to change file location
workload = get_binpacked_workload(date)
credentials = pickle.load(open(f'{LOCAL_PATH}/user_cred_df_100_199.pkl', 'rb'))

mp_workload = []
for i in range(len(workload)):
    mp_workload.append((credentials.index[i], credentials.iloc[i], workload[i]))

session = boto3.session.Session()
regions = get_regions(session)
spot_price_df_list = []
with Pool(NUM_CPU) as p:
    spot_price_df_list = p.map(get_spot_price, regions)
spot_price_df = pd.concat(spot_price_df_list).reset_index(drop=True)

ondemand_price_df = get_ondemand_price(date)

spotinfo_df = get_spotinfo()

sps_df_list = []
with Pool(NUM_CPU) as p:
    sps_df_list = p.map(get_sps, mp_workload)
sps_df = pd.concat(sps_df_list).reset_index(drop=True)

current_df = build_join_df(spot_price_df, ondemand_price_df, spotinfo_df, sps_df)

update_latest(current_df, timestamp) # upload current data to S3
save_raw(current_df, timestamp)

if 'latest_df.pkl' not in os.listdir(f'{LOCAL_PATH}/'):
    pickle.dump(current_df, open(f"{LOCAL_PATH}/latest_df.pkl", "wb"))
    upload_timestream(current_df, timestamp)
    exit()

previous_df = pickle.load(open(f"{LOCAL_PATH}/latest_df.pkl", "rb")) # load previous data from local file system
pickle.dump(current_df, open(f"{LOCAL_PATH}/latest_df.pkl", "wb")) # write current data to local file system

workload_cols = ['InstanceType', 'Region', 'AZ']
feature_cols = ['SPS', 'IF', 'SpotPrice', 'OndemandPrice']

changed_df, removed_df = compare(previous_df, current_df, workload_cols, feature_cols) # compare previous_df and current_df to extract changed rows

upload_timestream(changed_df, timestamp)
upload_timestream(removed_df, timestamp)
