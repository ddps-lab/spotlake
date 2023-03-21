import boto3
import botocore
import pickle
import pandas as pd
import argparse
import json
import sys
import os
import time
import gzip
from datetime import datetime, timedelta
from multiprocessing import Pool

from workload_binpacking import get_binpacked_workload
from load_price import get_spot_price, get_ondemand_price, get_regions
from load_spot_placement_score import get_sps
from load_spotinfo import get_spotinfo
from compare_data import compare
from upload_data import upload_timestream, update_latest, update_query_selector, save_raw
from join_data import build_join_df

sys.path.append('/home/ubuntu/spotlake/utility')

from slack_msg_sender import send_slack_message

sys.path.append('/home/ubuntu/spotlake')

from const_config import AwsCollector, Storage

STORAGE_CONST = Storage()
AWS_CONST = AwsCollector()

NUM_CPU = 2
s3 = boto3.resource('s3')

# get timestamp from argument
parser = argparse.ArgumentParser()
parser.add_argument('--timestamp', dest='timestamp', action='store')
args = parser.parse_args()
timestamp = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M")
date = args.timestamp.split("T")[0]

# need to change file location
workload = None
try:
    workload = pickle.load(gzip.open(s3.Object(STORAGE_CONST.BUCKET_NAME, f'{AWS_CONST.S3_WORKLOAD_SAVE_PATH}/{"/".join(date.split("-"))}/binpacked_workloads.pkl.gz').get()['Body']))
except Exception as e:
    send_slack_message(e)
    try :
        workload = pickle.load(open(f'{AWS_CONST.LOCAL_PATH}/workloads.pkl', 'rb'))
    except:
        workload = pickle.load(s3.Object(STORAGE_CONST.BUCKET_NAME, f'{AWS_CONST.S3_LOCAL_FILES_SAVE_PATH}/workloads.pkl').get()['Body'])

credentials = None
try:
    credentials = pickle.load(open(f'{AWS_CONST.LOCAL_PATH}/user_cred_df.pkl', 'rb'))
except:
    credentials = pickle.load(s3.Object(STORAGE_CONST.BUCKET_NAME, f'{AWS_CONST.S3_LOCAL_FILES_SAVE_PATH}/user_cred_df.pkl').get()['Body'])

mp_workload = []
for i in range(len(workload)):
    mp_workload.append((credentials.index[i], credentials.iloc[i], workload[i]))
try:
    session = boto3.session.Session()
    regions = get_regions(session)
    spot_price_df_list = []
    with Pool(NUM_CPU) as p:
        spot_price_df_list = p.map(get_spot_price, regions)
    spot_price_df = pd.concat(spot_price_df_list).reset_index(drop=True)
except Exception as e:
    send_slack_message(e)

try:
    ondemand_price_df = get_ondemand_price(date)
except Exception as e:
    send_slack_message(e)

try:
    spotinfo_df = get_spotinfo()
except Exception as e:
    send_slack_message(e)

try:
    sps_df_list = []
    with Pool(NUM_CPU) as p:
        sps_df_list = p.map(get_sps, mp_workload)
    sps_df = pd.concat(sps_df_list).reset_index(drop=True)
except Exception as e:
    send_slack_message(e)

current_df = None
try:
    current_df = build_join_df(spot_price_df, ondemand_price_df, spotinfo_df, sps_df)
except Exception as e:
    send_slack_message(e)

previous_df = None
try:
    try:
        previous_df = pd.DataFrame(json.load(open(f"{AWS_CONST.LOCAL_PATH}/latest_aws.json", "r")))
    except:
        previous_df = pd.DataFrame(json.load(s3.Object(STORAGE_CONST.BUCKET_NAME, f'{AWS_CONST.S3_LATEST_DATA_SAVE_PATH}').get()['Body'].read()))
    previous_df = previous_df.drop(columns=['id', 'time'])
except:
    update_latest(current_df, timestamp)
    try:
        upload_timestream(current_df, timestamp)
    except Exception as e:
        send_slack_message(e)
    exit()

try:
    update_latest(current_df, timestamp) # upload current data to S3
    save_raw(current_df, timestamp)
except Exception as e:
    send_slack_message(e)

workload_cols = ['InstanceType', 'Region', 'AZ']
feature_cols = ['SPS', 'IF', 'SpotPrice', 'OndemandPrice']

changed_df, removed_df = compare(previous_df, current_df, workload_cols, feature_cols) # compare previous_df and current_df to extract changed rows

try:
    upload_timestream(changed_df, timestamp)
    upload_timestream(removed_df, timestamp)
except Exception as e:
    send_slack_message(e)

try:
    update_query_selector(changed_df)
except Exception as e:
    send_slack_message(e)
