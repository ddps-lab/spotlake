import argparse
import pandas as pd
import datetime
import os
from compare_data import compare
from load_price import collect_price_with_multithreading
from upload_data import upload_timestream, update_latest, save_raw

SAVE_DIR = '/home/ubuntu/spot-score/collection/azure/'
SAVE_FILENAME = 'latest_azure_df.pkl'
WORKLOAD_COLS = ['InstanceTier', 'InstanceType', 'Region']
FEATURE_COLS = ['OndemandPrice', 'SpotPrice']


# get timestamp from argument
parser = argparse.ArgumentParser()
parser.add_argument('--timestamp', dest='timestamp', action='store')
args = parser.parse_args()
timestamp = datetime.datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M")


#collect azure price data with multithreading
current_df = collect_price_with_multithreading()


# check first execution
if SAVE_FILENAME not in os.listdir(SAVE_DIR):
    update_latest(current_df, timestamp)
    save_raw(current_df, timestamp)
    upload_timestream(current_df, timestamp)
    exit()


# load previous dataframe, save current dataframe
previous_df = pd.read_pickle(SAVE_DIR + SAVE_FILENAME)
current_df.to_pickle(SAVE_DIR + SAVE_FILENAME)


# upload latest azure price to s3
update_latest(current_df, timestamp)
save_raw(current_df, timestamp)


# compare and upload changed_df to timestream
changed_df, removed_df = compare(previous_df, current_df, WORKLOAD_COLS, FEATURE_COLS)
upload_timestream(changed_df, timestamp)
upload_timestream(removed_df, timestamp)

