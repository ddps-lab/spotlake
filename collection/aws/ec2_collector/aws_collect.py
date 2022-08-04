import boto3
import pickle
import pandas as pd
import argparse
from datetime import datetime, timedelta

from load_price import get_spot_price, get_ondemand_price
from load_spot_placement_score import get_sps
from load_spotinfo import get_spotinfo
from compare_data import compare

# get timestamp from argument
parser = argparse.ArgumentParser()
parser.add_argument('--timestamp', dest='timestamp', action='store')
args = parser.parse_args()
timestamp = datetime.strptime(args.timestamp, "%Y-%m-%d %H:%M")

spot_price_df = get_spot_price()
ondemand_price_df = get_ondemand_price()
spotinfo_df = get_spotinfo()
sps_df = get_sps()

# current_df = JOIN(spot_price_df, ondemand_price_df, spotinfo_df, sps_df)
# previous_df = pickle.load(open("./latest_df.pkl", "rb")) # load previous data from local file system
# pickle.dump(current_df, open("./latest_df.pkl", "wb")) # write current data to local file system

# update_latest(current_df, timestamp) # upload current data to S3

workload_cols = ['InstanceType', 'Region', 'AZ']
feature_cols = ['SPS', 'IF', 'Savings']

# changed_df = compare(previous_df, current_df, workload_cols, feature_cols) # compare previous_df and current_df to extract changed rows

# upload_timestream(changed_df, timestamp)
