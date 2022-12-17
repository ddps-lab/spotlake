import argparse
import pandas as pd
import datetime
import os
from compare_data import compare
from const_config import AzureCollector
from load_price import collect_price_with_multithreading
from upload_data import upload_timestream, update_latest, save_raw

AZURE_CONST = AzureCollector()

# get timestamp from argument
parser = argparse.ArgumentParser()
parser.add_argument('--timestamp', dest='timestamp', action='store')
args = parser.parse_args()
timestamp = datetime.datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M")


#collect azure price data with multithreading
current_df = collect_price_with_multithreading()


# check first execution
if AZURE_CONST.SERVER_SAVE_FILENAME not in os.listdir(AZURE_CONST.SERVER_SAVE_DIR):
    update_latest(current_df, timestamp)
    save_raw(current_df, timestamp)
    upload_timestream(current_df, timestamp)
    exit()


# load previous dataframe, save current dataframe
previous_df = pd.read_pickle(AZURE_CONST.SERVER_SAVE_DIR + AZURE_CONST.SERVER_SAVE_FILENAME)
current_df.to_pickle(AZURE_CONST.SERVER_SAVE_DIR + AZURE_CONST.SERVER_SAVE_FILENAME)


# upload latest azure price to s3
update_latest(current_df, timestamp)
save_raw(current_df, timestamp)


# compare and upload changed_df to timestream
changed_df, removed_df = compare(previous_df, current_df, AZURE_CONST.DF_WORKLOAD_COLS, AZURE_CONST.DF_FEATURE_COLS)
upload_timestream(changed_df, timestamp)
upload_timestream(removed_df, timestamp)

