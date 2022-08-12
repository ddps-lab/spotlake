import argparse
import threading
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

from compare_data import compare
from load_price import get_price, preprocessing_price
from upload_data import upload_timestream, update_latest, save_raw


# get timestamp from argument
parser = argparse.ArgumentParser()
parser.add_argument('--timestamp', dest='timestamp', action='store')
args = parser.parse_args()
timestamp = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M")


MAX_SKIP = 2000
SKIP_NUM_LIST = [i*100 for i in range(MAX_SKIP)]

SAVE_DIR = './azure/'
SAVE_FILENAME = 'latest_azure_df.pkl'

WORKLOAD_COLS = ['InstanceTier', 'InstanceType', 'Region']
FEATURE_COLS = ['OndemandPrice', 'SpotPrice']


# collect azure price with multithreading
price_list = []
event = threading.Event()
with ThreadPoolExecutor(max_workers=16) as executor:
    for skip_num in SKIP_NUM_LIST:
        future = executor.submit(get_price, skip_num)
    event.wait()
    executor.shutdown(wait=True, cancel_futures=True)


# azure price dataframe preprocesing
current_df = pd.DataFrame(price_list)
current_df = preprocessing_price(current_df)


# check first execution
if SAVE_FILENAME not in os.listdir(SAVE_DIR):
    update_latest(current_df)
    save_raw(current_df, timestamp)
    upload_timestream(current_df, timestamp)
    exit()
    

# load previous dataframe, save current dataframe
previous_df = pd.read_pickle(SAVE_DIR + SAVE_FILENAME)
current_df.to_pickle(SAVE_DIR + SAVE_FILENAME)


# upload latest azure price to s3
update_latest(current_df)
save_raw(current_df, timestamp)


# compare and upload changed_df to timestream
changed_df = compare(previous_df, current_df, workload_cols, feature_cols)
upload_timestream(changed_df, timestamp)
