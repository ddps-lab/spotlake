import boto3
import pickle
import json
import pandas as pd
import argparse
from datetime import datetime, timedelta
from botocore.config import Config
from botocore.exceptions import ClientError

from load_price import get_spot_price, get_ondemand_price
from load_spot_placement_score import get_sps
from load_spotinfo import get_spotinfo

# get timestamp from argument
parser = argparse.ArgumentParser()
parser.add_argument('--timestamp', dest='timestamp', action='store')
args = parser.parse_args()
timestamp = datetime.strptime(args.timestamp, "%Y-%m-%d %H:%M")

spot_price_df = get_spot_price()
ondemand_price_df = get_ondemand_price()
spotinfo_df = get_spotinfo()
sps_df = get_sps()

# spotlake_df = JOIN(spot_price_df, ondemand_price_df, spotinfo_df, sps_df)
# upload_timestream(spotlake_df, timestamp)