import requests
import pandas as pd
import argparse
import os
from datetime import datetime

from load_pricelist import get_price, preprocessing_price
from load_vminstance_pricing import get_url_list, get_table, extract_price
from upload_data import save_raw, update_latest, upload_timestream
from compare_data import compare
from gcp_metadata import machine_type_list, region_list

API_LINK = "https://cloudpricingcalculator.appspot.com/static/data/pricelist.json"
PAGE_URL = "https://cloud.google.com/compute/vm-instance-pricing"
LOCAL_PATH = "/home/ubuntu/spot-score/collection/gcp"

# get timestamp from argument
parser = argparse.ArgumentParser()
parser.add_argument('--timestamp', dest='timestamp', action='store')
args = parser.parse_args()
timestamp = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M")

# load pricelist
data = requests.get(API_LINK).json()

updated_date = data['updated']
pricelist = data['gcp_price_list']

# get price from pricelist
output_pricelist = get_price(pricelist)
df_pricelist = pd.DataFrame(output_pricelist)

# get price from vm instance pricing page (crawling)
output_vminstance_pricing = {}
for machine_type in machine_type_list:
    output_vminstance_pricing[machine_type] = {}
    for region in region_list:
        output_vminstance_pricing[machine_type][region] = {}
        output_vminstance_pricing[machine_type][region]['ondemand'] = -1
        output_vminstance_pricing[machine_type][region]['preemptible'] = -1

url_list = get_url_list(PAGE_URL)

table_list = []
for url in url_list:
    table = get_table(url)
    if table != None:
        table_list.append(table)

for table in table_list:
    output_vminstance_pricing = extract_price(table, output_vminstance_pricing)

df_vminstance_pricing = pd.DataFrame(output_vminstance_pricing)


# preprocessing
df_pricelist = pd.DataFrame(preprocessing_price(df_pricelist), columns=[
    'InstanceType', 'Region', 'Calculator OnDemand Price', 'Calculator Preemptible Price'])
df_vminstance_pricing = pd.DataFrame(preprocessing_price(df_vminstance_pricing), columns=[
    'InstanceType', 'Region', 'VM Instance OnDemand Price', 'VM Instance Preemptible Price'])


# make final dataframe
df_current = pd.merge(df_pricelist, df_vminstance_pricing)

# update current data to S3
update_latest(df_current, timestamp)
save_raw(df_current, timestamp)

# compare latest and current data
if 'latest_df.pkl' not in os.listdir(f'{LOCAL_PATH}/'):
    df_current.to_pickle(f'{LOCAL_PATH}/latest_df.pkl')
    upload_timestream(df_current, timestamp)
    exit()

df_previous = pd.read_pickle(f'{LOCAL_PATH}/latest_df.pkl')
df_current.to_pickle(f'{LOCAL_PATH}/latest_df.pkl')

workload_cols = ['InstanceType', 'Region']
feature_cols = ['Calculator OnDemand Price', 'Calculator Preemptible Price', 'VM Instance OnDemand Price', 'VM Instance Preemptible Price']

changed_df, removed_df = compare(df_previous, df_current, workload_cols, feature_cols)

# write timestream
upload_timestream(changed_df, timestamp)
upload_timestream(removed_df, timestamp)
