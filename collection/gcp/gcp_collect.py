import requests
import pandas as pd

from load_pricelist import get_price, preprocessing_price
from load_vminstance_pricing import get_url_list, get_table, extract_price
from gcp_metadata import machine_type_list, region_list

API_LINK = "https://cloudpricingcalculator.appspot.com/static/data/pricelist.json"
PAGE_URL = "https://cloud.google.com/compute/vm-instance-pricing"

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
        output_vminstance_pricing[machine_type][region]['ondemand'] = None
        output_vminstance_pricing[machine_type][region]['preemptible'] = None

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
    'Vendor', 'InstanceType', 'Region', 'Calculator OnDemand Price', 'Calculator Preemptible Price', 'Calculator Savings'])
df_vminstance_pricing = pd.DataFrame(preprocessing_price(df_vminstance_pricing), columns=[
    'Vendor', 'InstanceType', 'Region', 'VM Instance OnDemand Price', 'VM Instance Preemptible Price', 'VM Instance Savings'])


# make final dataframe
df_current = pd.merge(df_pricelist, df_vminstance_pricing)
# need to add timestamp -> argparse


# compare and save
