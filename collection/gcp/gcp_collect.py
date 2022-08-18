import requests
import pandas as pd

from load_pricelist import get_price
from load_vminstance_pricing import get_tbody, extract_price
from data_defenition import url_list
from data_defenition import machine_type_list, region_list

API_LINK = "https://cloudpricingcalculator.appspot.com/static/data/pricelist.json"

# load pricelist
data = requests.get(API_LINK).json()

updated_date = data['updated']
pricelist = data['gcp_price_list']

# get price from pricelist
output_pricelist = get_price(pricelist)

# get price from vm instance pricing page (crawling)
output_vminstance_pricing = {}
for machine_type in machine_type_list:
    output_vminstance_pricing[machine_type] = {}
    for region in region_list:
        output_vminstance_pricing[machine_type][region] = {}
        output_vminstance_pricing[machine_type][region]['ondemand'] = None
        output_vminstance_pricing[machine_type][region]['preemptible'] = None

tbody_list = []
for url in url_list:
    tbody_list.append(get_tbody(url))

for tbody in tbody_list:
    output_vminstance_pricing = extract_price(tbody, output_vminstance_pricing)


# preprocessing

# Issue : contain data of {'us', 'asia', 'australia', 'europe', 'asia-east', 'asia-southeast', 'asia-northeast'} regions or not? (only in pricelist)
# https://github.com/ddps-lab/spot-score/issues/115#issuecomment-1204745251

# filter N/A workload in pricelist
# -> get N/A workload from vm instance pricing data
# -> filtering in pricelist

# round matter
# -> 3rd or 4th?

# make final dataframe
df_pricelist = pd.DataFrame(output_pricelist)
df_pricelist.to_pickle('./pricelist.pkl')

df_vminstance_pricing = pd.DataFrame(output_vminstance_pricing)
df_vminstance_pricing.to_pickle('./vminstance_pricing.pkl')
