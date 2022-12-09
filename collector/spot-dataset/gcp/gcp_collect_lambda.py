import requests
import pandas as pd
import json
from datetime import datetime, timezone 
import boto3
import botocore

from load_pricelist import get_price, preprocessing_price
from load_vminstance_pricing import get_url_list, get_table, extract_price
from upload_data import save_raw, update_latest, upload_timestream
from compare_data import compare
from gcp_metadata import machine_type_list, region_list
from utility import slack_msg_sender

API_LINK = "https://cloudpricingcalculator.appspot.com/static/data/pricelist.json"
PAGE_URL = "https://cloud.google.com/compute/vm-instance-pricing"
BUCKET_NAME = 'spotlake'
KEY = 'latest_data/latest_gcp.json'


def gcp_collect(timestamp) :
    # load pricelist
    data = requests.get(API_LINK).json()
    
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
    
    # save current rawdata
    save_raw(df_current, timestamp)
    
    # check latest_data was in s3
    s3 = boto3.resource('s3')
    try:
        s3.Object(BUCKET_NAME, KEY).load()
    
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            update_latest(df_current, timestamp)
            upload_timestream(df_current, timestamp)
            return
        else:
            slack_msg_sender.send_slack_message(e)
            print(e)
    
    # get previous latest_data from s3
    object = s3.Object(BUCKET_NAME, KEY)
    response = object.get()
    data = json.load(response['Body'])
    df_previous = pd.DataFrame(data)
    
    # update latest (current data -> latest data)
    update_latest(df_current, timestamp)
    
    # compare previous and current data
    workload_cols = ['InstanceType', 'Region']
    feature_cols = ['Calculator OnDemand Price', 'Calculator Preemptible Price', 'VM Instance OnDemand Price', 'VM Instance Preemptible Price']
        
    changed_df, removed_df = compare(df_previous, df_current, workload_cols, feature_cols)
        
    # wirte timestream
    upload_timestream(changed_df, timestamp)
    upload_timestream(removed_df, timestamp)

    
def lambda_handler(event, context):
    str_datetime = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M")
    timestamp = datetime.strptime(str_datetime, "%Y-%m-%dT%H:%M")
    
    gcp_collect(timestamp)
    
    return {
        "statusCode": 200
    }
