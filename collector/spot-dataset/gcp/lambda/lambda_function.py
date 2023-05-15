import requests
import pandas as pd
import json
from datetime import datetime, timezone
import boto3
import botocore
from const_config import GcpCollector, Storage

from load_pricelist import get_price, preprocessing_price
from load_available_region_data import get_pricing_data, get_available_region_data, requests_retry_session
from get_metadata import get_aggregated_list, parsing_data_from_aggragated_list
from upload_data import save_raw, update_latest, upload_timestream
from compare_data import compare
from gcp_metadata import machine_type_list, region_list
from utility import slack_msg_sender

STORAGE_CONST = Storage()
GCP_CONST = GcpCollector()

def drop_negative(df):
    idx = df[(df['OnDemand Price']==-1.0) | (df['Spot Price'] == -1.0)].index
    df.drop(idx, inplace=True)
    return df


def gcp_collect(timestamp):
    # load pricelist
    response = requests_retry_session().get(GCP_CONST.API_LINK)

    if response.status_code != 200:
        slack_msg_sender.send_slack_message(f"GCP get pricelist : status code is {response.status_code}")
        raise Exception(f"GCP get pricelist : status code is {response.status_code}")

    data = response.json()

    pricelist = data['gcp_price_list']

    # get instance metadata
    df_raw_metadata = get_aggregated_list()
    available_region_lists, df_instance_metadata = parsing_data_from_aggragated_list(df_raw_metadata)

    # get price from pricelist
    output_pricelist = get_price(pricelist, df_instance_metadata, available_region_lists)
    df_pricelist = pd.DataFrame(output_pricelist)

    ##### Maybe, it doesn't need #####
    # # get pricing data from vm instance pricing tabale
    # pricing_data = get_pricing_data(GCP_CONST.PAGE_URL)
    # available_region_data = get_available_region_data(pricing_data)

    # preprocessing
    df_current = pd.DataFrame(preprocessing_price(df_pricelist), columns=[
        'InstanceType', 'Region', 'OnDemand Price', 'Spot Price'])
    
    # change unavailable region price into -1
    for idx, row in df_current.iterrows():
        if row['Region'].split('-')[0] + row['Region'].split('-')[1] not in available_region_data[row['InstanceType']]:
            df_current.loc[idx, 'OnDemand Price'] = -1
            df_current.loc[idx, 'Spot Price'] = -1

    # drop negative row
    drop_negative(df_current)

    # save current rawdata
    save_raw(df_current, timestamp)

    # check latest_data was in s3
    s3 = boto3.resource('s3')
    try:
        s3.Object(STORAGE_CONST.BUCKET_NAME, GCP_CONST.S3_LATEST_DATA_SAVE_PATH).load()

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            update_latest(df_current, timestamp)
            upload_timestream(df_current, timestamp)
            return
        else:
            slack_msg_sender.send_slack_message(e)
            print(e)

    # # get previous latest_data from s3
    object = s3.Object(STORAGE_CONST.BUCKET_NAME, GCP_CONST.S3_LATEST_DATA_SAVE_PATH)
    response = object.get()
    data = json.load(response['Body'])
    df_previous = pd.DataFrame(data)

    # update latest (current data -> latest data)
    update_latest(df_current, timestamp)

    # compare previous and current data
    workload_cols = ['InstanceType', 'Region']
    feature_cols = ['OnDemand Price', 'Spot Price']

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
    