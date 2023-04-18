import os
import json
import boto3
import slack_msg_sender
import pandas as pd
from const_config import AzureCollector, Storage
from datetime import datetime, timezone
from load_if import load_if
from merge_df import merge_df
from load_price import collect_price_with_multithreading
from upload_data import upload_timestream, update_latest, save_raw, query_selector
from compare_data import compare

STORAGE_CONST = Storage()
AZURE_CONST = AzureCollector()

BUCKET_NAME = os.environ.get('BUCKET_NAME')
KEY = os.environ.get('S3_LATEST_DATA_SAVE_PATH')
WORKLOAD_COLS = ['InstanceTier', 'InstanceType', 'Region']
FEATURE_COLS = ['OndemandPrice', 'SpotPrice', 'IF']
str_datetime = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M")
timestamp = datetime.strptime(str_datetime, "%Y-%m-%dT%H:%M")


def azure_collector(timestamp):
    price_exception_flag = True
    if_exception_flag = True
    
    # collect azure price data with multithreading
    try:
        current_df = collect_price_with_multithreading()
    except Exception as e:
        result_msg = """AZURE PRICE MODULE EXCEPTION!\n %s""" % (e)
        data = {'text': result_msg}
        slack_msg_sender.send_slack_message(result_msg)
        price_exception_flag = False
    
    try:
        eviction_df = load_if()
    except Exception as e:
        result_msg = """AZURE IF MODULE EXCEPTION!\n %s""" % (e)
        data = {'text': result_msg}
        slack_msg_sender.send_slack_message(result_msg)
        if_exception_flag = False

    if price_exception_flag and if_exception_flag:
        join_df = merge_df(current_df, eviction_df)
    elif not price_exception_flag and if_exception_flag:
        join_df = eviction_df
    elif price_exception_flag and not if_exception_flag:
        current_df['IF'] = -1.0
        current_df = current_df[['InstanceTier', 'InstanceType', 'Region',	'OndemandPrice', 'SpotPrice', 'Savings', 'IF']]
        join_df = current_df

    # load previous dataframe
    s3 = boto3.resource('s3')
    object = s3.Object(STORAGE_CONST.BUCKET_NAME, AZURE_CONST.S3_LATEST_DATA_SAVE_PATH)
    response = object.get()
    data = json.load(response['Body'])
    previous_df = pd.DataFrame(data)

    # upload latest azure price to s3
    update_latest(join_df, timestamp)
    save_raw(join_df, timestamp)

    # compare and upload changed_df to timestream
    changed_df = compare(previous_df, join_df, AZURE_CONST.DF_WORKLOAD_COLS, AZURE_CONST.DF_FEATURE_COLS)
    if not changed_df.empty:
        query_selector(changed_df)
        upload_timestream(changed_df, timestamp)



def lambda_handler(event, context):
    azure_collector(timestamp)
    return {
        'statusCode': 200,
    }
