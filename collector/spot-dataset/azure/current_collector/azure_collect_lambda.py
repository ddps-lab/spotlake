import requests
import pandas as pd
import json
from datetime import datetime, timezone, timedelta
from const_config import AzureCollector, Storage
import boto3
from load_if import load_if
from merge_df import merge_df
from load_price import collect_price_with_multithreading
from upload_data import upload_timestream, update_latest, save_raw
from compare_data import compare
from utility import slack_msg_sender

STORAGE_CONST = Storage()
AZURE_CONST = AzureCollector()
STR_DATETIME = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M")
TIMESTAMP = datetime.strptime(STR_DATETIME, "%Y-%m-%dT%H:%M")

def azure_collector(timestamp):
    try:
        # collect azure price data with multithreading
        current_df = collect_price_with_multithreading()
        eviction_df = load_if()
        join_df = merge_df(current_df, eviction_df)

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
        changed_df = compare(previous_df, current_df, AZURE_CONST.DF_WORKLOAD_COLS, AZURE_CONST.DF_FEATURE_COLS)
        if not changed_df.empty:
            upload_timestream(changed_df, timestamp)

    except Exception as e:
        result_msg = """AZURE Exception!\n %s""" % (e)
        data = {'text': result_msg}
        slack_msg_sender.send_slack_message(result_msg)


def lambda_handler(event, context):
    azure_collector(TIMESTAMP)
    return {
        'statusCode': 200,
    }