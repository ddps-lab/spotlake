import requests
import pandas as pd
import json
from datetime import datetime, timezone, timedelta
import boto3
import botocore
from load_if import load_if
from merge_df import merge_df
from load_price import collect_price_with_multithreading
from upload_data import upload_timestream, update_latest, save_raw
from compare_data import compare
from util.slack_msg_sender import send_slack_message

BUCKET_NAME = 'spotlake'
SAVE_FILENAME = 'latest_azure_df.pkl'
WORKLOAD_COLS = ['InstanceTier', 'InstanceType', 'Region']
FEATURE_COLS = ['OndemandPrice', 'SpotPrice', 'EvictionRate']
str_datetime = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M")
timestamp = datetime.strptime(str_datetime, "%Y-%m-%dT%H:%M")

KEY = 'latest_data/latest_azure.json'
url = 'https://hooks.slack.com/services/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'  # slackurl


def azure_collector(timestamp):
    try:
        # collect azure price data with multithreading
        current_df = collect_price_with_multithreading()
        eviction_df = load_if()
        join_df = merge_df(current_df, eviction_df)

        # load previous dataframe
        s3 = boto3.resource('s3')
        object = s3.Object(BUCKET_NAME, KEY)
        response = object.get()
        data = json.load(response['Body'])
        previous_df = pd.DataFrame(data)

        # upload latest azure price to s3
        update_latest(current_df, timestamp)
        save_raw(join_df, timestamp)

        # compare and upload changed_df to timestream
        changed_df = compare(previous_df, current_df, WORKLOAD_COLS, FEATURE_COLS)
        if not changed_df.empty:
            upload_timestream(changed_df, timestamp)


    except Exception as e:
        result_msg = """AZURE Exception!\n %s""" % (e)
        data = {'text': result_msg}
        resp = requests.post(url=url, json=data)
        send_slack_message(result_msg)


def lambda_handler(event, context):
    azure_collector(timestamp)
    return {
        'statusCode': 200,
    }
