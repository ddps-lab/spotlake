import pandas as pd
import json
from datetime import datetime, timezone
import boto3

from load_price import collect_price_with_multithreading
from upload_data import upload_timestream, update_latest, save_raw
from compare_data import compare

STR_DATETIME = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M")
TIMESTAMP = datetime.strptime(STR_DATETIME, "%Y-%m-%dT%H:%M")
KEY = 'latest_data/latest_azure.json'
BUCKET_NAME = 'spotlake'
WORKLOAD_COLS = ['InstanceTier', 'InstanceType', 'Region']
FEATURE_COLS = ['OndemandPrice', 'SpotPrice']


def azure_collector(timestamp):
    # collect azure price data with multithreading
    current_df = collect_price_with_multithreading()

    # load previous dataframe
    s3 = boto3.resource('s3')
    object = s3.Object(BUCKET_NAME, KEY)
    response = object.get()
    data = json.load(response['Body'])
    previous_df = pd.DataFrame(data)

    # upload latest azure price to s3
    update_latest(current_df, timestamp)
    save_raw(current_df, timestamp)

    # compare and upload changed_df to timestream
    changed_df = compare(previous_df, current_df, WORKLOAD_COLS, FEATURE_COLS)
    upload_timestream(changed_df, timestamp)


def lambda_handler(event, context):
    azure_collector(TIMESTAMP)
    return {
        'statusCode': 200,
    }
