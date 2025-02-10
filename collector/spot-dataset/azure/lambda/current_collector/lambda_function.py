import os
import json
import boto3
import pandas as pd
from datetime import datetime, timezone
from load_if import load_if
from load_price import collect_price_with_multithreading
from utils.merge_df import merge_price_eviction_df
from utils.compare_data import compare
from utils.upload_data import upload_timestream, update_latest, save_raw, query_selector, upload_cloudwatch
from utils.pub_service import send_slack_message, AZURE_CONST, STORAGE_CONST

WORKLOAD_COLS = ['InstanceTier', 'InstanceType', 'Region']
FEATURE_COLS = ['OndemandPrice', 'SpotPrice', 'IF']


def lambda_handler(event, _):
    event_time_utc = event.get("time")
    event_time_utc_datetime = datetime.strptime(event_time_utc, "%Y-%m-%dT%H:%M:%SZ")

    is_price_fetch_success = True
    is_if_fetch_success = True

    # collect azure price data with multithreading
    try:
        current_df = collect_price_with_multithreading()
    except Exception as e:
        result_msg = """AZURE PRICE MODULE EXCEPTION!\n %s""" % (e)
        data = {'text': result_msg}
        send_slack_message(result_msg)
        is_price_fetch_success = False

    try:
        eviction_df = load_if()
    except Exception as e:
        result_msg = """AZURE IF MODULE EXCEPTION!\n %s""" % (e)
        data = {'text': result_msg}
        send_slack_message(result_msg)
        is_if_fetch_success = False

    if is_price_fetch_success and is_if_fetch_success:
        join_df = merge_price_eviction_df(current_df, eviction_df)
    elif not is_price_fetch_success and is_if_fetch_success:
        join_df = eviction_df
    elif is_price_fetch_success and not is_if_fetch_success:
        current_df['IF'] = -1.0
        current_df = current_df[
            ['InstanceTier', 'InstanceType', 'Region', 'OndemandPrice', 'SpotPrice', 'Savings', 'IF']]
        join_df = current_df
    else:
        result_msg = """AZURE PRICE MODULE AND IF MODULE EXCEPTION!"""
        data = {'text': result_msg}
        send_slack_message(result_msg)
        return

    try:
        # load previous dataframe
        s3 = boto3.resource('s3')
        object = s3.Object(STORAGE_CONST.BUCKET_NAME, AZURE_CONST.S3_LATEST_DATA_SAVE_PATH)
        response = object.get()
        data = json.load(response['Body'])
        previous_df = pd.DataFrame(data)

        # upload latest azure price to s3
        update_latest(join_df, event_time_utc_datetime)
        save_raw(join_df, event_time_utc_datetime)

        # upload count-log to cloudwatch
        upload_cloudwatch(join_df, event_time_utc_datetime)

        # compare and upload changed_df to timestream
        changed_df = compare(previous_df, join_df, AZURE_CONST.DF_WORKLOAD_COLS, AZURE_CONST.DF_FEATURE_COLS)
        if not changed_df.empty:
            query_selector(changed_df)
            upload_timestream(changed_df, event_time_utc_datetime)

    except Exception as e:
        result_msg = """AZURE UPLOAD MODULE EXCEPTION!\n %s""" % (e)
        data = {'text': result_msg}
        send_slack_message(result_msg)
        if_exception_flag = False

    return {
        'statusCode': 200,
    }