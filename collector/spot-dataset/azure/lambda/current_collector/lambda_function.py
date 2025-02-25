import pandas as pd
from datetime import datetime
from load_if import load_if
from load_price import collect_price_with_multithreading
from utils.merge_df import merge_price_saving_if_df
from utils.upload_data import update_latest_price_saving_if, save_raw_price_saving_if
from utils.pub_service import send_slack_message

def lambda_handler(event, _):
    event_time_utc = event.get("time")
    event_time_utc_datetime = datetime.strptime(event_time_utc, "%Y-%m-%dT%H:%M:%SZ")

    is_price_fetch_success = True
    is_if_fetch_success = True

    # collect azure price data with multithreading
    try:
        price_saving_df = collect_price_with_multithreading()
    except Exception as e:
        result_msg = """AZURE PRICE MODULE EXCEPTION!\n %s""" % (e)
        data = {'text': result_msg}
        send_slack_message(result_msg)
        is_price_fetch_success = False

    try:
        if_df = load_if()
    except Exception as e:
        result_msg = """AZURE IF MODULE EXCEPTION!\n %s""" % (e)
        data = {'text': result_msg}
        send_slack_message(result_msg)
        is_if_fetch_success = False

    if is_price_fetch_success and is_if_fetch_success:
        join_df = merge_price_saving_if_df(price_saving_df, if_df)
    elif not is_price_fetch_success and is_if_fetch_success:
        join_df = if_df
    elif is_price_fetch_success and not is_if_fetch_success:
        price_saving_df['IF'] = -1.0
        current_df = price_saving_df[
            ['InstanceTier', 'InstanceType', 'Region', 'OndemandPrice', 'SpotPrice', 'Savings', 'IF']]
        join_df = current_df
    else:
        result_msg = """AZURE PRICE MODULE AND IF MODULE EXCEPTION!"""
        data = {'text': result_msg}
        send_slack_message(result_msg)
        return

    try:
        # upload latest azure price to s3
        update_latest_price_saving_if(join_df, event_time_utc_datetime)
        save_raw_price_saving_if(join_df, event_time_utc_datetime)

    except Exception as e:
        result_msg = """AZURE UPLOAD MODULE EXCEPTION!\n %s""" % (e)
        data = {'text': result_msg}
        send_slack_message(result_msg)
        if_exception_flag = False

    return {
        'statusCode': 200,
    }