import pandas as pd
import json
from datetime import datetime, timezone
import boto3
import botocore
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from const_config import GcpCollector, Storage
from load_pricelist import get_price, preprocessing_price, drop_negative
from get_metadata import get_aggregated_list, parsing_data_from_aggragated_list
from s3_management import save_raw, update_latest, upload_timestream, load_metadata
from compare_data import compare
from utility import slack_msg_sender

STORAGE_CONST = Storage()
GCP_CONST = GcpCollector()

def requests_retry_session(
        retries=3,
        backoff_factor=0.3,
        status_forcelist=(500, 501, 502, 503, 504),
        session=None
):
    session = session or requests.Session()
    retry = Retry(total=retries, read=retries, connect=retries, backoff_factor=backoff_factor,
                  status_forcelist=status_forcelist)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def gcp_collect(timestamp):
    # load pricelist
    response = requests_retry_session().get(GCP_CONST.API_LINK)

    if response.status_code != 200:
        slack_msg_sender.send_slack_message(f"GCP get pricelist : status code is {response.status_code}")
        raise Exception(f"GCP get pricelist : status code is {response.status_code}")

    data = response.json()
    pricelist = data['gcp_price_list']

    # get instance metadata and upload to s3
    df_raw_metadata = get_aggregated_list()
    parsing_data_from_aggragated_list(df_raw_metadata)

    df_instance_metadata = pd.DataFrame(load_metadata('instance_metadata'))
    available_region_lists = load_metadata('available_region_lists')

    # get price from pricelist
    output_pricelist = get_price(pricelist, df_instance_metadata, available_region_lists)
    df_pricelist = pd.DataFrame(output_pricelist)

    # preprocessing
    df_current = pd.DataFrame(preprocessing_price(df_pricelist), columns=[
        'InstanceType', 'Region', 'OnDemand Price', 'Spot Price'])
    
    # drop negative row
    df_current = drop_negative(df_current)

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
