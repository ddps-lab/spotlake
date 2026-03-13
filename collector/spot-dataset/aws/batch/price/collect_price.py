# ------ import module ------
from datetime import datetime, timezone
import argparse
import concurrent.futures
import gzip
import io
import json
import pickle
import random
import time

import boto3.session
from botocore.config import Config
import pandas as pd

# ------ import user module ------
from utility.slack_msg_sender import send_slack_message
from load_price import get_spot_price, get_regions


MAX_RETRY_ATTEMPTS = 3
INITIAL_BACKOFF_SECONDS = 1.0
MAX_BACKOFF_SECONDS = 8.0
THROTTLE_BACKOFF_MULTIPLIER = 2.0


def is_throttling_exception(err):
    response = getattr(err, "response", None)
    if isinstance(response, dict):
        error_code = str(response.get("Error", {}).get("Code", ""))
        if "Throttl" in error_code or error_code in ("TooManyRequestsException", "RequestLimitExceeded"):
            return True
    err_text = str(err)
    return ("ThrottlingException" in err_text) or ("Rate exceeded" in err_text)


def retry_with_backoff(func, label, max_attempts=MAX_RETRY_ATTEMPTS, return_attempts=False):
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            result = func()
            if return_attempts:
                return result, attempt
            return result
        except Exception as e:
            last_error = e
            if attempt == max_attempts:
                break
            backoff_base = INITIAL_BACKOFF_SECONDS * (2 ** (attempt - 1))
            throttled = is_throttling_exception(e)
            if throttled:
                backoff_base *= THROTTLE_BACKOFF_MULTIPLIER
            sleep_seconds = min(MAX_BACKOFF_SECONDS, backoff_base) + random.uniform(0.0, 0.25)
            error_class = "throttling" if throttled else "generic"
            print(
                f"[WARN] {label} failed ({attempt}/{max_attempts}, {error_class}): {e}. "
                f"Retrying in {sleep_seconds:.2f}s..."
            )
            time.sleep(sleep_seconds)
    raise RuntimeError(f"{label} failed after {max_attempts} attempts: {last_error}")


def load_snapshot_from_s3(s3_client, bucket_name, key):
    try:
        obj = s3_client.get_object(Bucket=bucket_name, Key=key)
        with gzip.GzipFile(fileobj=obj["Body"], mode="rb") as gz:
            df = pickle.load(gz)
        if isinstance(df, pd.DataFrame):
            return df
        print(f"[WARN] Snapshot at {key} is not a DataFrame. Ignoring.")
        return pd.DataFrame()
    except Exception as e:
        print(f"[INFO] No snapshot loaded from s3://{bucket_name}/{key}: {e}")
        return pd.DataFrame()


def dump_snapshot_to_s3(s3_client, bucket_name, key, df):
    buffer = io.BytesIO()
    pickle.dump(df, buffer)
    buffer.seek(0)

    compressed_buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=compressed_buffer, mode="wb") as gz:
        gz.write(buffer.getvalue())
    compressed_buffer.seek(0)

    s3_client.upload_fileobj(compressed_buffer, bucket_name, key)


def apply_region_fallback(current_df, snapshot_df, failed_regions, dataset_name, key_columns):
    failed_regions = sorted(set(failed_regions))
    if not failed_regions:
        return current_df, [], []

    if snapshot_df.empty:
        print(f"[WARN] {dataset_name}: fallback snapshot is empty. unresolved={failed_regions}")
        return current_df, [], failed_regions

    if "Region" not in snapshot_df.columns:
        print(f"[WARN] {dataset_name}: fallback snapshot has no Region column. unresolved={failed_regions}")
        return current_df, [], failed_regions

    fallback_chunks = []
    fallback_applied = []
    unresolved = []

    for region in failed_regions:
        region_rows = snapshot_df[snapshot_df["Region"] == region]
        if region_rows.empty:
            unresolved.append(region)
            continue
        fallback_chunks.append(region_rows)
        fallback_applied.append(region)

    if not fallback_chunks:
        print(f"[WARN] {dataset_name}: no rows found in snapshot for failed regions. unresolved={unresolved}")
        return current_df, [], unresolved

    merged_df = pd.concat([current_df] + fallback_chunks, ignore_index=True)
    merged_df = merged_df.drop_duplicates(subset=key_columns, keep="first").reset_index(drop=True)
    return merged_df, fallback_applied, unresolved


# get ondemand price by all instance type in single region
def get_ondemand_price_region(region, pricing_client):
    response_list = []

    filters = [
        {'Type': 'TERM_MATCH', 'Field': 'capacitystatus', 'Value': 'Used'},
        {'Type': 'TERM_MATCH', 'Field': 'regionCode', 'Value': region},
        {'Type': 'TERM_MATCH', 'Field': 'tenancy', 'Value': 'Shared'},
        {'Type': 'TERM_MATCH', 'Field': 'operatingSystem', 'Value': 'Linux'},
        {'Type': 'TERM_MATCH', 'Field': 'preInstalledSw', 'Value': 'NA'},
        {'Type': 'TERM_MATCH', 'Field': 'licenseModel', 'Value': 'No License required'}
    ]

    response = pricing_client.get_products(ServiceCode='AmazonEC2', Filters=filters)
    response_list = response['PriceList']

    while "NextToken" in response:
        response = pricing_client.get_products(ServiceCode='AmazonEC2', Filters=filters, NextToken=response["NextToken"])
        response_list.extend(response['PriceList'])

    return response_list


# get all ondemand price with regions
def get_ondemand_price(regions):
    session = boto3.session.Session()
    pricing_client = session.client(
        'pricing',
        region_name='us-east-1',
        config=Config(retries={'max_attempts': 2, 'mode': 'standard'})
    )

    ondemand_dict = {"Region": [], "InstanceType": [], "OndemandPrice": []}
    failed_regions = []

    def process_region(region):
        print(f"Collecting on-demand price for region: {region}")
        local_data = {"Region": [], "InstanceType": [], "OndemandPrice": []}
        try:
            # Slight delay to spread request bursts across workers.
            time.sleep(0.2)
            price_infos, attempts_used = retry_with_backoff(
                lambda: get_ondemand_price_region(region, pricing_client),
                label=f"On-Demand Price region={region}",
                return_attempts=True
            )
            for price_info in price_infos:
                parsed = json.loads(price_info)
                instance_type = parsed['product']['attributes']['instanceType']
                instance_price = float(
                    list(list(parsed['terms']['OnDemand'].values())[0]['priceDimensions'].values())[0]['pricePerUnit']['USD']
                )

                # case of instance-region is not available
                if instance_price == 0.0:
                    continue

                local_data['Region'].append(region)
                local_data['InstanceType'].append(instance_type)
                local_data['OndemandPrice'].append(instance_price)
            return region, local_data, None, attempts_used
        except Exception as e:
            print(f"Error collecting on-demand price for region {region}: {e}")
            return region, None, str(e), MAX_RETRY_ATTEMPTS

    # Reduced from 10 to 3 to avoid AWS Pricing API rate limits
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        future_to_region = {executor.submit(process_region, region): region for region in regions}
        for future in concurrent.futures.as_completed(future_to_region):
            region, result, err, attempts_used = future.result()
            if result is not None:
                ondemand_dict['Region'].extend(result['Region'])
                ondemand_dict['InstanceType'].extend(result['InstanceType'])
                ondemand_dict['OndemandPrice'].extend(result['OndemandPrice'])
                print(f"[INFO] On-Demand region success: {region}, attempts={attempts_used}")
            else:
                print(f"[WARN] On-Demand region failed: {region}, attempts={attempts_used}, error={err}")
                failed_regions.append(region)

    ondemand_price_df = pd.DataFrame(ondemand_dict)
    if not ondemand_price_df.empty:
        ondemand_price_df = ondemand_price_df.drop_duplicates(subset=["Region", "InstanceType"], keep="first").reset_index(drop=True)

    return ondemand_price_df, failed_regions


def get_spot_price_all(regions):
    spot_price_df_list = []
    failed_regions = []

    def process_spot_price_region(region):
        print(f"Collecting price for region: {region}")
        try:
            region_df, attempts_used = retry_with_backoff(
                lambda: get_spot_price(region),
                label=f"Spot Price region={region}",
                return_attempts=True
            )
            if region_df is None:
                return region, None, "Spot price function returned None", attempts_used
            if not isinstance(region_df, pd.DataFrame):
                return region, None, "Spot price function did not return DataFrame", attempts_used
            region_df = region_df.copy()
            region_df["Region"] = region
            return region, region_df, None, attempts_used
        except Exception as e:
            print(f"Error collecting price for region {region}: {e}")
            return region, None, str(e), MAX_RETRY_ATTEMPTS

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_region = {executor.submit(process_spot_price_region, region): region for region in regions}
        for future in concurrent.futures.as_completed(future_to_region):
            region, result, err, attempts_used = future.result()
            if result is not None:
                spot_price_df_list.append(result)
                print(f"[INFO] Spot Price region success: {region}, attempts={attempts_used}")
            else:
                print(f"[WARN] Spot Price region failed: {region}, attempts={attempts_used}, error={err}")
                failed_regions.append(region)

    if not spot_price_df_list:
        return pd.DataFrame(columns=["InstanceType", "AZ", "SpotPrice", "Region"]), failed_regions

    spot_price_df = pd.concat(spot_price_df_list).reset_index(drop=True)
    spot_price_df = spot_price_df.drop_duplicates(subset=["Region", "InstanceType", "AZ"], keep="first").reset_index(drop=True)
    return spot_price_df, failed_regions


def main():
    # ------ Set Constants ------
    S3_PATH_PREFIX = "rawdata/aws"
    BUCKET_NAME = "spotlake"

    # ------ Set time data ------
    parser = argparse.ArgumentParser()
    parser.add_argument('--timestamp', dest='timestamp', action='store', help='Timestamp in format YYYY-MM-DDTHH:MM')
    args = parser.parse_args()

    if args.timestamp:
        # Handle EventBridge timestamp format (YYYY-MM-DDTHH:MM:SSZ)
        if args.timestamp.endswith('Z'):
            timestamp_utc = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M:%SZ")
        else:
            try:
                timestamp_utc = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                timestamp_utc = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M")
    else:
        start_time = datetime.now(timezone.utc)
        timestamp_utc = start_time.replace(minute=((start_time.minute // 10) * 10), second=0)

    S3_DIR_NAME = timestamp_utc.strftime('%Y/%m/%d')
    S3_OBJECT_PREFIX = timestamp_utc.strftime('%H-%M')
    SPOTPRICE_FILE_NAME = f"{S3_PATH_PREFIX}/spot_price/{S3_DIR_NAME}/{S3_OBJECT_PREFIX}_spot_price.pkl.gz"
    ONDEMAND_PRICE_FILE_NAME = f"{S3_PATH_PREFIX}/ondemand_price/{S3_DIR_NAME}/ondemand_price.pkl.gz"
    SPOTPRICE_LATEST_SUCCESS_KEY = f"{S3_PATH_PREFIX}/spot_price/latest_success.pkl.gz"
    ONDEMAND_LATEST_SUCCESS_KEY = f"{S3_PATH_PREFIX}/ondemand_price/latest_success.pkl.gz"

    print(f"Collecting Spot Price and On-Demand Price for timestamp: {timestamp_utc}")
    session = boto3.session.Session()
    s3 = session.client('s3')
    regions = get_regions(session)
    print(f"Expected regions: {len(regions)}")

    # ------ Collect Spot Price ------
    start_time = datetime.now(timezone.utc)
    try:
        spot_price_df, spot_failed_regions = get_spot_price_all(regions)
        print(f"Collected {len(spot_price_df)} rows of Spot Price data before fallback")
    except Exception as e:
        send_slack_message(f"Error during spot price collection\n{e}")
        raise e

    end_time = datetime.now(timezone.utc)
    print(f"Collecting Spot Price time is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")

    # ------ Collect On-Demand Price ------
    start_time = datetime.now(timezone.utc)
    try:
        print("Collecting On-Demand Price...")
        ondemand_price_df, ondemand_failed_regions = get_ondemand_price(regions)
        print(f"Collected {len(ondemand_price_df)} rows of On-Demand Price data before fallback")
    except Exception as e:
        send_slack_message(f"Error during on-demand price collection\n{e}")
        raise e

    end_time = datetime.now(timezone.utc)
    print(f"Collecting On-Demand Price time is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")

    # ------ Apply fallback from latest_success snapshots ------
    spot_snapshot_df = load_snapshot_from_s3(s3, BUCKET_NAME, SPOTPRICE_LATEST_SUCCESS_KEY)
    spot_price_df, spot_fallback_regions, spot_unresolved_regions = apply_region_fallback(
        current_df=spot_price_df,
        snapshot_df=spot_snapshot_df,
        failed_regions=spot_failed_regions,
        dataset_name="spot_price",
        key_columns=["Region", "InstanceType", "AZ"]
    )
    print(
        f"[spot_price] failed={len(spot_failed_regions)} "
        f"fallback_applied={len(spot_fallback_regions)} unresolved={len(spot_unresolved_regions)} "
        f"rows_final={len(spot_price_df)}"
    )

    ondemand_snapshot_df = load_snapshot_from_s3(s3, BUCKET_NAME, ONDEMAND_LATEST_SUCCESS_KEY)
    ondemand_price_df, ondemand_fallback_regions, ondemand_unresolved_regions = apply_region_fallback(
        current_df=ondemand_price_df,
        snapshot_df=ondemand_snapshot_df,
        failed_regions=ondemand_failed_regions,
        dataset_name="ondemand_price",
        key_columns=["Region", "InstanceType"]
    )
    print(
        f"[ondemand_price] failed={len(ondemand_failed_regions)} "
        f"fallback_applied={len(ondemand_fallback_regions)} unresolved={len(ondemand_unresolved_regions)} "
        f"rows_final={len(ondemand_price_df)}"
    )

    # ------ Save Raw Data in S3 ------
    start_time = datetime.now(timezone.utc)
    try:
        # Save Spot Price daily file (legacy schema for merge compatibility)
        if spot_price_df.empty:
            spot_daily_df = pd.DataFrame(columns=["InstanceType", "AZ", "SpotPrice"])
        else:
            spot_daily_df = spot_price_df[["InstanceType", "AZ", "SpotPrice"]].copy()

        dump_snapshot_to_s3(s3, BUCKET_NAME, SPOTPRICE_FILE_NAME, spot_daily_df)
        print(f"Uploaded Spot Price data to s3://{BUCKET_NAME}/{SPOTPRICE_FILE_NAME}")

        # Save On-Demand Price daily file
        if ondemand_price_df.empty:
            ondemand_daily_df = pd.DataFrame(columns=["Region", "InstanceType", "OndemandPrice"])
        else:
            ondemand_daily_df = ondemand_price_df[["Region", "InstanceType", "OndemandPrice"]].copy()

        dump_snapshot_to_s3(s3, BUCKET_NAME, ONDEMAND_PRICE_FILE_NAME, ondemand_daily_df)
        print(f"Uploaded On-Demand Price data to s3://{BUCKET_NAME}/{ONDEMAND_PRICE_FILE_NAME}")

        # Update latest_success snapshots only when unresolved fallback is zero.
        if len(spot_unresolved_regions) == 0 and not spot_price_df.empty:
            dump_snapshot_to_s3(s3, BUCKET_NAME, SPOTPRICE_LATEST_SUCCESS_KEY, spot_price_df)
            print(f"Updated latest spot price snapshot: s3://{BUCKET_NAME}/{SPOTPRICE_LATEST_SUCCESS_KEY}")
        else:
            reason = []
            if spot_unresolved_regions:
                reason.append(f"unresolved={spot_unresolved_regions}")
            if spot_price_df.empty:
                reason.append("empty dataframe")
            print(f"[WARN] Skipped spot latest_success update: {', '.join(reason)}")

        if len(ondemand_unresolved_regions) == 0 and not ondemand_price_df.empty:
            dump_snapshot_to_s3(s3, BUCKET_NAME, ONDEMAND_LATEST_SUCCESS_KEY, ondemand_price_df)
            print(f"Updated latest on-demand snapshot: s3://{BUCKET_NAME}/{ONDEMAND_LATEST_SUCCESS_KEY}")
        else:
            reason = []
            if ondemand_unresolved_regions:
                reason.append(f"unresolved={ondemand_unresolved_regions}")
            if ondemand_price_df.empty:
                reason.append("empty dataframe")
            print(f"[WARN] Skipped ondemand latest_success update: {', '.join(reason)}")

    except Exception as e:
        send_slack_message(f"Store spot price data to be stored in the cloud in memory\n{e}")
        raise e

    end_time = datetime.now(timezone.utc)
    print(f"Upload time is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")


if __name__ == "__main__":
    start_time = datetime.now(timezone.utc)
    main()
    end_time = datetime.now(timezone.utc)
    print(f"Running time is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")
