# ------ import module ------
from datetime import datetime, timezone, timedelta
import boto3
import pickle
import gzip
import json
import os
import resource
import sys
import pandas as pd
import argparse
import time
from pathlib import Path

# ------ TITANS setup ------
# Add titans_common path (merge -> batch -> aws -> spot-dataset -> collector)
COLLECTOR_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(COLLECTOR_ROOT))

from titans_common.upload_titans import upload_hot_tier
from titans_common.warm_compactor import run_compaction, ConcurrencyConflictError
from titans_common.utils import prepare_for_upload

PROVIDER = "aws"
TITANS_ENABLED = os.environ.get("TITANS_ENABLED", "0") == "1" # Default OFF until titans cold data cleaning resolved

# ------ import user module ------
from utility.slack_msg_sender import send_slack_message
from upload_data import upload_timestream, update_latest, save_raw, update_query_selector
from compare_data import compare, compare_max_instance

class FirstRunError(Exception):
    pass


def _rss_mb() -> float:
    """Return current process RSS in MiB when available."""
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024.0
    except OSError:
        pass

    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def _stage_log(stage: str, *, extra: str = "") -> None:
    suffix = f" {extra}" if extra else ""
    print(f"[TITANS/{PROVIDER}] {stage} rss_mb={_rss_mb():.1f}{suffix}", flush=True)


def _merge_log(stage: str, *, extra: str = "") -> None:
    suffix = f" {extra}" if extra else ""
    print(f"[MERGE/{PROVIDER}] {stage} rss_mb={_rss_mb():.1f}{suffix}", flush=True)

def main():
    _merge_log("start")
    start_time = datetime.now(timezone.utc)

    # ------ Parse Arguments ------
    parser = argparse.ArgumentParser()
    parser.add_argument('--sps_key', dest='sps_key', action='store', help='S3 Key of the SPS file')
    parser.add_argument('--bucket', dest='bucket', action='store', help='S3 Bucket Name')
    parser.add_argument('--timestamp', dest='timestamp', action='store', help='Timestamp in format YYYY-MM-DDTHH:MM (optional override)')
    args = parser.parse_args()

    # ------ Set Constants ------
    BUCKET_NAME = "spotlake"
    S3_PATH_PREFIX = "rawdata/aws"
    # BUCKET_FILE_PATH is removed in favor of specific paths from const_config
    
    if args.sps_key:
        sps_file_name = args.sps_key
        # Extract info from key
        # Expected format: .../2023/11/23/02-10_sps_50.pkl.gz
        try:
            filename = sps_file_name.split('/')[-1]
            parts = filename.split('_')
            # parts[0] is "02-10" (time)
            # parts[1] is "sps"
            # parts[2] is "50.pkl.gz"
            time_part = parts[0]
            target_capacity = int(parts[2].split('.')[0])
            
            # Extract date from path
            # .../2023/11/23/...
            path_parts = sps_file_name.split('/')
            date_str = f"{path_parts[-4]}/{path_parts[-3]}/{path_parts[-2]}" # YYYY/MM/DD
            
            TIMESTAMP = datetime.strptime(f"{date_str} {time_part}", "%Y/%m/%d %H-%M").replace(tzinfo=timezone.utc)
            S3_DIR_NAME = date_str
            S3_OBJECT_PREFIX = time_part
            
        except Exception as e:
            print(f"Error parsing SPS key: {sps_file_name}. Error: {e}")
            # Fallback or exit?
            # If we can't parse, we might fail to find other files.
            raise e
    elif args.timestamp:
        # If timestamp provided but no key, try to find the file? 
        # Or assume this mode is for manual run?
        # Handle EventBridge timestamp format (YYYY-MM-DDTHH:MM:SSZ)
        if args.timestamp.endswith('Z'):
            TIMESTAMP = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        else:
            try:
                TIMESTAMP = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
            except ValueError:
                TIMESTAMP = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M").replace(tzinfo=timezone.utc)
        S3_DIR_NAME = TIMESTAMP.strftime('%Y/%m/%d')
        S3_OBJECT_PREFIX = TIMESTAMP.strftime('%H-%M')
        # We need a target capacity. Default to 50? Or loop?
        # The original lambda logic found the file.
        # Let's assume if timestamp is given, we might need to find the file or just fail if sps_key is missing.
        print("Timestamp provided without SPS Key. This mode might be ambiguous regarding Target Capacity.")
        return
    else:
        # Default behavior (like Lambda triggered by schedule? No, Lambda was triggered by something or just ran)
        # The original lambda: TIMESTAMP = start_time ... - 10 mins
        TIMESTAMP = start_time.replace(minute=((start_time.minute // 10) * 10), second=0) - timedelta(minutes=10)
        S3_DIR_NAME = TIMESTAMP.strftime('%Y/%m/%d')
        S3_OBJECT_PREFIX = TIMESTAMP.strftime('%H-%M')
        # It then listed objects to find the file.
        s3_client = boto3.client('s3')
        SPS_FILE_PREFIX = f"{S3_PATH_PREFIX}/sps/{S3_DIR_NAME}"
        sps_file_list = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=SPS_FILE_PREFIX)
        sps_files = []
        if 'Contents' in sps_file_list:
            for obj in sps_file_list['Contents']:
                if obj['Key'].startswith(f"{SPS_FILE_PREFIX}/{S3_OBJECT_PREFIX}"):
                    sps_files.append(obj['Key'])
        
        if not sps_files:
            print(f"No SPS files found for {S3_OBJECT_PREFIX}")
            return
            
        sps_file_name = sps_files[0] # Just take the first one? Original code did this.
        target_capacity = int(sps_file_name.split('/')[-1].split('_')[2].split('.')[0])

    _merge_log(
        "input_resolved",
        extra=f"sps_key={sps_file_name} timestamp={TIMESTAMP.isoformat()} target_capacity={target_capacity}",
    )

    SPOTIF_FILE_NAME = f"{S3_PATH_PREFIX}/spot_if/{S3_DIR_NAME}/{S3_OBJECT_PREFIX}_spot_if.pkl.gz"
    ONDEMAND_PRICE_FILE_NAME = f"{S3_PATH_PREFIX}/ondemand_price/{S3_DIR_NAME}/ondemand_price.pkl.gz"
    SPOTPRICE_FILE_NAME = f"{S3_PATH_PREFIX}/spot_price/{S3_DIR_NAME}/{S3_OBJECT_PREFIX}_spot_price.pkl.gz"

    # ------ Set time data ------
    time_value = TIMESTAMP.strftime("%Y-%m-%d %H:%M:%S")

    try:
        # ------ Create Boto3 Session ------
        s3 = boto3.resource("s3")
        s3_client = boto3.client('s3')

        # ------ Load Data from PKL File in S3 ------
        _merge_log("load_inputs start")
        try:
            sps_df = pickle.load(gzip.open(s3.Object(BUCKET_NAME, sps_file_name).get()["Body"]))
        except Exception as e:
             print(f"Failed to load SPS file: {e}")
             raise e
             
        try:
            spotinfo_df = pickle.load(gzip.open(s3.Object(BUCKET_NAME, SPOTIF_FILE_NAME.strip()).get()["Body"]))
        except Exception as e:
            print(f"Failed to load Spot IF file ({SPOTIF_FILE_NAME}): {e}")
            # Should we fail or continue with empty? Original code would fail.
            raise e

        try:
            ondemand_price_df = pickle.load(gzip.open(s3.Object(BUCKET_NAME, ONDEMAND_PRICE_FILE_NAME.strip()).get()["Body"]))
        except Exception as e:
             print(f"Failed to load OnDemand Price file ({ONDEMAND_PRICE_FILE_NAME}): {e}")
             # Maybe ondemand price is not collected every 10 mins? 
             # Original code assumes it exists.
             raise e

        try:
            spot_price_df = pickle.load(gzip.open(s3.Object(BUCKET_NAME, SPOTPRICE_FILE_NAME.strip()).get()["Body"]))
        except Exception as e:
            print(f"Failed to load Spot Price file ({SPOTPRICE_FILE_NAME}): {e}")
            raise e

        # ------ Create a DF by Selecting Only The Columns Required ------
        sps_df = sps_df[['InstanceType', 'Region', 'AZ', 'SPS', 'T3', 'T2']]
        spotinfo_df = spotinfo_df[['InstanceType', 'Region', 'IF']]
        ondemand_price_df = ondemand_price_df[['InstanceType', 'Region', 'OndemandPrice']]
        spot_price_df = spot_price_df[['InstanceType', 'AZ', 'SpotPrice']]

        # ------ Formatting Data ------
        spot_price_df['SpotPrice'] = spot_price_df['SpotPrice'].astype('float').round(5)
        ondemand_price_df['OndemandPrice'] = ondemand_price_df['OndemandPrice'].astype('float').round(5)

        # ------ Need to Change to Outer Join ------
        _merge_log("merge_frames start")
        merge_df = pd.merge(sps_df, spotinfo_df, how="outer")
        merge_df = pd.merge(merge_df, ondemand_price_df, how="outer")
        merge_df = pd.merge(merge_df, spot_price_df, how="outer")

        merge_df['Savings'] = 100.0 - (merge_df['SpotPrice'] * 100 / merge_df['OndemandPrice'])
        merge_df['Savings'] = merge_df['Savings'].fillna(-1)
        merge_df['SPS'] = merge_df['SPS'].fillna(-1)
        merge_df['SpotPrice'] = merge_df['SpotPrice'].fillna(-1)
        merge_df['OndemandPrice'] = merge_df['OndemandPrice'].fillna(-1)
        merge_df['IF'] = merge_df['IF'].fillna(-1)

        merge_df['Savings'] = merge_df['Savings'].astype('int')
        merge_df['SPS'] = merge_df['SPS'].astype('int')
        merge_df['T3'] = merge_df['T3'].fillna(0).astype('int')
        merge_df['T2'] = merge_df['T2'].fillna(0).astype('int')

        merge_df = merge_df.drop(merge_df[(merge_df['AZ'].isna()) | (merge_df['Region'].isna()) | (merge_df['InstanceType'].isna())].index)

        merge_df.reset_index(drop=True, inplace=True)
        merge_df['Time'] = time_value

        end_time = datetime.now(timezone.utc)
        _merge_log(
            "merge_frames end",
            extra=(
                f"elapsed_min={(end_time - start_time).total_seconds() * 1000 / 60000:.2f} "
                f"rows={len(merge_df)}"
            ),
        )

        # ------ Check The Previous DF File in S3 and Local ------
        previous_df = None
        start_time = datetime.now(timezone.utc)
        filename = 'latest_aws.json'
        LATEST_PATH = f'latest_data/{filename}'
        try:
            previous_df = pd.DataFrame(json.load(s3.Object(BUCKET_NAME, LATEST_PATH).get()['Body']))
            # Verify that the data is in the old format
            columns_to_check = ["T3", "T2"]
            existing_columns = [col for col in columns_to_check if col in previous_df.columns]

            if len(existing_columns) == 0:
                raise FirstRunError("Can't load the previous df from s3 bucket or First run since changing the collector")
            else:
                previous_df = previous_df.drop(columns=['id'])
        except Exception as e: # Catching generic exception to handle NoSuchKey or FirstRunError
            _merge_log(f"previous_df missing_or_invalid error={e}")
            # If system is first time uploading data, make a new one and upload it to TSDB
            _merge_log("update_latest start", extra="first_run=1")
            update_latest(merge_df, TIMESTAMP)
            _merge_log("update_latest end", extra="first_run=1")
            _merge_log("save_raw start", extra="first_run=1")
            save_raw(merge_df, TIMESTAMP)
            _merge_log("save_raw end", extra="first_run=1")
            _merge_log("upload_timestream start", extra=f"first_run=1 rows={len(merge_df)}")
            upload_timestream(merge_df, TIMESTAMP)
            _merge_log("upload_timestream end", extra="first_run=1")
            end_time = datetime.now(timezone.utc)
            _merge_log(
                "first_run complete",
                extra=f"elapsed_min={(end_time - start_time).total_seconds() * 1000 / 60000:.2f}",
            )
            return

        end_time = datetime.now(timezone.utc)
        _merge_log(
            "previous_df loaded",
            extra=f"elapsed_min={(end_time - start_time).total_seconds() * 1000 / 60000:.2f} rows={len(previous_df)}",
        )

        start_time = datetime.now(timezone.utc)

        # ------ Compare T3 and T2 Data ------
        _merge_log("compare_max_instance start", extra=f"prev_rows={len(previous_df)} current_rows={len(merge_df)}")
        current_df = compare_max_instance(previous_df, merge_df, target_capacity)
        _merge_log("compare_max_instance end", extra=f"rows={len(current_df)}")

        # ------ Upload Merge DF to s3 Bucket ------
        _merge_log("update_latest start", extra=f"rows={len(current_df)}")
        update_latest(current_df, TIMESTAMP)
        _merge_log("update_latest end", extra=f"rows={len(current_df)}")
        _merge_log("save_raw start", extra=f"rows={len(current_df)}")
        save_raw(current_df, TIMESTAMP)
        _merge_log("save_raw end", extra=f"rows={len(current_df)}")

        # ------ Compare All Data ------
        workload_cols = ['InstanceType', 'Region', 'AZ']
        feature_cols = ['SPS', 'T3', 'T2', 'IF', 'SpotPrice', 'OndemandPrice']

        _merge_log("compare start", extra=f"prev_rows={len(previous_df)} current_rows={len(current_df)}")
        changed_df, removed_df = compare(previous_df, current_df, workload_cols, feature_cols)  # compare previous_df and current_df to extract changed rows)
        _merge_log("compare end", extra=f"changed_rows={len(changed_df)} removed_rows={len(removed_df)}")
        ts_utc = TIMESTAMP if TIMESTAMP.tzinfo else TIMESTAMP.replace(tzinfo=timezone.utc)

        # Ceased timestamp semantics: disappearance is observed at current batch time.
        # removed_df rows come from previous_df, so their Time must be overwritten.
        if not removed_df.empty and "Time" in removed_df.columns:
            removed_df = removed_df.copy()
            removed_df["Time"] = ts_utc.strftime("%Y-%m-%d %H:%M:%S")

        end_time = datetime.now(timezone.utc)
        _merge_log(
            "compare_total end",
            extra=f"elapsed_min={(end_time - start_time).total_seconds() * 1000 / 60000:.2f}",
        )

        # ------ Upload TSDB ------
        start_time = datetime.now(timezone.utc)
        _merge_log(
            "upload_timestream start",
            extra=f"changed_rows={len(changed_df)} removed_rows={len(removed_df)}",
        )
        upload_timestream(changed_df, TIMESTAMP)
        upload_timestream(removed_df, TIMESTAMP)
        end_time = datetime.now(timezone.utc)
        _merge_log(
            "upload_timestream end",
            extra=f"elapsed_min={(end_time - start_time).total_seconds() * 1000 / 60000:.2f}",
        )

        # ------ TITANS Hot tier upload + Warm compaction ------
        _merge_log(
            "titans_gate",
            extra=f"enabled={int(TITANS_ENABLED)} changed_rows={len(changed_df)} removed_rows={len(removed_df)}",
        )
        if TITANS_ENABLED:
            try:
                _stage_log("start", extra=f"changed_rows={len(changed_df)} removed_rows={len(removed_df)}")
                prep_started = time.time()
                _stage_log("prepare_for_upload start")
                combined_df = prepare_for_upload(changed_df, removed_df, pk_columns=workload_cols)
                _stage_log(
                    "prepare_for_upload end",
                    extra=f"elapsed_s={time.time() - prep_started:.2f} combined_rows={len(combined_df)}",
                )

                if not combined_df.empty:
                    _stage_log("creating titans s3 client")
                    titans_s3 = boto3.client("s3")
                    hot_started = time.time()
                    _stage_log("upload_hot_tier start")
                    hot_key = upload_hot_tier(combined_df, ts_utc, provider=PROVIDER, s3_client=titans_s3)
                    _stage_log(
                        "upload_hot_tier end",
                        extra=f"elapsed_s={time.time() - hot_started:.2f} hot_key={hot_key}",
                    )
                    if hot_key:
                        compact_started = time.time()
                        _stage_log("run_compaction start", extra=f"hot_key={hot_key}")
                        run_compaction(hot_key, ts_utc, provider=PROVIDER, timeout_seconds=30.0, s3_client=titans_s3)
                        _stage_log(
                            "run_compaction end",
                            extra=f"elapsed_s={time.time() - compact_started:.2f} hot_key={hot_key}",
                        )
                    _stage_log("success")
                    print(f"[TITANS/{PROVIDER}/PROD] Successfully uploaded")
                else:
                    _stage_log("skip empty combined_df")

            except ConcurrencyConflictError as e:
                _stage_log("concurrency_conflict", extra=f"error={e}")
                print(f"[TITANS/{PROVIDER}/PROD] Concurrency conflict, will retry next cycle: {e}")
            except Exception as e:
                _stage_log("failure", extra=f"error={e}")
                print(f"[TITANS/{PROVIDER}/PROD] Failed (non-fatal): {e}")

        # ------ Upload Spotlake Query Selector to S3 ------
        start_time = datetime.now(timezone.utc)
        _merge_log("update_query_selector start", extra=f"rows={len(changed_df)}")
        update_query_selector(changed_df)
        end_time = datetime.now(timezone.utc)
        _merge_log(
            "update_query_selector end",
            extra=f"elapsed_min={(end_time - start_time).total_seconds() * 1000 / 60000:.2f}",
        )
    except Exception as e:
        send_slack_message(e)
        _merge_log(f"failure error={e}")
        raise

if __name__ == "__main__":
    main()
