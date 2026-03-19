import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
import polars as pl

from collector_core import (
    build_snapshot_df,
    fetch_billing_catalog,
    get_access_token,
    list_regions_and_machine_types_rest,
    list_regions_and_machine_types_sdk,
    load_service_account_info,
)
from compare_data import compare
from const_config import GcpCollector
from runtime_config import load_runtime_config
from s3_management import (
    load_latest_state,
    save_raw,
    update_latest,
    update_query_selector,
    upload_timestream,
)
from utility.slack_msg_sender import send_slack_message

# ------ TITANS setup ------
# Support both repo layout and flat Lambda zip layout.
for candidate in [Path(__file__).resolve().parent, *Path(__file__).resolve().parents]:
    if (candidate / "titans_common").is_dir():
        candidate_str = str(candidate)
        if candidate_str not in sys.path:
            sys.path.insert(0, candidate_str)
        break

try:
    from titans_common.upload_titans import upload_hot_tier
    from titans_common.warm_compactor import ConcurrencyConflictError, run_compaction

    TITANS_AVAILABLE = True
    TITANS_IMPORT_ERROR = ""
except ImportError as exc:
    TITANS_AVAILABLE = False
    TITANS_IMPORT_ERROR = str(exc)

PROVIDER = "gcp"
TITANS_ENABLED = os.environ.get("TITANS_ENABLED", "0") == "1"
GCP_CONST = GcpCollector()


def upload_cloudwatch(df_current: pl.DataFrame, timestamp: datetime) -> None:
    ondemand_count = (
        df_current.select(["Time", "InstanceType", "Region", "OnDemand Price"]).drop_nulls().height
    )
    spot_count = (
        df_current.select(["Time", "InstanceType", "Region", "Spot Price"]).drop_nulls().height
    )

    cw_client = boto3.client("logs")
    log_event = {
        "timestamp": int(timestamp.timestamp()) * 1000,
        "message": f"GCPONDEMAND: {ondemand_count} GCPSPOT: {spot_count}",
    }
    cw_client.put_log_events(
        logGroupName=GCP_CONST.SPOT_DATA_COLLECTION_LOG_GROUP_NAME,
        logStreamName=GCP_CONST.LOG_STREAM_NAME,
        logEvents=[log_event],
    )


def prepare_titans_upload(changed_df: pl.DataFrame, removed_df: pl.DataFrame) -> pl.DataFrame:
    if changed_df.is_empty() and removed_df.is_empty():
        return pl.DataFrame()

    rename_map = {
        "OnDemand Price": "OndemandPrice",
        "Spot Price": "SpotPrice",
    }

    frames: list[pl.DataFrame] = []
    if not changed_df.is_empty():
        changed = changed_df.rename({k: v for k, v in rename_map.items() if k in changed_df.columns})
        if "Ceased" not in changed.columns:
            changed = changed.with_columns(pl.lit(False).alias("Ceased"))
        frames.append(changed)

    if not removed_df.is_empty():
        removed = removed_df.rename({k: v for k, v in rename_map.items() if k in removed_df.columns})
        if "Ceased" not in removed.columns:
            removed = removed.with_columns(pl.lit(True).alias("Ceased"))
        frames.append(removed)

    combined = pl.concat(frames, how="diagonal_relaxed")
    if "Ceased" in combined.columns:
        combined = (
            combined
            .sort(["InstanceType", "Region", "Time", "Ceased"])
            .unique(subset=["InstanceType", "Region", "Time"], keep="first")
            .sort(["InstanceType", "Region", "Time"])
        )
    return combined


def lambda_handler(event, context):
    try:
        start_time = time.time()
        runtime_config = load_runtime_config()
        print(
            "[GCP Collector] Runtime config "
            f"read_bucket={runtime_config.read_bucket_name} "
            f"write_bucket={runtime_config.write_bucket_name} "
            f"latest_read={runtime_config.latest_read_path} "
            f"latest_write={runtime_config.latest_write_path} "
            f"raw_prefix={runtime_config.raw_prefix} "
            f"timestream_enabled={runtime_config.timestream_enabled} "
            f"query_selector_enabled={runtime_config.query_selector_enabled} "
            f"compute_api_backend={runtime_config.compute_api_backend}"
        )
        print(
            "[GCP Collector] TITANS "
            f"enabled={TITANS_ENABLED} "
            f"available={TITANS_AVAILABLE} "
            f"import_error={TITANS_IMPORT_ERROR or 'none'}"
        )

        timestamp = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        access_token = get_access_token()
        project_id = load_service_account_info()["project_id"]

        sku_infos, gpu_sku_infos, price_infos, gpu_price_infos = fetch_billing_catalog(access_token)
        print("Complete to get sku_infos")
        print("Complete to get price_infos")

        gpu_families = sorted(
            {
                gpu_info["gpuType"]
                for gpu_info in gpu_sku_infos
                if gpu_info.get("gpuType")
            }
        )
        if runtime_config.compute_api_backend == "rest":
            machine_types_infos = list_regions_and_machine_types_rest(
                gpu_families,
                access_token=access_token,
                project_id=project_id,
            )
        else:
            machine_types_infos = list_regions_and_machine_types_sdk(
                gpu_families,
                service_account_file=os.environ["GOOGLE_APPLICATION_CREDENTIALS"],
            )
        print("Complete to get machine_types_infos")

        df_final = build_snapshot_df(
            sku_infos,
            gpu_sku_infos,
            price_infos,
            gpu_price_infos,
            machine_types_infos,
            timestamp=timestamp,
        )

        upload_cloudwatch(df_final, timestamp)

        # Load previous data BEFORE updating latest so compare uses the selected
        # read baseline (production for shadow test, write bucket for production).
        df_previous = load_latest_state()

        update_latest(df_final, timestamp)
        save_raw(df_final, timestamp)

        workload_cols = ["InstanceType", "Region"]
        feature_cols = ["OnDemand Price", "Spot Price"]
        changed_df, removed_df = compare(df_previous, df_final, workload_cols, feature_cols)
        print(
            "[GCP Collector] compare result "
            f"changed_rows={changed_df.height} "
            f"removed_rows={removed_df.height}"
        )
        ts_utc = timestamp.astimezone(timezone.utc)

        if not removed_df.is_empty() and "Time" in removed_df.columns:
            removed_df = removed_df.with_columns(
                pl.lit(ts_utc.strftime("%Y-%m-%d %H:%M:%S")).alias("Time")
            )

        if runtime_config.query_selector_enabled:
            update_query_selector(changed_df)

        if runtime_config.timestream_enabled:
            upload_timestream(changed_df, timestamp)
            upload_timestream(removed_df, timestamp)

        if TITANS_ENABLED and TITANS_AVAILABLE:
            try:
                combined_df = prepare_titans_upload(changed_df, removed_df)
                print(f"[TITANS/{PROVIDER}] prepared_rows={combined_df.height}")
                if not combined_df.is_empty():
                    titans_s3 = boto3.client("s3")
                    hot_key = upload_hot_tier(combined_df, ts_utc, provider=PROVIDER, s3_client=titans_s3)
                    if hot_key:
                        run_compaction(hot_key, ts_utc, provider=PROVIDER, timeout_seconds=30.0, s3_client=titans_s3)
                    print(f"[TITANS/{PROVIDER}] Successfully uploaded")
            except ConcurrencyConflictError as e:
                print(f"[TITANS/{PROVIDER}] Concurrency conflict, will retry next cycle: {e}")
            except Exception as e:
                print(f"[TITANS/{PROVIDER}] Failed (non-fatal): {e}")

        end_time = time.time()
        print(f"Total time taken: {end_time - start_time} seconds")
    except Exception as e:
        send_slack_message(f"Unhandled exception in main: {str(e)}")
        raise


if __name__ == "__main__":
    lambda_handler({}, {})
