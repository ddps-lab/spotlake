import sys
import os
import argparse
import pandas as pd
import boto3
import yaml
from datetime import datetime, timezone

# Add parent directory to path to import utils and modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import load_sps
from sps_resilience import CollectionDeadline, DynamoDBLease, SPSCallCoordinator
from sps_notifications import (
    format_deadline_exceeded_message,
    format_failure_message,
    format_request_failure_message,
    format_superseded_message,
)
from utils.common import S3, Logger
from utils.constants import AZURE_CONST
from utils.slack_msg_sender import send_slack_message

SPS_METADATA_S3_KEY = f"{AZURE_CONST.S3_RAW_DATA_PATH}/localfile/sps_metadata.yaml"
DESIRED_COUNTS = [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50]
BUCKET_NAME = "spotlake"
SPS_ACTIVE_CALL_LEASE_ID = "spotlake-azure-sps-collector-lease"
SPS_PRIORITY_LEASE_ID = "spotlake-azure-sps-collector-priority"
SPS_COLLECTION_BUDGET_SECONDS = int(
    os.environ.get("SPS_COLLECTION_BUDGET_SECONDS", "600")
)
SPS_LEASE_SECONDS = 60
SPS_LEASE_HEARTBEAT_SECONDS = 5
SPS_CALL_HANDOFF_TIMEOUT_SECONDS = 70


class SPSLeaseUnavailableError(RuntimeError):
    pass


def _as_utc(value):
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _format_epoch_utc(epoch_seconds):
    return datetime.fromtimestamp(epoch_seconds, timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )

def read_metadata():
    try:
        data = S3.read_file(SPS_METADATA_S3_KEY, 'yaml')
        if data:
            Logger.info(f"Read metadata from S3: {SPS_METADATA_S3_KEY}")
            return data
    except Exception as e:
        Logger.info(f"Failed to read metadata from S3: {e}")
    return None

def write_metadata(metadata):
    try:
        S3.upload_file(metadata, SPS_METADATA_S3_KEY, 'yaml')
        Logger.info(f"Saved metadata to S3: {SPS_METADATA_S3_KEY}")
    except Exception as e:
        Logger.error(f"Failed to save metadata to S3: {e}")


def save_sps_data(sps_df, timestamp_utc, desired_count, *, partial=False):
    if sps_df is None or sps_df.empty:
        raise ValueError("SPS data is empty")

    snapshot = sps_df.copy()
    current_time_str = timestamp_utc.strftime("%Y-%m-%d %H:%M:%S")
    if "time" in snapshot.columns:
        existing_times = snapshot["time"].dropna().unique()
        if len(existing_times) > 0:
            Logger.warning(
                f"SPS DataFrame already has 'time' column with "
                f"{len(existing_times)} unique values"
            )
    snapshot["time"] = current_time_str

    unique_times = snapshot["time"].unique()
    if len(unique_times) > 1:
        raise ValueError(
            f"SPS data contains {len(unique_times)} different timestamps"
        )

    time_str = timestamp_utc.strftime("%H-%M")
    date_path = timestamp_utc.strftime("%Y/%m/%d")
    s3_key = (
        f"{AZURE_CONST.S3_RAW_DATA_PATH}/sps/{date_path}/"
        f"{time_str}_sps_{desired_count}.pkl.gz"
    )
    Logger.info(
        f"Saving {'partial ' if partial else ''}SPS data: "
        f"rows={len(snapshot)}, key={s3_key}"
    )

    local_path = "/tmp/sps_data_partial.pkl.gz" if partial else "/tmp/sps_data.pkl.gz"
    snapshot.to_pickle(local_path, compression="gzip")
    try:
        s3_client = boto3.client("s3")
        with open(local_path, "rb") as file_obj:
            s3_client.upload_fileobj(file_obj, BUCKET_NAME, s3_key)
    finally:
        if os.path.exists(local_path):
            os.remove(local_path)

    with open("/tmp/sps_key.txt", "w") as file_obj:
        file_obj.write(s3_key)
    if partial:
        with open("/tmp/sps_partial.txt", "w") as file_obj:
            file_obj.write("partial")

    print(f"Uploaded {'partial ' if partial else ''}SPS data to S3: {s3_key}")
    return s3_key


def save_partial_sps_from_error(error, timestamp_utc, desired_count):
    partial_sps_df = getattr(error, "partial_sps_df", None)
    if partial_sps_df is None or partial_sps_df.empty:
        return False
    try:
        save_sps_data(
            partial_sps_df,
            timestamp_utc,
            desired_count,
            partial=True,
        )
        return True
    except Exception as save_error:
        Logger.error(f"Failed to save partial SPS data: {save_error}")
        return False

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--timestamp', dest='timestamp', action='store')
    args = parser.parse_args()

    if args.timestamp:
        if args.timestamp.endswith('Z'):
            timestamp_utc = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M:%SZ")
        else:
            timestamp_utc = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M")
    else:
        timestamp_utc = datetime.now(timezone.utc)
        timestamp_utc = timestamp_utc.replace(minute=((timestamp_utc.minute // 10) * 10), second=0, microsecond=0)

    timestamp_utc = _as_utc(timestamp_utc)
    priority_lease = DynamoDBLease(
        lease_id=SPS_PRIORITY_LEASE_ID,
        lease_seconds=SPS_LEASE_SECONDS,
        heartbeat_interval=SPS_LEASE_HEARTBEAT_SECONDS,
        priority=int(timestamp_utc.timestamp()),
    )
    active_call_lease = DynamoDBLease(
        lease_id=SPS_ACTIVE_CALL_LEASE_ID,
        lease_seconds=SPS_LEASE_SECONDS,
        heartbeat_interval=SPS_LEASE_HEARTBEAT_SECONDS,
    )
    call_coordinator = SPSCallCoordinator(
        priority_lease=priority_lease,
        active_call_lease=active_call_lease,
        handoff_timeout_seconds=SPS_CALL_HANDOFF_TIMEOUT_SECONDS,
    )

    def begin_sps_calls():
        if not call_coordinator.begin_calls():
            raise SPSLeaseUnavailableError(
                "newer scheduled SPS run has priority or previous SPS calls "
                "did not drain within the handoff window"
            )
        Logger.info(
            f"[SPS_PRIORITY] Claimed owner={priority_lease.owner_id} "
            f"schedule={timestamp_utc.strftime('%Y-%m-%dT%H:%M:%SZ')}"
        )
        Logger.info(
            f"[SPS_CALLS] Acquired active call slot owner={active_call_lease.owner_id}"
        )

    def finish_sps_calls():
        call_coordinator.finish_calls()
        Logger.info(
            f"[SPS_CALLS] Released active call slot owner={active_call_lease.owner_id}"
        )

    collection_deadline = CollectionDeadline(
        duration_seconds=SPS_COLLECTION_BUDGET_SECONDS,
        before_start=begin_sps_calls,
        on_finish=finish_sps_calls,
        cancelled=call_coordinator.cancellation_reason,
    )
    active_desired_counts = [1]

    print(f"Collection timestamp (UTC): {timestamp_utc}")

    current_date = timestamp_utc.strftime("%Y-%m-%d")

    try:
        load_sps.SS_Resources.collection_deadline = collection_deadline

        metadata = read_metadata()
        sps_df = None
        current_desired_count = 1
        metadata_to_commit = None

        if metadata:
            metadata = dict(metadata)
            desired_count_index = metadata.get("desired_count_index", 0)
            current_desired_count = DESIRED_COUNTS[desired_count_index]
            
            workload_date = metadata.get("workload_date")
            is_first_time_optimization = False
            
            if workload_date != current_date:
                Logger.info(f"Workload date changed: {workload_date} -> {current_date}. Application First Time Optimization.")
                is_first_time_optimization = True
                metadata["workload_date"] = current_date
                
                # Check if we should force desired count to 1 for optimization
                # Legacy code says: "Force Desired Count to 1 for First Time Optimization execution"
                current_execution_desired_count = 1
            else:
                current_execution_desired_count = current_desired_count
            
            next_index = (desired_count_index + 1) % len(DESIRED_COUNTS)
            metadata["desired_count_index"] = next_index
            metadata_to_commit = metadata
            current_desired_count = current_execution_desired_count
            active_desired_counts = [current_execution_desired_count]
            
            if is_first_time_optimization:
                Logger.info(f"Executing First Time Optimization with Count: {current_execution_desired_count}")
                sps_df = load_sps.collect_spot_placement_score_first_time(desired_counts=[current_execution_desired_count])
            else:
                Logger.info(f"Executing Regular Collection. Desired Count: {current_execution_desired_count}")
                sps_df = load_sps.collect_spot_placement_score(desired_counts=[current_execution_desired_count])

        else:
            Logger.info("Metadata missing. Starting fresh.")
            # Default behavior
            initial_metadata = {
                "desired_count_index": 1, 
                "workload_date": current_date
            }
            metadata_to_commit = initial_metadata
            
            Logger.info("Executing First Time Optimization (Fresh Start)")
            sps_df = load_sps.collect_spot_placement_score_first_time(desired_counts=[1])
            current_desired_count = 1
            active_desired_counts = [1]

        if sps_df is None or sps_df.empty:
            raise RuntimeError("Azure SPS collection returned no data")

        save_sps_data(sps_df, timestamp_utc, current_desired_count)

        if call_coordinator.owns_priority():
            write_metadata(metadata_to_commit)
        else:
            Logger.info(
                "[SPS_PRIORITY] Newer scheduled run claimed priority after "
                "SPS calls completed; snapshot saved and metadata update skipped"
            )

    except load_sps.SPSCollectionDeadlineError as e:
        partial_sps_saved = save_partial_sps_from_error(
            e,
            timestamp_utc,
            current_desired_count,
        )
        send_slack_message(
            format_deadline_exceeded_message(
                timestamp=timestamp_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                desired_counts=e.desired_counts,
                query_started_timestamp=_format_epoch_utc(e.started_at),
                deadline_timestamp=_format_epoch_utc(e.deadline_at),
                budget_seconds=SPS_COLLECTION_BUDGET_SECONDS,
                completed_request_count=e.completed_request_count,
                failed_request_count=e.failed_request_count,
                total_request_count=e.total_request_count,
                partial_sps_saved=partial_sps_saved,
            )
        )
        raise
    except load_sps.SPSCollectionSupersededError as e:
        partial_sps_saved = save_partial_sps_from_error(
            e,
            timestamp_utc,
            current_desired_count,
        )
        send_slack_message(
            format_superseded_message(
                timestamp=timestamp_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                desired_counts=e.desired_counts,
                query_started_timestamp=_format_epoch_utc(e.started_at),
                completed_request_count=e.completed_request_count,
                failed_request_count=e.failed_request_count,
                total_request_count=e.total_request_count,
                partial_sps_saved=partial_sps_saved,
            )
        )
        raise
    except load_sps.SPSCollectionRequestError as e:
        partial_sps_saved = save_partial_sps_from_error(
            e,
            timestamp_utc,
            current_desired_count,
        )
        send_slack_message(
            format_request_failure_message(
                timestamp=timestamp_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                desired_counts=e.desired_counts,
                query_started_timestamp=_format_epoch_utc(e.started_at),
                completed_request_count=e.completed_request_count,
                failed_request_count=e.failed_request_count,
                total_request_count=e.total_request_count,
                failure_reasons=e.failure_reasons,
                partial_sps_saved=partial_sps_saved,
            )
        )
        raise
    except Exception as e:
        send_slack_message(
            format_failure_message(
                timestamp=timestamp_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
                desired_counts=active_desired_counts,
                error=e,
            )
        )
        raise
    finally:
        call_coordinator.close()

if __name__ == "__main__":
    main()
