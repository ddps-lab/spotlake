import re
import random
import requests
import time
import os
import hashlib
import json
import pandas as pd
import sys
import uuid
from dataclasses import dataclass
from enum import Enum
from functools import wraps
from datetime import datetime, timezone
from concurrent.futures import CancelledError, ThreadPoolExecutor, as_completed

# Add parent directory to path to import utils and modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from price import collect_price as load_price
from sps_module import sps_location_manager
from sps_module import sps_shared_resources
from sps_module import sps_prepare_parameters
from sps_resilience import TimeoutWindowMetrics
from utils.azure_auth import get_sps_token_and_subscriptions
from utils.common import S3, Logger
from utils.constants import AZURE_CONST

availability_zones = True  # Hardcoded to always collect AvailabilityZone data
DEADLINE_EXCEEDED = "DEADLINE_EXCEEDED"
LEASE_SUPERSEDED = "LEASE_SUPERSEDED"
ACTIVE_LEASE_LOST = "ACTIVE_LEASE_LOST"
RETRY_EXHAUSTED = "RETRY_EXHAUSTED"
CONNECTION_RETRY_EXHAUSTED = "CONNECTION_RETRY_EXHAUSTED"
SERVER_ERROR_RETRY_EXHAUSTED = "SERVER_ERROR_RETRY_EXHAUSTED"
NO_AVAILABLE_LOCATIONS = "NO_AVAILABLE_LOCATIONS"
FILTERED_INVALID_PARAMETERS = "FILTERED_INVALID_PARAMETERS"
UNEXPECTED_ERROR = "UNEXPECTED_ERROR"
INVALID_RESPONSE = "INVALID_RESPONSE"
FANOUT_CANCELLED = "FANOUT_CANCELLED"
CONNECTION_MAX_RETRIES = 8
CONNECTION_BACKOFF_MAX_SECONDS = 10
SERVER_ERROR_MAX_RETRIES = 8


class SPSRequestStatus(str, Enum):
    SUCCESS = "success"
    UNAVAILABLE = "unavailable"
    FATAL = "fatal"


@dataclass(frozen=True)
class SPSRequestOutcome:
    status: SPSRequestStatus
    response: dict | None = None
    reason: str | None = None
    error: str | None = None

    @classmethod
    def success(cls, response, *, reason=None):
        return cls(
            status=SPSRequestStatus.SUCCESS,
            response=response,
            reason=reason,
        )

    @classmethod
    def unavailable(cls, reason):
        return cls(status=SPSRequestStatus.UNAVAILABLE, reason=reason)

    @classmethod
    def fatal(cls, reason, error):
        return cls(
            status=SPSRequestStatus.FATAL,
            reason=reason,
            error=str(error),
        )


class SPSCollectionIncompleteError(RuntimeError):
    def __init__(
        self,
        *,
        message,
        started_at,
        desired_counts,
        deadline_at=None,
        partial_sps_df=None,
        completed_request_count=0,
        failed_request_count=0,
        total_request_count=0,
        failure_reasons=None,
    ):
        self.started_at = started_at
        self.deadline_at = deadline_at
        self.desired_counts = list(desired_counts)
        self.partial_sps_df = (
            pd.DataFrame() if partial_sps_df is None else partial_sps_df.copy()
        )
        self.completed_request_count = completed_request_count
        self.failed_request_count = failed_request_count
        self.total_request_count = total_request_count
        self.failure_reasons = sorted(set(failure_reasons or []))
        super().__init__(message)


class SPSCollectionDeadlineError(SPSCollectionIncompleteError):
    def __init__(
        self,
        *,
        started_at,
        deadline_at,
        desired_counts,
        partial_sps_df=None,
        completed_request_count=0,
        failed_request_count=0,
        total_request_count=0,
        failure_reasons=None,
    ):
        super().__init__(
            message=f"Azure SPS collection deadline exceeded: deadline_at={deadline_at}",
            started_at=started_at,
            deadline_at=deadline_at,
            desired_counts=desired_counts,
            partial_sps_df=partial_sps_df,
            completed_request_count=completed_request_count,
            failed_request_count=failed_request_count,
            total_request_count=total_request_count,
            failure_reasons=failure_reasons,
        )


class SPSCollectionSupersededError(SPSCollectionIncompleteError):
    def __init__(
        self,
        *,
        started_at,
        desired_counts,
        partial_sps_df=None,
        completed_request_count=0,
        failed_request_count=0,
        total_request_count=0,
        failure_reasons=None,
    ):
        super().__init__(
            message="Azure SPS collection superseded by a newer scheduled run",
            started_at=started_at,
            desired_counts=desired_counts,
            partial_sps_df=partial_sps_df,
            completed_request_count=completed_request_count,
            failed_request_count=failed_request_count,
            total_request_count=total_request_count,
            failure_reasons=failure_reasons,
        )


class SPSCollectionRequestError(SPSCollectionIncompleteError):
    def __init__(self, **kwargs):
        reasons = sorted(set(kwargs.get("failure_reasons") or []))
        super().__init__(
            message=(
                "Azure SPS requests did not all complete: "
                f"reasons={','.join(reasons) or 'unknown'}"
            ),
            **kwargs,
        )


def _collection_stop_result(collection_deadline):
    if collection_deadline is None:
        return None
    cancellation_reason = getattr(
        collection_deadline,
        "cancellation_reason",
        None,
    )
    if cancellation_reason:
        reason = cancellation_reason()
        if reason:
            return reason
    superseded = getattr(collection_deadline, "superseded", None)
    if superseded and superseded():
        return LEASE_SUPERSEDED
    if collection_deadline.expired():
        return DEADLINE_EXCEEDED
    return None


def _build_unavailable_sps_rows(request_specs):
    rows = []
    resource_map = getattr(SS_Resources, "region_map_and_instance_map_tmp", {})
    region_map = resource_map.get("region_map", {})
    instance_map = resource_map.get("instance_map", {})

    for request_spec in request_specs:
        for region_code in request_spec["regions"]:
            for instance_type_sps in request_spec["instance_types"]:
                instance = instance_map.get(instance_type_sps, {})
                row = {
                    "DesiredCount": request_spec["desired_count"],
                    "RegionCodeSPS": region_code,
                    "Region": region_map.get(region_code, region_code),
                    "InstanceTypeSPS": instance_type_sps,
                    "InstanceTier": instance.get("InstanceTier"),
                    "InstanceType": instance.get(
                        "InstanceTypeOld", instance_type_sps
                    ),
                    "Score": pd.NA,
                    "T3": pd.NA,
                    "T2": pd.NA,
                }
                if availability_zones is True:
                    row["AvailabilityZone"] = pd.NA
                rows.append(row)

    return rows


def _build_sps_dataframe(rows):
    sps_res_df = pd.DataFrame(rows)
    if sps_res_df.empty:
        return sps_res_df

    subset_cols = ["DesiredCount", "RegionCodeSPS", "InstanceTypeSPS"]
    if availability_zones is True:
        subset_cols.append("AvailabilityZone")
    return sps_res_df.drop_duplicates(subset=subset_cols, keep="last")

SS_Resources = sps_shared_resources
SL_Manager = sps_location_manager
SS_Resources.sps_token, SS_Resources.subscriptions = get_sps_token_and_subscriptions(availability_zones)

def log_execution_time(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if getattr(wrapper, "_is_running", False):
            return func(*args, **kwargs)

        wrapper._is_running = True
        try:
            start_time = datetime.now()
            result = func(*args, **kwargs)
            end_time = datetime.now()

            elapsed_time = end_time - start_time
            minutes, seconds = divmod(elapsed_time.seconds, 60)

            print(f"{func.__name__} executed in {minutes}min {seconds}sec")

            return result
        finally:
            wrapper._is_running = False
    return wrapper

@log_execution_time
def collect_spot_placement_score_first_time(desired_counts):
    Logger.info(f"Executing: collect_spot_placement_score_first_time (desired_counts={desired_counts})")
    if initialize_files_in_s3():
        assert prepare_the_variables()

        start_time = time.time()
        regions_and_instance_types_df, SS_Resources.region_map_and_instance_map_tmp['region_map'], \
        SS_Resources.region_map_and_instance_map_tmp[
            'instance_map'] = collect_regions_and_instance_types_df_by_priceapi()
        az_str = f"availability-zones-{str(availability_zones).lower()}"
        region_map_and_instance_map_json_path = f"{AZURE_CONST.S3_SAVED_VARIABLE_PATH}/{az_str}/{AZURE_CONST.S3_REGION_MAP_AND_INSTANCE_MAP_JSON_FILENAME}"
        S3.upload_file(SS_Resources.region_map_and_instance_map_tmp, region_map_and_instance_map_json_path, "json")

        end_time = time.time()
        elapsed = end_time - start_time
        minutes, seconds = divmod(int(elapsed), 60)
        print(f"collect_regions_and_instance_types_df_by_priceapi. time: {minutes}min {seconds}sec")

        start_time = time.time()
        optimized_initial_df = sps_prepare_parameters.grouping_to_create_optimized_request_list(regions_and_instance_types_df)
        end_time = time.time()
        elapsed = end_time - start_time
        minutes, seconds = divmod(int(elapsed), 60)
        print(f"grouping_to_create_optimized_request_list. time: {minutes}min {seconds}sec")

        start_time = time.time()
        sps_res_availability_zones_true_df = execute_spot_placement_score_task_by_parameter_pool_df(optimized_initial_df, desired_counts)
        print(f'Time_out_retry_count: {SS_Resources.time_out_retry_count}')
        print(f'Connection_error_retry_count: {SS_Resources.connection_error_retry_count}')
        print(f'Server_error_retry_count: {SS_Resources.server_error_retry_count}')
        print(f'Bad_request_retry_count: {SS_Resources.bad_request_retry_count}')
        print(f'Too_many_requests_count: {SS_Resources.too_many_requests_count}')
        print(f'Too_many_requests_count_2: {SS_Resources.too_many_requests_count_2}')
        print(f'Found_invalid_region_retry_count: {SS_Resources.found_invalid_region_retry_count}')
        print(f'Found_invalid_instance_type_retry_count: {SS_Resources.found_invalid_instance_type_retry_count}')

        print(f'\n========================================')
        print(f'df_greedy_clustering_filtered lens: {len(optimized_initial_df)}')
        print(f'Successfully_to_get_sps_count: {SS_Resources.succeed_to_get_sps_count}')
        print(f'Successfully_get_next_available_location_count: {SS_Resources.succeed_to_get_next_available_location_count}')
        print(f'========================================')

        end_time = time.time()
        elapsed = end_time - start_time
        minutes, seconds = divmod(int(elapsed), 60)
        print(f"execute_spot_placement_score_task_by_parameter_pool_df time: {minutes}min {seconds}sec")

        start_time = time.time()
        regions_and_instance_types_filtered_df = sps_prepare_parameters.filter_invalid_parameter(regions_and_instance_types_df)
        df_greedy_clustering_filtered_df = sps_prepare_parameters.greedy_clustering_to_create_optimized_request_list(regions_and_instance_types_filtered_df)

        S3.upload_file(df_greedy_clustering_filtered_df, f"{AZURE_CONST.S3_DF_TO_USE_TODAY_PKL_FILENAME}", "pkl")

        end_time = time.time()
        elapsed = end_time - start_time
        minutes, seconds = divmod(int(elapsed), 60)
        print(f"Prepare the request pool. time: {minutes}min {seconds}sec")

        return sps_res_availability_zones_true_df
    return None


@log_execution_time
def collect_spot_placement_score(desired_counts, instance_types=None):
    if instance_types:
        Logger.info(f"Executing: collect_spot_placement_score. Type: SPECIFIC. desired_counts={desired_counts}, instance_types={instance_types}, availability_zones={availability_zones}")
    else:
        if desired_counts[0] == 1:
            Logger.info(f"Executing: collect_spot_placement_score. Type: DESIRED_COUNT_1. desired_counts={desired_counts}, availability_zones={availability_zones}")
        else:
            Logger.info(f"Executing: collect_spot_placement_score. Type: MULTI. desired_counts={desired_counts}, availability_zones={availability_zones}")

    assert prepare_the_variables()

    if instance_types:
        regions = list(SS_Resources.region_map_and_instance_map_tmp['region_map'].keys())
        invalid_regions = SS_Resources.invalid_regions_tmp

        valid_regions = [region for region in regions if region not in invalid_regions]

        def chunk_list(lst, chunk_size):
            return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

        regions_chunks = chunk_list(valid_regions, 8)
        instance_types_chunks = chunk_list(instance_types, 3)

        data_tmp = []
        for region_chunk in regions_chunks:
            for instance_type_chunk in instance_types_chunks:
                data_tmp.append({'Regions': region_chunk, 'InstanceTypes': instance_type_chunk})

        requests_df = pd.DataFrame(data_tmp)

    else:
        requests_df = S3.read_file(f"{AZURE_CONST.S3_DF_TO_USE_TODAY_PKL_FILENAME}", 'pkl')

    sps_res_availability_zones_df = execute_spot_placement_score_task_by_parameter_pool_df(requests_df, desired_counts)
    print(f'Time_out_retry_count: {SS_Resources.time_out_retry_count}')
    print(f'Connection_error_retry_count: {SS_Resources.connection_error_retry_count}')
    print(f'Server_error_retry_count: {SS_Resources.server_error_retry_count}')
    print(f'Bad_request_retry_count: {SS_Resources.bad_request_retry_count}')
    print(f'Too_many_requests_count: {SS_Resources.too_many_requests_count}')
    print(f'Too_many_requests_count_2: {SS_Resources.too_many_requests_count_2}')
    print(f'Found_invalid_region_retry_count: {SS_Resources.found_invalid_region_retry_count}')
    print(f'Found_invalid_instance_type_retry_count: {SS_Resources.found_invalid_instance_type_retry_count}')


    print(f'\n========================================')
    print(f'lens(df_greedy_clustering_filtered) * lens(desired_counts): {len(requests_df)*len(desired_counts)}')
    print(f'Successfully_to_get_sps_count: {SS_Resources.succeed_to_get_sps_count}')
    print(f'Successfully_get_next_available_location_count: {SS_Resources.succeed_to_get_next_available_location_count}')
    print(f'========================================')
    return sps_res_availability_zones_df


def execute_spot_placement_score_task_by_parameter_pool_df(api_calls_df, desired_counts):
    collection_deadline = getattr(SS_Resources, "collection_deadline", None)
    if collection_deadline:
        deadline_at = collection_deadline.start()
        started_at = collection_deadline.started_at
        print(
            "SPS query start time (UTC): "
            f"{datetime.fromtimestamp(started_at, timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}"
        )
        print(
            "SPS collection deadline (UTC): "
            f"{datetime.fromtimestamp(deadline_at, timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}"
        )
    Logger.info(f"Executing: execute_spot_placement_score_task_by_parameter_pool_df. desired_counts={desired_counts}, availability_zones={availability_zones}")
    results = []
    locations = list(SS_Resources.locations_call_history_tmp[list(SS_Resources.locations_call_history_tmp.keys())[0]].keys())
    total_request_count = len(api_calls_df) * len(desired_counts)
    try:
        with SS_Resources.location_lock:
            SL_Manager.prepare_location_routing(
                total_request_count=total_request_count,
            )
    except Exception as health_error:
        SS_Resources.required_sps_call_slots = 0
        SS_Resources.effective_excluded_locations = set()
        SS_Resources.fallback_locations = set()
        SS_Resources.probe_locations_pending = []
        SS_Resources.probed_locations_this_job = set()
        print(
            "[SPS_LOCATION_HEALTH] action=disabled "
            f"error={str(health_error)[:300]}"
        )
    no_available_locations_flag = False
    failed_request_specs = []
    failure_reasons = []
    successful_request_count = 0
    global_stop_reason = None
    futures = []
    future_to_request_spec = {}
    executor = ThreadPoolExecutor(max_workers=int(len(locations) * 2))

    def cancel_pending(reason):
        nonlocal global_stop_reason
        if global_stop_reason is None:
            global_stop_reason = reason
        for pending_future in futures:
            if not pending_future.done():
                pending_future.cancel()

    try:
        for row in api_calls_df.itertuples(index=False):
            for desired_count in desired_counts:
                request_spec = {
                    "regions": list(row.Regions),
                    "instance_types": list(row.InstanceTypes),
                    "desired_count": desired_count,
                }
                future = executor.submit(
                    execute_spot_placement_score_api,
                    list(row.Regions),
                    list(row.InstanceTypes),
                    desired_count,
                    max_retries=50,
                )
                futures.append(future)
                future_to_request_spec[future] = request_spec

        for future in as_completed(futures):
            request_spec = future_to_request_spec[future]
            if future.cancelled():
                outcome = SPSRequestOutcome.unavailable(
                    global_stop_reason or FANOUT_CANCELLED
                )
            else:
                try:
                    outcome = future.result()
                except CancelledError:
                    outcome = SPSRequestOutcome.unavailable(
                        global_stop_reason or FANOUT_CANCELLED
                    )
                except Exception as error:
                    outcome = SPSRequestOutcome.fatal(
                        UNEXPECTED_ERROR,
                        error,
                    )

            if not isinstance(outcome, SPSRequestOutcome):
                outcome = SPSRequestOutcome.fatal(
                    UNEXPECTED_ERROR,
                    f"worker returned {type(outcome).__name__} instead of SPSRequestOutcome",
                )

            if outcome.status is SPSRequestStatus.SUCCESS:
                successful_request_count += 1
                response = outcome.response or {}
                desired_count = request_spec["desired_count"]
                for score in response.get("placementScores", []):
                    score_value = map_score_to_int(score.get("score"))
                    score_data = {
                        "DesiredCount": desired_count,
                        "RegionCodeSPS": score.get("region"),
                        "Region": SS_Resources.region_map_and_instance_map_tmp['region_map'].get(
                            score.get("region", ""), ""),
                        "InstanceTypeSPS": score.get("sku"),
                        "InstanceTier": SS_Resources.region_map_and_instance_map_tmp['instance_map'].get(
                            score.get("sku", ""), {}).get("InstanceTier"),
                        "InstanceType": SS_Resources.region_map_and_instance_map_tmp['instance_map'].get(
                            score.get("sku", ""), {}).get("InstanceTypeOld"),
                        "Score": score_value,
                        "T3": desired_count if score_value >= 3 else 0,
                        "T2": desired_count if score_value >= 2 else 0,
                    }
                    if availability_zones is True:
                        score_data["AvailabilityZone"] = score.get(
                            "availabilityZone",
                            "Single",
                        )
                    results.append(score_data)
                continue

            failed_request_specs.append(request_spec)
            failure_reasons.append(outcome.reason or UNEXPECTED_ERROR)
            if outcome.error:
                print(
                    "[SPS_REQUEST_FAILURE] "
                    f"reason={outcome.reason or UNEXPECTED_ERROR} "
                    f"error={outcome.error}"
                )

            if outcome.reason == NO_AVAILABLE_LOCATIONS:
                no_available_locations_flag = True

            if (
                outcome.status is SPSRequestStatus.FATAL
                or outcome.reason
                in {
                    DEADLINE_EXCEEDED,
                    LEASE_SUPERSEDED,
                    ACTIVE_LEASE_LOST,
                    NO_AVAILABLE_LOCATIONS,
                }
            ):
                cancel_pending(outcome.reason or UNEXPECTED_ERROR)
    finally:
        executor.shutdown(wait=True)
        finish_collection = getattr(collection_deadline, "finish", None)
        if finish_collection:
            finish_collection()
        save_tmp_files_to_s3()
        if no_available_locations_flag:
            current_utc_time = datetime.now(timezone.utc).strftime("%Y_%m_%dT%H_%M_%S")
            S3.upload_file(
                SS_Resources.locations_call_history_tmp,
                f"{AZURE_CONST.ERROR_LOCATIONS_CALL_HISTORY_JSON_PATH}/{current_utc_time}.json",
                "json",
            )
            print("No available locations found. Cancelling remaining tasks. ")

    failed_request_count = len(failed_request_specs)
    total_request_count = len(futures)
    if successful_request_count + failed_request_count != total_request_count:
        raise RuntimeError(
            "SPS request accounting invariant failed: "
            f"completed={successful_request_count} failed={failed_request_count} "
            f"total={total_request_count}"
        )

    if failed_request_specs:
        results.extend(_build_unavailable_sps_rows(failed_request_specs))
        partial_sps_df = _build_sps_dataframe(results)
        error_kwargs = {
            "started_at": getattr(collection_deadline, "started_at", None),
            "deadline_at": getattr(collection_deadline, "deadline_at", None),
            "desired_counts": desired_counts,
            "partial_sps_df": partial_sps_df,
            "completed_request_count": successful_request_count,
            "failed_request_count": failed_request_count,
            "total_request_count": total_request_count,
            "failure_reasons": failure_reasons,
        }

    if LEASE_SUPERSEDED in failure_reasons:
        error_kwargs.pop("deadline_at", None)
        raise SPSCollectionSupersededError(**error_kwargs)
    if DEADLINE_EXCEEDED in failure_reasons:
        raise SPSCollectionDeadlineError(
            **error_kwargs,
        )
    if failed_request_specs:
        raise SPSCollectionRequestError(**error_kwargs)

    sps_res_df = _build_sps_dataframe(results)

    print(f"execute_spot_placement_score_task_by_parameter_pool_df Successfully! availability_zones: {availability_zones}")
    return sps_res_df


def map_score_to_int(score_val):
    if isinstance(score_val, (int, float)):
        return int(score_val)

    score_map = {
        "High": 3,
        "Medium": 2,
        "Low": 1
    }
    return score_map.get(score_val, -1)


def build_sps_request_hash(request_body):
    normalized = {
        "availabilityZones": request_body["availabilityZones"],
        "desiredCount": request_body["desiredCount"],
        "desiredLocations": sorted(request_body["desiredLocations"]),
        "desiredSizes": sorted(
            request_body["desiredSizes"], key=lambda item: item["sku"]
        ),
    }
    serialized = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _record_location_health(location, *, outcome, elapsed_seconds, route_mode):
    try:
        with SS_Resources.location_lock:
            SL_Manager.record_location_result(
                location,
                outcome=outcome,
                elapsed_seconds=elapsed_seconds,
                route_mode=route_mode,
            )
    except Exception as health_error:
        print(
            "[SPS_LOCATION_HEALTH] action=record_failed "
            f"location={location} error={str(health_error)[:300]}"
        )


def execute_spot_placement_score_api(region_chunk, instance_type_chunk, desired_count, max_retries=12):
    retries = 0
    exhausted_reason = RETRY_EXHAUSTED
    while retries <= max_retries:
        timeout_metrics = getattr(SS_Resources, "timeout_metrics", None)
        collection_deadline = getattr(SS_Resources, "collection_deadline", None)
        stop_result = _collection_stop_result(collection_deadline)
        if stop_result:
            return SPSRequestOutcome.unavailable(stop_result)

        region_chunk = filter_invalid_items(region_chunk, "invalid_regions")
        instance_type_chunk = filter_invalid_items(instance_type_chunk, "invalid_instance_types")

        if region_chunk is None or instance_type_chunk is None:
            print(f"execute_spot_placement_score_api: Execution skipped as filtered chunks are empty. "
                  f"region_chunk: {region_chunk}, instance_type_chunk: {instance_type_chunk}")
            return SPSRequestOutcome.success(
                {"placementScores": []},
                reason=FILTERED_INVALID_PARAMETERS,
            )

        request_body = {
            "availabilityZones": availability_zones,
            "desiredCount": desired_count,
            'desiredLocations' : region_chunk,
            'desiredSizes' : [{"sku": instance_type} for instance_type in instance_type_chunk]
        }
        request_hash = build_sps_request_hash(request_body)

        with SS_Resources.location_lock:
            res = SL_Manager.get_next_available_location()
            if res is None:
                return SPSRequestOutcome.unavailable(NO_AVAILABLE_LOCATIONS)
            elif len(res) == 3:
                subscription_id, location, route_mode = res
            else:
                subscription_id, location = res
                route_mode = "normal"

        url = f"https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.Compute/locations/{location}/placementScores/spot/generate?api-version=2025-06-05"
        client_request_id = str(uuid.uuid4())
        headers = {
            "Authorization": f"Bearer {sps_shared_resources.sps_token}",
            "Content-Type": "application/json",
            "x-ms-client-request-id": client_request_id,
            "x-ms-return-client-request-id": "true",
        }
        try:
            request_timeout = (
                SL_Manager.PROBE_SUCCESS_SECONDS
                if route_mode == "probe"
                else 50
            )
            if collection_deadline:
                remaining_seconds = collection_deadline.remaining_seconds()
                if remaining_seconds <= 0:
                    return SPSRequestOutcome.unavailable(DEADLINE_EXCEEDED)
                request_timeout = min(request_timeout, remaining_seconds)
            request_started = time.monotonic()
            response = requests.post(
                url,
                headers=headers,
                json=request_body,
                timeout=request_timeout,
            )
            response.raise_for_status()
            elapsed = time.monotonic() - request_started
            azure_request_id = response.headers.get("x-ms-request-id", "missing")
            response_payload = response.json()
            if (
                not isinstance(response_payload, dict)
                or not isinstance(response_payload.get("placementScores"), list)
            ):
                return SPSRequestOutcome.fatal(
                    INVALID_RESPONSE,
                    "response must contain a placementScores list",
                )
            _record_location_health(
                location,
                outcome="success",
                elapsed_seconds=elapsed,
                route_mode=route_mode,
            )
            print(
                f"[SPS_SUCCESS] elapsed_seconds={elapsed:.3f} attempt={retries + 1} "
                f"location={location} route_mode={route_mode} desired_count={desired_count} "
                f"region_count={len(region_chunk)} sku_count={len(instance_type_chunk)} "
                f"request_hash={request_hash} client_request_id={client_request_id} "
                f"azure_request_id={azure_request_id}"
            )
            SS_Resources.succeed_to_get_sps_count += 1
            if timeout_metrics:
                timeout_metrics.record_success()
            return SPSRequestOutcome.success(response_payload)

        except requests.exceptions.Timeout as timeout_error:
            exhausted_reason = RETRY_EXHAUSTED
            elapsed = time.monotonic() - request_started
            if timeout_metrics:
                timeout_metrics.record_timeout()
            _record_location_health(
                location,
                outcome="timeout",
                elapsed_seconds=elapsed,
                route_mode=route_mode,
            )
            print(
                f"[SPS_TIMEOUT] type={type(timeout_error).__name__} elapsed_seconds={elapsed:.3f} "
                f"attempt={retries + 1} location={location} route_mode={route_mode} "
                f"desired_count={desired_count} "
                f"region_count={len(region_chunk)} sku_count={len(instance_type_chunk)} "
                f"request_hash={request_hash} client_request_id={client_request_id}"
            )
            stop_result = _collection_stop_result(collection_deadline)
            if stop_result:
                return SPSRequestOutcome.unavailable(stop_result)
            retries = handle_retry(
                "Timeout",
                retries,
                max_retries,
                collection_deadline=collection_deadline,
            )
            stop_result = _collection_stop_result(collection_deadline)
            if stop_result:
                return SPSRequestOutcome.unavailable(stop_result)

        except requests.exceptions.ConnectionError as connection_error:
            exhausted_reason = CONNECTION_RETRY_EXHAUSTED
            elapsed = time.monotonic() - request_started
            print(
                f"[SPS_CONNECTION_ERROR] type={type(connection_error).__name__} "
                f"elapsed_seconds={elapsed:.3f} attempt={retries + 1} "
                f"location={location} desired_count={desired_count} "
                f"region_count={len(region_chunk)} sku_count={len(instance_type_chunk)} "
                f"request_hash={request_hash} client_request_id={client_request_id} "
                f"error={json.dumps(str(connection_error)[:500])}"
            )
            stop_result = _collection_stop_result(collection_deadline)
            if stop_result:
                return SPSRequestOutcome.unavailable(stop_result)
            retries = handle_retry(
                "ConnectionError",
                retries,
                min(max_retries, CONNECTION_MAX_RETRIES),
                collection_deadline=collection_deadline,
            )
            stop_result = _collection_stop_result(collection_deadline)
            if stop_result:
                return SPSRequestOutcome.unavailable(stop_result)

        except requests.exceptions.HTTPError as http_err:
            http_response = http_err.response
            status_code = getattr(http_response, "status_code", None)
            error_message = getattr(http_response, "text", str(http_err))
            elapsed = time.monotonic() - request_started
            response_headers = getattr(http_response, "headers", {}) or {}
            azure_request_id = response_headers.get("x-ms-request-id", "missing")

            if status_code is not None and 500 <= status_code < 600:
                exhausted_reason = SERVER_ERROR_RETRY_EXHAUSTED
                print(
                    f"[SPS_SERVER_ERROR] status_code={status_code} "
                    f"elapsed_seconds={elapsed:.3f} attempt={retries + 1} "
                    f"location={location} route_mode={route_mode} "
                    f"desired_count={desired_count} "
                    f"region_count={len(region_chunk)} sku_count={len(instance_type_chunk)} "
                    f"request_hash={request_hash} client_request_id={client_request_id} "
                    f"azure_request_id={azure_request_id} "
                    f"error={json.dumps(error_message[:500])}"
                )
                stop_result = _collection_stop_result(collection_deadline)
                if stop_result:
                    return SPSRequestOutcome.unavailable(stop_result)
                retries = handle_retry(
                    "ServerError",
                    retries,
                    min(max_retries, SERVER_ERROR_MAX_RETRIES),
                    collection_deadline=collection_deadline,
                )
                stop_result = _collection_stop_result(collection_deadline)
                if stop_result:
                    return SPSRequestOutcome.unavailable(stop_result)
                if retries is None:
                    break
                continue

            match_res = extract_invalid_values(error_message)
            if match_res:
                if match_res["invalid_region"]:
                    region_chunk = del_invalid_chunk(region_chunk, match_res["invalid_region"], "invalid_region")
                    if region_chunk is None:
                        print(f"This retry will not execute because, after filtering, the region_chunk becomes empty.")
                        return SPSRequestOutcome.success(
                            {"placementScores": []},
                            reason=FILTERED_INVALID_PARAMETERS,
                        )
                    retries = handle_retry(
                        "InvalidRegion",
                        retries,
                        max_retries,
                        collection_deadline=collection_deadline,
                    )

                if match_res["invalid_instanceType"]:
                    instance_type_chunk = del_invalid_chunk(instance_type_chunk, match_res["invalid_instanceType"],"invalid_instanceType")
                    if instance_type_chunk is None:
                        print(f"This retry will not execute because, after filtering, the instance_type_chunk becomes empty.")
                        return SPSRequestOutcome.success(
                            {"placementScores": []},
                            reason=FILTERED_INVALID_PARAMETERS,
                        )
                    retries = handle_retry(
                        "InvalidInstanceType",
                        retries,
                        max_retries,
                        collection_deadline=collection_deadline,
                    )

            if "BadGatewayConnection" in error_message:
                print(f"[DEBUG_ERROR] BadGatewayConnection occurred!")
                print(f"URL: {url}")
                retries = handle_retry(
                    "BadGatewayConnection",
                    retries,
                    max_retries,
                    collection_deadline=collection_deadline,
                )

            elif "InvalidParameter" in error_message:
                print(f"HTTP error occurred: {error_message}")
                retries = handle_retry(
                    "InvalidParameter",
                    retries,
                    max_retries,
                    collection_deadline=collection_deadline,
                )

            elif "You have reached the maximum number of requests allowed." in error_message:
                if SS_Resources.too_many_requests_count == 0:
                    print(f"HTTP error occurred: {error_message}")
                SL_Manager.update_over_limit_locations(subscription_id, location)
                retries = handle_retry(
                    "Too Many Requests",
                    retries,
                    max_retries,
                    collection_deadline=collection_deadline,
                )

            elif "Max retries exceeded with url" in error_message:
                print(f"HTTP error occurred: {error_message}")
                SL_Manager.update_over_limit_locations(subscription_id, location)
                retries = handle_retry(
                    "Too Many Requests(2)",
                    retries,
                    max_retries,
                    collection_deadline=collection_deadline,
                )

            elif not match_res:
                print(
                    f"[SPS_HTTP_ERROR] status_code={status_code} "
                    f"elapsed_seconds={elapsed:.3f} attempt={retries + 1} "
                    f"location={location} route_mode={route_mode} "
                    f"desired_count={desired_count} "
                    f"region_count={len(region_chunk)} sku_count={len(instance_type_chunk)} "
                    f"request_hash={request_hash} client_request_id={client_request_id} "
                    f"azure_request_id={azure_request_id} "
                    f"error={json.dumps(error_message[:500])}"
                )
                return SPSRequestOutcome.fatal(
                    UNEXPECTED_ERROR,
                    f"HTTP {status_code}: {error_message[:500]}",
                )

        except Exception as e:
            print(f"execute_spot_placement_score_api. An unexpected error occurred: {e}")
            return SPSRequestOutcome.fatal(UNEXPECTED_ERROR, e)

        if retries is None:
            break
        if retries:
            continue

    if retries is None:
        print(f"SPS request retries exhausted: reason={exhausted_reason}")
    return SPSRequestOutcome.unavailable(exhausted_reason)


def extract_invalid_values(error_message):
    region_match = re.search(
        r"The value '([a-zA-Z0-9-_]+)' provided for the input parameter 'desiredLocations' is not valid",
        error_message
    )

    instance_type_match = re.search(
        r"The value '([a-zA-Z0-9-_]+)' provided for the input parameter 'SpotPlacementRecommenderInput.desiredSizes' is not valid",
        error_message
    )

    if not region_match and not instance_type_match:
        return None

    match_res = {
        "invalid_region": region_match.group(1) if region_match else None,
        "invalid_instanceType": instance_type_match.group(1) if instance_type_match else None
    }
    return match_res


def initialize_files_in_s3():
    try:
        az_str = f"availability-zones-{str(availability_zones).lower()}"
        files_to_initialize = {
            f"{AZURE_CONST.S3_SAVED_VARIABLE_PATH}/{az_str}/{AZURE_CONST.S3_INVALID_REGIONS_JSON_FILENAME}": [],
            f"{AZURE_CONST.S3_SAVED_VARIABLE_PATH}/{az_str}/{AZURE_CONST.S3_INVALID_INSTANCE_TYPES_JSON_FILENAME}": []
        }

        for file_path, data in files_to_initialize.items():
            S3.upload_file(data, file_path, "json")

        print("Successfully initialized files in S3.")
        return True

    except Exception as e:
        print(f"An error occurred during S3 initialization: {e}")
        return False


def del_invalid_chunk(chunk, invalid_value, value_type):
    with SS_Resources.lock:
        if value_type == "invalid_region":
            if invalid_value not in SS_Resources.invalid_regions_tmp:
                SS_Resources.invalid_regions_tmp.append(invalid_value)

        elif value_type == "invalid_instanceType":
            if invalid_value not in SS_Resources.invalid_instance_types_tmp:
                SS_Resources.invalid_instance_types_tmp.append(invalid_value)

    if invalid_value in chunk:
        chunk.remove(invalid_value)
    else:
        print(f"x not in list, invalid_value: {invalid_value}, chunk: {chunk}")

    return chunk if chunk else None


def handle_retry(error_type, retries, max_retries, *, collection_deadline=None):
    if error_type == "Timeout":
        SS_Resources.time_out_retry_count += 1
    elif error_type == "ConnectionError":
        SS_Resources.connection_error_retry_count += 1
    elif error_type == "ServerError":
        SS_Resources.server_error_retry_count += 1
    elif error_type == "BadGatewayConnection":
        SS_Resources.bad_request_retry_count += 1
    elif error_type == "Too Many Requests":
        SS_Resources.too_many_requests_count += 1
    elif error_type == "Too Many Requests(2)":
        SS_Resources.too_many_requests_count_2 += 1
    elif error_type == "InvalidRegion":
        SS_Resources.found_invalid_region_retry_count += 1
    elif error_type == "InvalidInstanceType":
        SS_Resources.found_invalid_instance_type_retry_count += 1


    if retries < max_retries:
        if error_type == "ConnectionError":
            backoff_seconds = min(2 ** retries, CONNECTION_BACKOFF_MAX_SECONDS)
            sleep_time = backoff_seconds * random.uniform(0.8, 1.2)
        else:
            sleep_time = round(random.uniform(0.5, 1.5), 1)
        if collection_deadline:
            sleep_time = max(
                0,
                min(sleep_time, collection_deadline.remaining_seconds()),
            )
        if sleep_time > 0:
            time.sleep(sleep_time)
        retries += 1
        return retries
    else:
        return None


def filter_invalid_items(items, invalid_type):
    if invalid_type == "invalid_regions":
        invalid_data = SS_Resources.invalid_regions_tmp
    elif invalid_type == "invalid_instance_types":
        invalid_data = SS_Resources.invalid_instance_types_tmp
    else:
        return None

    filtered_items = [item for item in items if item not in invalid_data]
    return filtered_items if filtered_items else None


def initialize_sps_count_resources():
    SS_Resources.bad_request_retry_count = 0
    SS_Resources.time_out_retry_count = 0
    SS_Resources.connection_error_retry_count = 0
    SS_Resources.server_error_retry_count = 0
    SS_Resources.too_many_requests_count = 0
    SS_Resources.too_many_requests_count_2 = 0
    SS_Resources.found_invalid_region_retry_count = 0
    SS_Resources.found_invalid_instance_type_retry_count = 0
    SS_Resources.succeed_to_get_sps_count = 0
    SS_Resources.succeed_to_get_next_available_location_count = 0
    SS_Resources.timeout_metrics = TimeoutWindowMetrics()

def save_tmp_files_to_s3():
    az_str = f"availability-zones-{str(availability_zones).lower()}"
    base_path = AZURE_CONST.S3_SAVED_VARIABLE_PATH
    files_to_upload = {
        f"{base_path}/{az_str}/{AZURE_CONST.S3_INVALID_REGIONS_JSON_FILENAME}": SS_Resources.invalid_regions_tmp,
        f"{base_path}/{az_str}/{AZURE_CONST.S3_INVALID_INSTANCE_TYPES_JSON_FILENAME}": SS_Resources.invalid_instance_types_tmp,
        f"{base_path}/{az_str}/{AZURE_CONST.S3_LOCATIONS_CALL_HISTORY_JSON_FILENAME}": SS_Resources.locations_call_history_tmp,
        f"{base_path}/{az_str}/{AZURE_CONST.S3_LAST_SUBSCRIPTION_ID_AND_LOCATION_JSON_FILENAME}": SS_Resources.last_subscription_id_and_location_tmp,
        f"{base_path}/{az_str}/{AZURE_CONST.S3_LOCATIONS_OVER_LIMIT_JSON_FILENAME}": SS_Resources.locations_over_limit_tmp,
        f"{base_path}/{az_str}/{AZURE_CONST.S3_LOCATION_HEALTH_JSON_FILENAME}": SS_Resources.location_health_tmp,
    }

    for file_path, file_data in files_to_upload.items():
        if file_data:
            S3.upload_file(file_data, file_path, "json")

def get_variable_from_s3():
    try:
        az_str = f"availability-zones-{str(availability_zones).lower()}"
        base_path = AZURE_CONST.S3_SAVED_VARIABLE_PATH

        invalid_regions_data = S3.read_file(f"{base_path}/{az_str}/{AZURE_CONST.S3_INVALID_REGIONS_JSON_FILENAME}", 'json')
        instance_types_data = S3.read_file(f"{base_path}/{az_str}/{AZURE_CONST.S3_INVALID_INSTANCE_TYPES_JSON_FILENAME}", 'json')
        call_history_data = S3.read_file(f"{base_path}/{az_str}/{AZURE_CONST.S3_LOCATIONS_CALL_HISTORY_JSON_FILENAME}", 'json')
        over_limit_data = S3.read_file(f"{base_path}/{az_str}/{AZURE_CONST.S3_LOCATIONS_OVER_LIMIT_JSON_FILENAME}", 'json')
        location_health_data = S3.read_file(f"{base_path}/{az_str}/{AZURE_CONST.S3_LOCATION_HEALTH_JSON_FILENAME}", 'json')
        last_subscription_id_and_location = S3.read_file(f"{base_path}/{az_str}/{AZURE_CONST.S3_LAST_SUBSCRIPTION_ID_AND_LOCATION_JSON_FILENAME}", 'json')
        region_map_and_instance_map = S3.read_file(f"{base_path}/{az_str}/{AZURE_CONST.S3_REGION_MAP_AND_INSTANCE_MAP_JSON_FILENAME}", 'json')

        SS_Resources.invalid_regions_tmp = invalid_regions_data
        SS_Resources.invalid_instance_types_tmp = instance_types_data
        SS_Resources.locations_call_history_tmp = call_history_data
        SS_Resources.locations_over_limit_tmp = over_limit_data
        SS_Resources.location_health_tmp = location_health_data or {
            "version": 1,
            "locations": {},
        }
        SS_Resources.last_subscription_id_and_location_tmp = last_subscription_id_and_location
        SS_Resources.region_map_and_instance_map_tmp = {
            "region_map": region_map_and_instance_map.get('region_map'),
            "instance_map": region_map_and_instance_map.get('instance_map')
        }

        if all(data is not None for data in [
            SS_Resources.invalid_regions_tmp,
            SS_Resources.invalid_instance_types_tmp,
            SS_Resources.locations_call_history_tmp,
            SS_Resources.locations_over_limit_tmp,
            SS_Resources.last_subscription_id_and_location_tmp,
            SS_Resources.region_map_and_instance_map_tmp
        ]):
            print("[S3]: Successfully prepared variable from s3.")
            return True

        else:
            return False

    except KeyError as e:
        print(f"Missing expected key in S3 JSON data: {e}")
        return False
    except Exception as e:
        print(f"Error loading files from S3: {e}")
        return False

def collect_regions_and_instance_types_df_by_priceapi():
    try:
        price_source_df = load_price.collect_price_with_multithreading()
        if price_source_df.empty:
             return None, None, None

        price_source_df = price_source_df[price_source_df['InstanceTier'].notna()]
        price_source_df['InstanceTypeNew'] = price_source_df.apply(
            lambda row: f"{row['InstanceTier']}_{row['InstanceType']}" if pd.notna(row['InstanceTier']) else row[
                'InstanceType'], axis=1
        )
        regions_and_instance_types_df = price_source_df[['armRegionName', 'Region', 'InstanceTypeNew', 'InstanceTier', 'InstanceType']]
        regions_and_instance_types_df = regions_and_instance_types_df.rename(columns={
            'InstanceType': 'InstanceTypeOld',
            'armRegionName': 'RegionCode',
            'InstanceTypeNew': 'InstanceType'
        })

        regions_and_instance_types_df['RegionCode'] = regions_and_instance_types_df['RegionCode'].map(lambda x: x.strip() if isinstance(x, str) else x)
        regions_and_instance_types_df['InstanceType'] = regions_and_instance_types_df['InstanceType'].map(lambda x: x.strip() if isinstance(x, str) else x)


        region_map = regions_and_instance_types_df[['RegionCode', 'Region']].drop_duplicates().set_index('RegionCode')['Region'].to_dict()

        instance_map = regions_and_instance_types_df[['InstanceType', 'InstanceTier', 'InstanceTypeOld']].drop_duplicates().set_index('InstanceType').to_dict(orient='index')

        return regions_and_instance_types_df, region_map, instance_map

    except Exception as e:
        print(f"Failed to collect_regions_and_instance_types_df_by_priceapi, Error: {e}")
        return None, None, None


def prepare_the_variables():
    res = get_variable_from_s3()
    SL_Manager.check_and_add_available_locations(availability_zones)
    initialize_sps_count_resources()
    return res
