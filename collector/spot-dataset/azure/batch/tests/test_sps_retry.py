import importlib.util
import re
import sys
import threading
import types
from pathlib import Path

import pandas as pd
import pytest
import requests


def _load_sps_module(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).parents[1] / "sps"))
    resources = types.ModuleType("sps_shared_resources")
    resources.time_out_retry_count = 0
    resources.connection_error_retry_count = 0
    resources.server_error_retry_count = 0
    resources.bad_request_retry_count = 0
    resources.too_many_requests_count = 0
    resources.too_many_requests_count_2 = 0
    resources.found_invalid_region_retry_count = 0
    resources.found_invalid_instance_type_retry_count = 0
    resources.succeed_to_get_sps_count = 0
    resources.location_lock = threading.Lock()
    resources.invalid_regions_tmp = []
    resources.invalid_instance_types_tmp = []
    resources.locations_call_history_tmp = {"subscription": {"location": []}}
    resources.locations_over_limit_tmp = {}
    resources.last_subscription_id_and_location_tmp = {}
    resources.location_health_tmp = {"version": 1, "locations": {}}

    class FakeS3:
        @staticmethod
        def upload_file(*args, **kwargs):
            return None

    class FakeLogger:
        @staticmethod
        def info(*args, **kwargs):
            return None

    location_manager = types.ModuleType("sps_location_manager")
    location_manager.get_next_available_location = lambda: ("subscription", "location")
    location_manager.prepare_location_routing = lambda **kwargs: None
    location_manager.record_location_result = lambda *args, **kwargs: None
    location_manager.PROBE_SUCCESS_SECONDS = 10

    sps_module = types.ModuleType("sps_module")
    sps_module.sps_location_manager = location_manager
    sps_module.sps_shared_resources = resources
    sps_module.sps_prepare_parameters = types.ModuleType("sps_prepare_parameters")

    price = types.ModuleType("price")
    price.collect_price = types.ModuleType("collect_price")

    azure_auth = types.ModuleType("utils.azure_auth")
    azure_auth.get_sps_token_and_subscriptions = lambda _: ("token", ["subscription"])

    common = types.ModuleType("utils.common")
    common.S3 = FakeS3()
    common.Logger = FakeLogger()

    constants = types.ModuleType("utils.constants")
    constants.AZURE_CONST = object()

    utils = types.ModuleType("utils")
    monkeypatch.setitem(sys.modules, "price", price)
    monkeypatch.setitem(sys.modules, "sps_module", sps_module)
    monkeypatch.setitem(sys.modules, "utils", utils)
    monkeypatch.setitem(sys.modules, "utils.azure_auth", azure_auth)
    monkeypatch.setitem(sys.modules, "utils.common", common)
    monkeypatch.setitem(sys.modules, "utils.constants", constants)

    module_path = Path(__file__).parents[1] / "sps" / "load_sps.py"
    spec = importlib.util.spec_from_file_location("azure_batch_load_sps_test", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, resources


def test_timeout_retry_exhaustion_returns_unavailable_outcome(monkeypatch):
    load_sps, resources = _load_sps_module(monkeypatch)

    def raise_timeout(*args, **kwargs):
        raise requests.exceptions.Timeout

    monkeypatch.setattr(load_sps.requests, "post", raise_timeout)
    monkeypatch.setattr(load_sps.time, "sleep", lambda _: None)

    result = load_sps.execute_spot_placement_score_api(
        ["region"], ["instance"], desired_count=40, max_retries=2
    )

    assert result.status is load_sps.SPSRequestStatus.UNAVAILABLE
    assert result.reason == load_sps.RETRY_EXHAUSTED
    assert resources.time_out_retry_count == 3


def test_widespread_timeouts_continue_until_retry_limit_without_deadline(monkeypatch):
    load_sps, resources = _load_sps_module(monkeypatch)
    resources.timeout_metrics = load_sps.TimeoutWindowMetrics()

    def raise_timeout(*args, **kwargs):
        raise requests.exceptions.ReadTimeout

    monkeypatch.setattr(load_sps.requests, "post", raise_timeout)
    monkeypatch.setattr(load_sps.time, "sleep", lambda _: None)

    result = load_sps.execute_spot_placement_score_api(
        ["region"], ["instance"], desired_count=40, max_retries=50
    )

    assert result.status is load_sps.SPSRequestStatus.UNAVAILABLE
    assert result.reason == load_sps.RETRY_EXHAUSTED
    assert resources.time_out_retry_count == 51


def test_transient_connection_errors_retry_with_backoff_and_recover(monkeypatch):
    load_sps, resources = _load_sps_module(monkeypatch)
    sleep_seconds = []
    call_count = 0

    class SuccessfulResponse:
        headers = {"x-ms-request-id": "azure-request-after-reconnect"}

        @staticmethod
        def raise_for_status():
            return None

        @staticmethod
        def json():
            return {"placementScores": []}

    def reconnect_on_third_call(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise requests.exceptions.ConnectionError(
                "Failed to establish a new connection: [Errno 101] Network is unreachable"
            )
        return SuccessfulResponse()

    monkeypatch.setattr(load_sps.requests, "post", reconnect_on_third_call)
    monkeypatch.setattr(load_sps.random, "uniform", lambda *_: 1.0)
    monkeypatch.setattr(load_sps.time, "sleep", sleep_seconds.append)

    result = load_sps.execute_spot_placement_score_api(
        ["region"], ["instance"], desired_count=40, max_retries=50
    )

    assert result.status is load_sps.SPSRequestStatus.SUCCESS
    assert call_count == 3
    assert resources.connection_error_retry_count == 2
    assert sleep_seconds == [1.0, 2.0]


def test_connection_retry_exhaustion_only_marks_request_unavailable(monkeypatch):
    load_sps, resources = _load_sps_module(monkeypatch)
    sleep_seconds = []
    call_count = 0

    def connection_error(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        raise requests.exceptions.ConnectionError("Network is unreachable")

    monkeypatch.setattr(load_sps.requests, "post", connection_error)
    monkeypatch.setattr(load_sps.random, "uniform", lambda *_: 1.0)
    monkeypatch.setattr(load_sps.time, "sleep", sleep_seconds.append)

    result = load_sps.execute_spot_placement_score_api(
        ["region"], ["instance"], desired_count=40, max_retries=50
    )

    assert result.status is load_sps.SPSRequestStatus.UNAVAILABLE
    assert result.reason == load_sps.CONNECTION_RETRY_EXHAUSTED
    assert call_count == load_sps.CONNECTION_MAX_RETRIES + 1
    assert resources.connection_error_retry_count == call_count
    assert sleep_seconds == [1.0, 2.0, 4.0, 8.0, 10.0, 10.0, 10.0, 10.0]


def test_http_500_retries_on_another_location_and_recovers(monkeypatch, capsys):
    load_sps, resources = _load_sps_module(monkeypatch)
    monkeypatch.setattr(load_sps.time, "sleep", lambda _: None)
    locations = iter(
        [
            ("subscription", "slow-location", "normal"),
            ("subscription", "healthy-location", "normal"),
        ]
    )
    monkeypatch.setattr(
        load_sps.SL_Manager,
        "get_next_available_location",
        lambda: next(locations),
    )
    client_request_ids = iter(["client-500", "client-success"])
    monkeypatch.setattr(
        load_sps.uuid,
        "uuid4",
        lambda: next(client_request_ids),
    )

    class ServerErrorResponse:
        status_code = 500
        text = (
            '{"error":{"code":"InternalServerError","message":'
            '"Encountered unknown error while generating recommendations."}}'
        )
        headers = {"x-ms-request-id": "azure-500"}

        def raise_for_status(self):
            raise requests.exceptions.HTTPError(response=self)

    class SuccessfulResponse:
        headers = {"x-ms-request-id": "azure-success"}

        @staticmethod
        def raise_for_status():
            return None

        @staticmethod
        def json():
            return {"placementScores": []}

    responses = iter([ServerErrorResponse(), SuccessfulResponse()])
    monkeypatch.setattr(
        load_sps.requests,
        "post",
        lambda *args, **kwargs: next(responses),
    )

    result = load_sps.execute_spot_placement_score_api(
        ["region"], ["instance"], desired_count=50, max_retries=2
    )

    assert result.status is load_sps.SPSRequestStatus.SUCCESS
    assert resources.server_error_retry_count == 1
    output = capsys.readouterr().out
    assert "[SPS_SERVER_ERROR]" in output
    assert "status_code=500" in output
    assert "location=slow-location" in output
    assert "request_hash=" in output
    assert "client_request_id=client-500" in output
    assert "azure_request_id=azure-500" in output
    assert "location=healthy-location" in output


def test_http_500_retry_exhaustion_only_marks_request_unavailable(monkeypatch):
    load_sps, resources = _load_sps_module(monkeypatch)
    monkeypatch.setattr(load_sps.time, "sleep", lambda _: None)
    call_count = 0

    class ServerErrorResponse:
        status_code = 500
        text = '{"error":{"code":"InternalServerError"}}'
        headers = {}

        def raise_for_status(self):
            raise requests.exceptions.HTTPError(response=self)

    def server_error(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        return ServerErrorResponse()

    monkeypatch.setattr(load_sps.requests, "post", server_error)

    result = load_sps.execute_spot_placement_score_api(
        ["region"], ["instance"], desired_count=50, max_retries=2
    )

    assert result.status is load_sps.SPSRequestStatus.UNAVAILABLE
    assert result.reason == load_sps.SERVER_ERROR_RETRY_EXHAUSTED
    assert call_count == 3
    assert resources.server_error_retry_count == 3


def test_unrecognized_http_4xx_remains_fatal(monkeypatch):
    load_sps, _ = _load_sps_module(monkeypatch)

    class ClientErrorResponse:
        status_code = 418
        text = '{"error":{"code":"UnexpectedClientError"}}'
        headers = {}

        def raise_for_status(self):
            raise requests.exceptions.HTTPError(response=self)

    monkeypatch.setattr(
        load_sps.requests,
        "post",
        lambda *args, **kwargs: ClientErrorResponse(),
    )

    result = load_sps.execute_spot_placement_score_api(
        ["region"], ["instance"], desired_count=50, max_retries=2
    )

    assert result.status is load_sps.SPSRequestStatus.FATAL
    assert result.reason == load_sps.UNEXPECTED_ERROR
    assert "HTTP 418" in result.error


def test_unexpected_request_exception_remains_fatal(monkeypatch):
    load_sps, _ = _load_sps_module(monkeypatch)

    def raise_programming_error(*args, **kwargs):
        raise ValueError("broken invariant")

    monkeypatch.setattr(load_sps.requests, "post", raise_programming_error)

    result = load_sps.execute_spot_placement_score_api(
        ["region"], ["instance"], desired_count=40, max_retries=50
    )

    assert result.status is load_sps.SPSRequestStatus.FATAL
    assert result.reason == load_sps.UNEXPECTED_ERROR
    assert result.error == "broken invariant"


def test_mixed_success_and_retry_exhaustion_is_saved_as_partial(monkeypatch):
    load_sps, resources = _load_sps_module(monkeypatch)
    resources.region_map_and_instance_map_tmp = {
        "region_map": {
            "successful-region": "Successful Region",
            "failed-region": "Failed Region",
        },
        "instance_map": {
            "successful-instance": {
                "InstanceTier": "Standard",
                "InstanceTypeOld": "D2s_v5",
            },
            "failed-instance": {
                "InstanceTier": "Standard",
                "InstanceTypeOld": "D4s_v5",
            },
        },
    }
    resources.collection_deadline = types.SimpleNamespace(
        started_at=880,
        deadline_at=1_480,
        start=lambda: 1_480,
    )

    def complete_or_exhaust(regions, instance_types, desired_count, **kwargs):
        if regions == ["failed-region"]:
            return load_sps.SPSRequestOutcome.unavailable(
                load_sps.RETRY_EXHAUSTED
            )
        return load_sps.SPSRequestOutcome.success(
            {
                "placementScores": [
                    {
                        "region": regions[0],
                        "sku": instance_types[0],
                        "score": "High",
                        "availabilityZone": "1",
                    }
                ]
            }
        )

    monkeypatch.setattr(
        load_sps,
        "execute_spot_placement_score_api",
        complete_or_exhaust,
    )
    monkeypatch.setattr(load_sps, "save_tmp_files_to_s3", lambda: None)
    requests_df = pd.DataFrame(
        [
            {
                "Regions": ["successful-region"],
                "InstanceTypes": ["successful-instance"],
            },
            {
                "Regions": ["failed-region"],
                "InstanceTypes": ["failed-instance"],
            },
        ]
    )

    with pytest.raises(load_sps.SPSCollectionRequestError) as exc_info:
        load_sps.execute_spot_placement_score_task_by_parameter_pool_df(
            requests_df,
            [40],
        )

    error = exc_info.value
    assert error.completed_request_count == 1
    assert error.failed_request_count == 1
    assert error.total_request_count == 2
    assert error.completed_request_count + error.failed_request_count == 2

    completed = error.partial_sps_df.loc[
        error.partial_sps_df["RegionCodeSPS"] == "successful-region"
    ].iloc[0]
    failed = error.partial_sps_df.loc[
        error.partial_sps_df["RegionCodeSPS"] == "failed-region"
    ].iloc[0]
    assert completed["Score"] == 3
    assert pd.isna(failed["Score"])


def test_expired_collection_deadline_stops_request_before_azure_call(monkeypatch):
    load_sps, resources = _load_sps_module(monkeypatch)

    class ExpiredDeadline:
        @staticmethod
        def expired():
            return True

        @staticmethod
        def remaining_seconds():
            return 0

    resources.collection_deadline = ExpiredDeadline()
    azure_called = False

    def unexpected_call(*args, **kwargs):
        nonlocal azure_called
        azure_called = True

    monkeypatch.setattr(load_sps.requests, "post", unexpected_call)

    result = load_sps.execute_spot_placement_score_api(
        ["region"], ["instance"], desired_count=40, max_retries=50
    )

    assert result.status is load_sps.SPSRequestStatus.UNAVAILABLE
    assert result.reason == load_sps.DEADLINE_EXCEEDED
    assert not azure_called


def test_superseded_collection_stops_request_before_azure_call(monkeypatch):
    load_sps, resources = _load_sps_module(monkeypatch)

    class SupersededDeadline:
        @staticmethod
        def expired():
            return False

        @staticmethod
        def superseded():
            return True

        @staticmethod
        def remaining_seconds():
            return 600

    resources.collection_deadline = SupersededDeadline()
    azure_called = False

    def unexpected_call(*args, **kwargs):
        nonlocal azure_called
        azure_called = True

    monkeypatch.setattr(load_sps.requests, "post", unexpected_call)

    result = load_sps.execute_spot_placement_score_api(
        ["region"], ["instance"], desired_count=40, max_retries=50
    )

    assert result.status is load_sps.SPSRequestStatus.UNAVAILABLE
    assert result.reason == load_sps.LEASE_SUPERSEDED
    assert not azure_called


def test_azure_request_timeout_is_limited_by_collection_deadline(monkeypatch):
    load_sps, resources = _load_sps_module(monkeypatch)

    class NearDeadline:
        @staticmethod
        def expired():
            return False

        @staticmethod
        def remaining_seconds():
            return 7.5

    class SuccessfulResponse:
        headers = {}

        @staticmethod
        def raise_for_status():
            return None

        @staticmethod
        def json():
            return {"placementScores": []}

    resources.collection_deadline = NearDeadline()
    captured_timeout = None

    def successful_call(*args, **kwargs):
        nonlocal captured_timeout
        captured_timeout = kwargs["timeout"]
        return SuccessfulResponse()

    monkeypatch.setattr(load_sps.requests, "post", successful_call)

    result = load_sps.execute_spot_placement_score_api(
        ["region"], ["instance"], desired_count=40, max_retries=50
    )

    assert result.status is load_sps.SPSRequestStatus.SUCCESS
    assert result.response == {"placementScores": []}
    assert captured_timeout == 7.5


def test_expired_deadline_rejects_partial_fanout_result(monkeypatch):
    load_sps, resources = _load_sps_module(monkeypatch)
    resources.region_map_and_instance_map_tmp = {
        "region_map": {"region": "Region Display Name"},
        "instance_map": {
            "instance": {
                "InstanceTier": "Standard",
                "InstanceTypeOld": "D2s_v5",
            }
        },
    }
    resources.collection_deadline = types.SimpleNamespace(
        started_at=880,
        deadline_at=1_480,
        start=lambda: 1_480,
    )
    monkeypatch.setattr(
        load_sps,
        "execute_spot_placement_score_api",
        lambda *args, **kwargs: load_sps.SPSRequestOutcome.unavailable(
            load_sps.DEADLINE_EXCEEDED
        ),
    )
    monkeypatch.setattr(load_sps, "save_tmp_files_to_s3", lambda: None)
    requests_df = pd.DataFrame(
        [{"Regions": ["region"], "InstanceTypes": ["instance"]}]
    )

    with pytest.raises(load_sps.SPSCollectionDeadlineError) as exc_info:
        load_sps.execute_spot_placement_score_task_by_parameter_pool_df(
            requests_df, [40]
        )

    assert exc_info.value.desired_counts == [40]
    assert exc_info.value.started_at == 880
    assert exc_info.value.deadline_at == 1_480
    assert exc_info.value.failed_request_count == 1
    assert exc_info.value.total_request_count == 1
    partial_row = exc_info.value.partial_sps_df.iloc[0]
    assert partial_row["DesiredCount"] == 40
    assert partial_row["RegionCodeSPS"] == "region"
    assert partial_row["Region"] == "Region Display Name"
    assert partial_row["InstanceTypeSPS"] == "instance"
    assert partial_row["InstanceTier"] == "Standard"
    assert partial_row["InstanceType"] == "D2s_v5"
    for column in ("Score", "T3", "T2", "AvailabilityZone"):
        assert pd.isna(partial_row[column])


def test_superseded_lease_rejects_partial_fanout_result(monkeypatch):
    load_sps, resources = _load_sps_module(monkeypatch)
    finished_call_slots = []
    resources.collection_deadline = types.SimpleNamespace(
        started_at=880,
        deadline_at=1_480,
        start=lambda: 1_480,
        finish=lambda: finished_call_slots.append(True),
    )
    monkeypatch.setattr(
        load_sps,
        "execute_spot_placement_score_api",
        lambda *args, **kwargs: load_sps.SPSRequestOutcome.unavailable(
            load_sps.LEASE_SUPERSEDED
        ),
    )
    monkeypatch.setattr(load_sps, "save_tmp_files_to_s3", lambda: None)
    requests_df = pd.DataFrame(
        [{"Regions": ["region"], "InstanceTypes": ["instance"]}]
    )

    with pytest.raises(load_sps.SPSCollectionSupersededError) as exc_info:
        load_sps.execute_spot_placement_score_task_by_parameter_pool_df(
            requests_df, [40]
        )

    assert exc_info.value.desired_counts == [40]
    assert exc_info.value.started_at == 880
    assert finished_call_slots == [True]


def test_deadline_partial_result_keeps_completed_request_and_marks_failed_range(
    monkeypatch,
):
    load_sps, resources = _load_sps_module(monkeypatch)
    resources.region_map_and_instance_map_tmp = {
        "region_map": {
            "successful-region": "Successful Region",
            "failed-region": "Failed Region",
        },
        "instance_map": {
            "successful-instance": {
                "InstanceTier": "Standard",
                "InstanceTypeOld": "D2s_v5",
            },
            "failed-instance": {
                "InstanceTier": "Standard",
                "InstanceTypeOld": "D4s_v5",
            },
        },
    }
    resources.collection_deadline = types.SimpleNamespace(
        started_at=880,
        deadline_at=1_480,
        start=lambda: 1_480,
    )
    workers_ready = threading.Barrier(2)

    def complete_one_request(regions, instance_types, desired_count, **kwargs):
        workers_ready.wait()
        if regions == ["failed-region"]:
            return load_sps.SPSRequestOutcome.unavailable(
                load_sps.DEADLINE_EXCEEDED
            )
        return load_sps.SPSRequestOutcome.success(
            {
                "placementScores": [
                    {
                        "region": regions[0],
                        "sku": instance_types[0],
                        "score": "High",
                        "availabilityZone": "1",
                    }
                ]
            }
        )

    monkeypatch.setattr(
        load_sps,
        "execute_spot_placement_score_api",
        complete_one_request,
    )
    monkeypatch.setattr(load_sps, "save_tmp_files_to_s3", lambda: None)
    requests_df = pd.DataFrame(
        [
            {
                "Regions": ["successful-region"],
                "InstanceTypes": ["successful-instance"],
            },
            {
                "Regions": ["failed-region"],
                "InstanceTypes": ["failed-instance"],
            },
        ]
    )

    with pytest.raises(load_sps.SPSCollectionDeadlineError) as exc_info:
        load_sps.execute_spot_placement_score_task_by_parameter_pool_df(
            requests_df,
            [40],
        )

    error = exc_info.value
    assert error.completed_request_count == 1
    assert error.failed_request_count == 1
    assert error.total_request_count == 2

    completed = error.partial_sps_df.loc[
        error.partial_sps_df["RegionCodeSPS"] == "successful-region"
    ].iloc[0]
    failed = error.partial_sps_df.loc[
        error.partial_sps_df["RegionCodeSPS"] == "failed-region"
    ].iloc[0]
    assert completed["Score"] == 3
    assert completed["T2"] == 40
    assert completed["T3"] == 40
    assert completed["AvailabilityZone"] == "1"
    assert failed["DesiredCount"] == 40
    for column in ("Score", "T2", "T3", "AvailabilityZone"):
        assert pd.isna(failed[column])


def test_sps_fanout_starts_collection_deadline_before_workers(monkeypatch):
    load_sps, resources = _load_sps_module(monkeypatch)

    class LazyDeadline:
        started_at = None
        deadline_at = None

        def start(self):
            self.started_at = 1_000
            self.deadline_at = 1_600
            return self.deadline_at

    deadline = LazyDeadline()
    resources.collection_deadline = deadline

    def assert_started_before_request(*args, **kwargs):
        assert deadline.started_at == 1_000
        return load_sps.SPSRequestOutcome.success({"placementScores": []})

    monkeypatch.setattr(
        load_sps,
        "execute_spot_placement_score_api",
        assert_started_before_request,
    )
    monkeypatch.setattr(load_sps, "save_tmp_files_to_s3", lambda: None)
    requests_df = pd.DataFrame(
        [{"Regions": ["region"], "InstanceTypes": ["instance"]}]
    )

    load_sps.execute_spot_placement_score_task_by_parameter_pool_df(
        requests_df, [40]
    )
    assert deadline.deadline_at == 1_600


def test_sps_call_slot_is_held_until_inflight_requests_finish(monkeypatch):
    load_sps, resources = _load_sps_module(monkeypatch)
    call_started = threading.Event()
    allow_call_to_finish = threading.Event()
    call_slot_released = threading.Event()

    class RecordingDeadline:
        started_at = 1_000
        deadline_at = 1_600

        def start(self):
            return self.deadline_at

        def finish(self):
            call_slot_released.set()

    resources.collection_deadline = RecordingDeadline()

    def blocking_request(*args, **kwargs):
        call_started.set()
        assert allow_call_to_finish.wait(timeout=2)
        return load_sps.SPSRequestOutcome.success({"placementScores": []})

    monkeypatch.setattr(
        load_sps,
        "execute_spot_placement_score_api",
        blocking_request,
    )
    postprocess_started = threading.Event()

    def save_after_call_slot_release():
        assert call_slot_released.is_set()
        postprocess_started.set()

    monkeypatch.setattr(
        load_sps,
        "save_tmp_files_to_s3",
        save_after_call_slot_release,
    )
    requests_df = pd.DataFrame(
        [{"Regions": ["region"], "InstanceTypes": ["instance"]}]
    )
    errors = []

    def run_fanout():
        try:
            load_sps.execute_spot_placement_score_task_by_parameter_pool_df(
                requests_df,
                [40],
            )
        except Exception as error:
            errors.append(error)

    fanout_thread = threading.Thread(target=run_fanout)
    fanout_thread.start()

    assert call_started.wait(timeout=2)
    assert not call_slot_released.is_set()

    allow_call_to_finish.set()
    fanout_thread.join(timeout=2)

    assert not fanout_thread.is_alive()
    assert errors == []
    assert call_slot_released.is_set()
    assert postprocess_started.is_set()


def test_request_hash_is_stable_across_region_and_sku_order(monkeypatch):
    load_sps, _ = _load_sps_module(monkeypatch)
    first = {
        "availabilityZones": True,
        "desiredCount": 10,
        "desiredLocations": ["eastus", "westus"],
        "desiredSizes": [{"sku": "Standard_D4s_v5"}, {"sku": "Standard_D2s_v5"}],
    }
    reordered = {
        "availabilityZones": True,
        "desiredCount": 10,
        "desiredLocations": ["westus", "eastus"],
        "desiredSizes": [{"sku": "Standard_D2s_v5"}, {"sku": "Standard_D4s_v5"}],
    }

    assert load_sps.build_sps_request_hash(first) == load_sps.build_sps_request_hash(reordered)


def test_timeout_then_success_logs_same_hash_and_request_ids(monkeypatch, capsys):
    load_sps, resources = _load_sps_module(monkeypatch)
    resources.timeout_metrics = load_sps.TimeoutWindowMetrics()
    monkeypatch.setattr(load_sps.time, "sleep", lambda _: None)

    client_request_ids = iter(["client-timeout", "client-success"])
    monkeypatch.setattr(load_sps.uuid, "uuid4", lambda: next(client_request_ids))
    captured_headers = []

    class SuccessfulResponse:
        headers = {"x-ms-request-id": "azure-request-123"}

        @staticmethod
        def raise_for_status():
            return None

        @staticmethod
        def json():
            return {"placementScores": []}

    outcomes = iter([requests.exceptions.ReadTimeout(), SuccessfulResponse()])

    def timeout_then_success(*args, **kwargs):
        captured_headers.append(kwargs["headers"])
        outcome = next(outcomes)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(load_sps.requests, "post", timeout_then_success)

    result = load_sps.execute_spot_placement_score_api(
        ["westus", "eastus"], ["Standard_D2s_v5"], desired_count=10, max_retries=2
    )

    assert result.status is load_sps.SPSRequestStatus.SUCCESS
    assert result.response == {"placementScores": []}
    assert captured_headers[0]["x-ms-client-request-id"] == "client-timeout"
    assert captured_headers[1]["x-ms-client-request-id"] == "client-success"
    assert captured_headers[0]["x-ms-return-client-request-id"] == "true"

    output = capsys.readouterr().out
    timeout_hash = re.search(r"\[SPS_TIMEOUT\].*request_hash=([0-9a-f]{64})", output).group(1)
    success_hash = re.search(r"\[SPS_SUCCESS\].*request_hash=([0-9a-f]{64})", output).group(1)
    assert timeout_hash == success_hash
    assert "client_request_id=client-timeout" in output
    assert "client_request_id=client-success" in output
    assert "azure_request_id=azure-request-123" in output


def test_probe_selection_uses_short_request_timeout(monkeypatch):
    load_sps, _ = _load_sps_module(monkeypatch)
    load_sps.SL_Manager.get_next_available_location = lambda: (
        "subscription",
        "location",
        "probe",
    )
    captured_timeout = None

    class SuccessfulResponse:
        headers = {}

        @staticmethod
        def raise_for_status():
            return None

        @staticmethod
        def json():
            return {"placementScores": []}

    def successful_call(*args, **kwargs):
        nonlocal captured_timeout
        captured_timeout = kwargs["timeout"]
        return SuccessfulResponse()

    monkeypatch.setattr(load_sps.requests, "post", successful_call)

    result = load_sps.execute_spot_placement_score_api(
        ["region"], ["instance"], desired_count=40
    )

    assert result.status is load_sps.SPSRequestStatus.SUCCESS
    assert captured_timeout == 10


def test_location_health_recording_failure_does_not_discard_sps_success(
    monkeypatch,
    capsys,
):
    load_sps, _ = _load_sps_module(monkeypatch)

    class SuccessfulResponse:
        headers = {}

        @staticmethod
        def raise_for_status():
            return None

        @staticmethod
        def json():
            return {"placementScores": []}

    monkeypatch.setattr(load_sps.requests, "post", lambda *args, **kwargs: SuccessfulResponse())
    monkeypatch.setattr(
        load_sps.SL_Manager,
        "record_location_result",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("state error")),
    )

    result = load_sps.execute_spot_placement_score_api(
        ["region"], ["instance"], desired_count=40
    )

    assert result.status is load_sps.SPSRequestStatus.SUCCESS
    assert "action=record_failed" in capsys.readouterr().out


def test_location_health_initialization_failure_keeps_fanout_available(
    monkeypatch,
):
    load_sps, resources = _load_sps_module(monkeypatch)
    resources.region_map_and_instance_map_tmp = {
        "region_map": {},
        "instance_map": {},
    }
    monkeypatch.setattr(
        load_sps.SL_Manager,
        "prepare_location_routing",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("state error")),
    )
    monkeypatch.setattr(
        load_sps,
        "execute_spot_placement_score_api",
        lambda *args, **kwargs: load_sps.SPSRequestOutcome.success(
            {"placementScores": []}
        ),
    )
    monkeypatch.setattr(load_sps, "save_tmp_files_to_s3", lambda: None)
    requests_df = pd.DataFrame(
        [{"Regions": ["region"], "InstanceTypes": ["instance"]}]
    )

    result = load_sps.execute_spot_placement_score_task_by_parameter_pool_df(
        requests_df,
        [40],
    )

    assert result.empty
    assert resources.effective_excluded_locations == set()
