import importlib.util
import sys
import types
from pathlib import Path

import pandas as pd


BATCH_ROOT = Path(__file__).parents[1]


def _load_collect_sps_module(monkeypatch):
    monkeypatch.syspath_prepend(str(BATCH_ROOT))
    monkeypatch.syspath_prepend(str(BATCH_ROOT / "sps"))

    class DeadlineError(RuntimeError):
        pass

    class SupersededError(RuntimeError):
        pass

    class RequestError(RuntimeError):
        pass

    load_sps = types.ModuleType("load_sps")
    load_sps.SPSCollectionDeadlineError = DeadlineError
    load_sps.SPSCollectionSupersededError = SupersededError
    load_sps.SPSCollectionRequestError = RequestError
    load_sps.SS_Resources = types.SimpleNamespace(
        collection_deadline=None,
        succeed_to_get_sps_count=0,
    )

    notifications = types.ModuleType("sps_notifications")
    notifications.format_deadline_exceeded_message = lambda **kwargs: "deadline"
    notifications.format_failure_message = lambda **kwargs: "failure"
    notifications.format_request_failure_message = lambda **kwargs: "request_failure"
    notifications.format_superseded_message = lambda **kwargs: "superseded"

    class FakeS3:
        @staticmethod
        def read_file(*args, **kwargs):
            return None

        @staticmethod
        def upload_file(*args, **kwargs):
            return None

    class FakeLogger:
        messages = []

        @classmethod
        def info(cls, message):
            cls.messages.append(message)

        @staticmethod
        def warning(*args, **kwargs):
            return None

        @staticmethod
        def error(*args, **kwargs):
            return None

    common = types.ModuleType("utils.common")
    common.S3 = FakeS3()
    common.Logger = FakeLogger

    constants = types.ModuleType("utils.constants")
    constants.AZURE_CONST = types.SimpleNamespace(S3_RAW_DATA_PATH="rawdata/azure")

    slack = types.ModuleType("utils.slack_msg_sender")
    slack.send_slack_message = lambda message: None

    from sps.sps_resilience import CollectionDeadline

    resilience = types.ModuleType("sps_resilience")
    resilience.CollectionDeadline = CollectionDeadline
    resilience.DynamoDBLease = object
    resilience.SPSCallCoordinator = object

    utils = types.ModuleType("utils")
    monkeypatch.setitem(sys.modules, "load_sps", load_sps)
    monkeypatch.setitem(sys.modules, "sps_notifications", notifications)
    monkeypatch.setitem(sys.modules, "sps_resilience", resilience)
    monkeypatch.setitem(sys.modules, "utils", utils)
    monkeypatch.setitem(sys.modules, "utils.common", common)
    monkeypatch.setitem(sys.modules, "utils.constants", constants)
    monkeypatch.setitem(sys.modules, "utils.slack_msg_sender", slack)

    module_path = BATCH_ROOT / "sps" / "collect_sps.py"
    spec = importlib.util.spec_from_file_location(
        "azure_batch_collect_sps_lifecycle_test",
        module_path,
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, load_sps, FakeLogger


def test_completed_sps_snapshot_survives_priority_handoff(monkeypatch):
    collect_sps, load_sps, logger = _load_collect_sps_module(monkeypatch)
    saved_snapshots = []
    metadata_writes = []
    coordinators = []

    class FakeLease:
        def __init__(self, *, lease_id, **kwargs):
            self.lease_id = lease_id
            self.owner_id = "job-old"

    class FakeCoordinator:
        def __init__(self, *, priority_lease, active_call_lease, **kwargs):
            self.priority_lease = priority_lease
            self.active_call_lease = active_call_lease
            self.calls_finished = False
            self.closed = False
            coordinators.append(self)

        @staticmethod
        def claim_priority():
            return True

        @staticmethod
        def begin_calls():
            return True

        def finish_calls(self):
            self.calls_finished = True

        @staticmethod
        def cancellation_reason():
            return None

        @staticmethod
        def owns_priority():
            return False

        def close(self):
            self.closed = True

    def collect_completed_sps(*, desired_counts):
        deadline = load_sps.SS_Resources.collection_deadline
        deadline.start()
        deadline.finish()
        return pd.DataFrame([{"Score": 3}])

    monkeypatch.setattr(collect_sps, "DynamoDBLease", FakeLease)
    monkeypatch.setattr(collect_sps, "SPSCallCoordinator", FakeCoordinator)
    monkeypatch.setattr(
        collect_sps,
        "read_metadata",
        lambda: {"desired_count_index": 0, "workload_date": "2026-08-05"},
    )
    monkeypatch.setattr(
        load_sps,
        "collect_spot_placement_score",
        collect_completed_sps,
        raising=False,
    )
    monkeypatch.setattr(
        collect_sps,
        "save_sps_data",
        lambda frame, timestamp, desired_count: saved_snapshots.append(
            (len(frame), desired_count)
        ),
    )
    monkeypatch.setattr(
        collect_sps,
        "write_metadata",
        metadata_writes.append,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["collect_sps.py", "--timestamp", "2026-08-05T00:20:00Z"],
    )

    collect_sps.main()

    assert saved_snapshots == [(1, 1)]
    assert metadata_writes == []
    assert coordinators[0].calls_finished
    assert coordinators[0].closed
    assert any("snapshot saved" in message for message in logger.messages)


def test_sps_priority_is_claimed_only_when_request_fanout_is_ready(monkeypatch):
    collect_sps, load_sps, _ = _load_collect_sps_module(monkeypatch)
    events = []

    class FakeLease:
        def __init__(self, *, lease_id, **kwargs):
            self.lease_id = lease_id
            self.owner_id = "job-new"

    class FakeCoordinator:
        def __init__(self, *, priority_lease, active_call_lease, **kwargs):
            self.priority_lease = priority_lease
            self.active_call_lease = active_call_lease

        @staticmethod
        def begin_calls():
            events.append("begin_calls")
            return True

        @staticmethod
        def finish_calls():
            return None

        @staticmethod
        def cancellation_reason():
            return None

        @staticmethod
        def owns_priority():
            return True

        @staticmethod
        def close():
            return None

    def read_metadata_after_preparation_starts():
        events.append("metadata_ready")
        return {"desired_count_index": 0, "workload_date": "2026-08-05"}

    def collect_when_request_pool_is_ready(*, desired_counts):
        events.append("request_pool_ready")
        deadline = load_sps.SS_Resources.collection_deadline
        deadline.start()
        deadline.finish()
        return pd.DataFrame([{"Score": 3}])

    monkeypatch.setattr(collect_sps, "DynamoDBLease", FakeLease)
    monkeypatch.setattr(collect_sps, "SPSCallCoordinator", FakeCoordinator)
    monkeypatch.setattr(collect_sps, "read_metadata", read_metadata_after_preparation_starts)
    monkeypatch.setattr(
        load_sps,
        "collect_spot_placement_score",
        collect_when_request_pool_is_ready,
        raising=False,
    )
    monkeypatch.setattr(collect_sps, "save_sps_data", lambda *args, **kwargs: None)
    monkeypatch.setattr(collect_sps, "write_metadata", lambda metadata: None)
    monkeypatch.setattr(
        sys,
        "argv",
        ["collect_sps.py", "--timestamp", "2026-08-05T00:20:00Z"],
    )

    collect_sps.main()

    assert events == [
        "metadata_ready",
        "request_pool_ready",
        "begin_calls",
    ]
