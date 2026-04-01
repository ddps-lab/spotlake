import io
import json
import sys
import unittest
from unittest import mock
from pathlib import Path

from botocore.exceptions import ClientError

UTILITY_PARENT = Path(__file__).resolve().parents[2]
if str(UTILITY_PARENT) not in sys.path:
    sys.path.insert(0, str(UTILITY_PARENT))

from monthly_cold_freezer import coordinator
from monthly_cold_freezer.signals import ProviderSignal


def _missing_key(operation: str, code: str = "NoSuchKey"):
    return ClientError({"Error": {"Code": code, "Message": "missing"}}, operation)


class FakeS3:
    def __init__(self):
        self.objects = {}

    def put_json(self, bucket: str, key: str, payload: dict):
        self.objects[(bucket, key)] = json.dumps(payload).encode("utf-8")

    def put_bytes(self, bucket: str, key: str, payload: bytes = b""):
        self.objects[(bucket, key)] = payload

    def get_object(self, Bucket: str, Key: str):
        try:
            payload = self.objects[(Bucket, Key)]
        except KeyError as exc:
            raise _missing_key("GetObject") from exc
        return {"Body": io.BytesIO(payload)}

    def put_object(self, Bucket: str, Key: str, Body, ContentType=None):
        if isinstance(Body, str):
            Body = Body.encode("utf-8")
        self.objects[(Bucket, Key)] = Body
        return {}

    def head_object(self, Bucket: str, Key: str):
        if (Bucket, Key) not in self.objects:
            raise _missing_key("HeadObject", code="404")
        return {}


class FakeBatch:
    def __init__(self, statuses=None):
        self.statuses = dict(statuses or {})
        self.submit_calls = []
        self.counter = 0

    def describe_jobs(self, jobs):
        return {
            "jobs": [
                {"jobId": job_id, "status": self.statuses[job_id]}
                for job_id in jobs
                if job_id in self.statuses
            ]
        }

    def submit_job(self, jobName, jobQueue, jobDefinition, containerOverrides=None):
        self.counter += 1
        job_id = f"job-{self.counter}"
        self.statuses[job_id] = "SUBMITTED"
        self.submit_calls.append(
            {
                "jobName": jobName,
                "jobQueue": jobQueue,
                "jobDefinition": jobDefinition,
                "containerOverrides": containerOverrides,
            }
        )
        return {"jobId": job_id, "jobName": jobName}


class FakeSession:
    def __init__(self, s3, batch):
        self._s3 = s3
        self._batch = batch

    def client(self, service_name: str):
        if service_name == "s3":
            return self._s3
        if service_name == "batch":
            return self._batch
        raise ValueError(service_name)


def ready_signal(provider: str) -> ProviderSignal:
    return ProviderSignal(
        provider=provider,
        ready=True,
        reason="ready",
        threshold="2026-02-28T23:50:00+00:00",
    )


class CoordinatorTests(unittest.TestCase):
    def test_decide_provider_action_covers_core_states(self):
        self.assertEqual(
            coordinator.decide_provider_action(
                output_complete=True,
                stored_state=None,
                observed_job_state=None,
            ),
            "complete",
        )
        self.assertEqual(
            coordinator.decide_provider_action(
                output_complete=False,
                stored_state="SUBMITTED",
                observed_job_state="RUNNING",
            ),
            "skip_running",
        )
        self.assertEqual(
            coordinator.decide_provider_action(
                output_complete=False,
                stored_state="FAILED",
                observed_job_state=None,
            ),
            "submit_retry",
        )
        self.assertEqual(
            coordinator.decide_provider_action(
                output_complete=False,
                stored_state="SUCCEEDED",
                observed_job_state=None,
            ),
            "anomaly",
        )

    def test_run_coordinator_submits_ready_providers_without_global_barrier(self):
        s3 = FakeS3()
        batch = FakeBatch()
        session = FakeSession(s3, batch)
        signals = {
            "aws": ready_signal("aws"),
            "azure": ready_signal("azure"),
            "gcp": ProviderSignal(
                provider="gcp",
                ready=False,
                reason="missing raw cutoff",
                threshold="2026-02-28T23:00:00+00:00",
                raw_cutoff_present=False,
            ),
        }

        with mock.patch.object(coordinator.boto3, "Session", return_value=session), \
             mock.patch.object(coordinator, "evaluate_all_signals", return_value=signals):
            summary = coordinator.run_coordinator(month="2026-02")

        self.assertFalse(summary["barrier_ready"])
        self.assertEqual(summary["waiting_on"], ["gcp"])
        self.assertCountEqual(summary["submitted"], ["aws", "azure"])
        self.assertEqual(summary["noop"], ["gcp"])
        self.assertEqual(len(batch.submit_calls), 2)

    def test_run_coordinator_marks_existing_outputs_complete(self):
        s3 = FakeS3()
        batch = FakeBatch()
        session = FakeSession(s3, batch)
        for provider in ("aws", "azure", "gcp"):
            s3.put_bytes("titans-spotlake-data", f"{provider}/2026-02.parquet")
            next_ap = "2026-03_AP.parquet"
            s3.put_bytes("titans-spotlake-data", f"{provider}/{next_ap}")

        signals = {provider: ready_signal(provider) for provider in ("aws", "azure", "gcp")}

        with mock.patch.object(coordinator.boto3, "Session", return_value=session), \
             mock.patch.object(coordinator, "evaluate_all_signals", return_value=signals):
            summary = coordinator.run_coordinator(month="2026-02")

        self.assertTrue(summary["barrier_ready"])
        self.assertCountEqual(summary["completed"], ["aws", "azure", "gcp"])
        self.assertEqual(batch.submit_calls, [])

    def test_run_coordinator_handles_running_failed_and_succeeded_without_outputs(self):
        s3 = FakeS3()
        batch = FakeBatch(
            {
                "job-running": "RUNNING",
                "job-succeeded": "SUCCEEDED",
                "job-failed": "FAILED",
            }
        )
        session = FakeSession(s3, batch)

        target_key = "ops/monthly_freeze/2026-02"
        s3.put_json(
            "titans-spotlake-data",
            f"{target_key}/aws.json",
            {"provider": "aws", "month": "2026-02", "state": "SUBMITTED", "job_id": "job-running"},
        )
        s3.put_json(
            "titans-spotlake-data",
            f"{target_key}/azure.json",
            {"provider": "azure", "month": "2026-02", "state": "SUBMITTED", "job_id": "job-succeeded"},
        )
        s3.put_json(
            "titans-spotlake-data",
            f"{target_key}/gcp.json",
            {"provider": "gcp", "month": "2026-02", "state": "FAILED", "job_id": "job-failed"},
        )

        signals = {provider: ready_signal(provider) for provider in ("aws", "azure", "gcp")}

        with mock.patch.object(coordinator.boto3, "Session", return_value=session), \
             mock.patch.object(coordinator, "evaluate_all_signals", return_value=signals), \
             mock.patch.object(coordinator, "_send_anomaly_alert"):
            summary = coordinator.run_coordinator(month="2026-02")

        self.assertEqual(summary["running"], ["aws"])
        self.assertEqual(summary["anomalies"], ["azure"])
        self.assertEqual(summary["submitted"], ["gcp"])
        self.assertEqual(len(batch.submit_calls), 1)

        gcp_status = json.loads(
            s3.objects[("titans-spotlake-data", f"{target_key}/gcp.json")].decode("utf-8")
        )
        self.assertEqual(gcp_status["state"], "SUBMITTED")
        self.assertNotEqual(gcp_status["job_id"], "job-failed")


if __name__ == "__main__":
    unittest.main()
