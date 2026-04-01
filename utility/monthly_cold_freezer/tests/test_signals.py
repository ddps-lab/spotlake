import io
import json
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

from botocore.exceptions import ClientError

UTILITY_PARENT = Path(__file__).resolve().parents[2]
if str(UTILITY_PARENT) not in sys.path:
    sys.path.insert(0, str(UTILITY_PARENT))

from monthly_cold_freezer.signals import (
    HOT_PREFIXES,
    ProviderSignal,
    RAW_BUCKET,
    TITANS_BUCKET,
    TargetMonth,
    evaluate_provider_signal,
    hot_prefix,
    manifest_key,
    previous_month_target,
    warm_progress_key,
)


def _missing_key(operation: str):
    return ClientError({"Error": {"Code": "NoSuchKey", "Message": "missing"}}, operation)


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

    def list_objects_v2(self, Bucket: str, Prefix: str, ContinuationToken=None):
        keys = sorted(
            key for (bucket, key) in self.objects
            if bucket == Bucket and key.startswith(Prefix)
        )
        return {
            "Contents": [{"Key": key} for key in keys],
            "IsTruncated": False,
        }


class SignalTests(unittest.TestCase):
    def test_previous_month_target_uses_previous_utc_month(self):
        target = previous_month_target(datetime(2026, 3, 31, 12, 0, tzinfo=timezone.utc))
        self.assertEqual(target.year, 2026)
        self.assertEqual(target.month, 2)

    def test_aws_signal_reads_manifest_threshold(self):
        target = TargetMonth(year=2026, month=2)
        s3 = FakeS3()
        s3.put_json(
            TITANS_BUCKET,
            manifest_key("aws", target, "production"),
            {"last_processed_time": "2026-02-28T23:50:00+00:00"},
        )

        signal = evaluate_provider_signal(s3, target, "aws")

        self.assertIsInstance(signal, ProviderSignal)
        self.assertTrue(signal.ready)
        self.assertEqual(signal.last_processed_time, "2026-02-28T23:50:00+00:00")
        self.assertEqual(signal.ready_via, "manifest")

    def test_aws_signal_uses_manifest_plus_hot_tail_when_manifest_is_stale(self):
        target = TargetMonth(year=2026, month=2)
        s3 = FakeS3()
        s3.put_json(
            TITANS_BUCKET,
            manifest_key("aws", target, "production"),
            {"last_processed_time": "2026-02-28T23:40:00+00:00"},
        )

        signal = evaluate_provider_signal(s3, target, "aws")

        self.assertTrue(signal.ready)
        self.assertEqual(signal.ready_via, "manifest_plus_hot_tail")
        self.assertIsNotNone(signal.warning)

    def test_gcp_signal_uses_manifest_plus_hot_tail_when_manifest_is_stale(self):
        target = TargetMonth(year=2026, month=2)
        s3 = FakeS3()
        s3.put_json(
            TITANS_BUCKET,
            manifest_key("gcp", target, "production"),
            {"last_processed_time": "2026-02-28T22:00:00+00:00"},
        )

        ready_signal = evaluate_provider_signal(s3, target, "gcp")
        self.assertTrue(ready_signal.ready)
        self.assertEqual(ready_signal.ready_via, "manifest_plus_hot_tail")
        self.assertIsNotNone(ready_signal.warning)

    def test_provider_signal_allows_hot_only_snapshot_without_manifest(self):
        target = TargetMonth(year=2026, month=2)
        s3 = FakeS3()
        s3.put_bytes(
            TITANS_BUCKET,
            f"{hot_prefix('aws', target, 'production')}28/23-50.parquet",
        )

        signal = evaluate_provider_signal(s3, target, "aws")
        self.assertTrue(signal.ready)
        self.assertEqual(signal.ready_via, "hot_only")

    def test_gcp_signal_reports_missing_inputs_when_no_manifest_and_no_hot(self):
        target = TargetMonth(year=2026, month=2)
        s3 = FakeS3()
        s3.put_json(
            TITANS_BUCKET,
            warm_progress_key("gcp", target, "production"),
            {"last_processed_raw_time": "2026-02-28T23:00:00+00:00"},
        )
        s3.put_bytes(RAW_BUCKET, "rawdata/gcp/2026/02/28/23-00-00.csv.gz")

        signal = evaluate_provider_signal(s3, target, "gcp")

        self.assertFalse(signal.ready)
        self.assertTrue(signal.raw_cutoff_present)


if __name__ == "__main__":
    unittest.main()
