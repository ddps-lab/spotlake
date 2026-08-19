import sys
import unittest
import io
import json
from datetime import datetime, timezone
from pathlib import Path

from botocore.exceptions import ClientError

UTILITY_PARENT = Path(__file__).resolve().parents[2]
if str(UTILITY_PARENT) not in sys.path:
    sys.path.insert(0, str(UTILITY_PARENT))

from monthly_cold_freezer.freezer_worker import (
    BUCKET,
    FreezeError,
    check_completeness,
    enumerate_input_files,
    resolve_control_flags,
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

    def list_objects_v2(self, Bucket: str, Prefix: str, ContinuationToken=None):
        keys = sorted(
            key for (bucket, key) in self.objects
            if bucket == Bucket and key.startswith(Prefix)
        )
        return {
            "Contents": [{"Key": key} for key in keys],
            "IsTruncated": False,
        }


class FreezerWorkerTests(unittest.TestCase):
    def test_force_alias_sets_both_controls(self):
        controls = resolve_control_flags(
            force=True,
            ignore_completeness=False,
            overwrite_existing=False,
        )
        self.assertTrue(controls.ignore_completeness)
        self.assertTrue(controls.overwrite_existing)

    def test_explicit_controls_stay_independent(self):
        controls = resolve_control_flags(
            force=False,
            ignore_completeness=True,
            overwrite_existing=False,
        )
        self.assertTrue(controls.ignore_completeness)
        self.assertFalse(controls.overwrite_existing)

    def test_check_completeness_allows_incomplete_month_when_ignored(self):
        manifest = {
            "last_processed_time": datetime(2026, 2, 28, 23, 0, tzinfo=timezone.utc).isoformat()
        }
        check_completeness(
            manifest,
            2026,
            2,
            ignore_completeness=True,
        )

    def test_check_completeness_rejects_incomplete_month_without_override(self):
        manifest = {
            "last_processed_time": datetime(2026, 2, 28, 23, 0, tzinfo=timezone.utc).isoformat()
        }
        with self.assertRaises(FreezeError):
            check_completeness(
                manifest,
                2026,
                2,
                ignore_completeness=False,
            )

    def test_enumerate_input_files_appends_only_tail_hot_after_manifest(self):
        s3 = FakeS3()
        manifest = {
            "last_processed_time": "2026-03-31T13:00:00+00:00",
            "levels": {
                "0": [
                    {"file": "parquet_cp_hot/gcp/2026/03/31/13-00.parquet", "hot_range": [18, 18]}
                ],
                "1": [
                    {"file": "L1_0000_00000-00015.parquet", "hot_range": [0, 15]}
                ],
            },
        }
        s3.put_bytes(BUCKET, "parquet_cp_hot/gcp/2026/03/31/13-00.parquet")
        s3.put_bytes(BUCKET, "parquet_cp_hot/gcp/2026/03/31/14-00.parquet")
        s3.put_bytes(BUCKET, "parquet_cp_hot/gcp/2026/03/31/15-00.parquet")
        s3.put_bytes(BUCKET, "parquet_cp_hot/gcp/2026/03/31/12-00.parquet")

        uris = enumerate_input_files(
            s3,
            manifest,
            "parquet_warm/gcp/m8/2026/03",
            "gcp",
            2026,
            3,
            "production",
        )

        self.assertEqual(
            uris,
            [
                "s3://titans-spotlake-data/parquet_cp_hot/gcp/2026/03/31/13-00.parquet",
                "s3://titans-spotlake-data/parquet_warm/gcp/m8/2026/03/L1_0000_00000-00015.parquet",
                "s3://titans-spotlake-data/parquet_cp_hot/gcp/2026/03/31/14-00.parquet",
                "s3://titans-spotlake-data/parquet_cp_hot/gcp/2026/03/31/15-00.parquet",
            ],
        )

    def test_enumerate_input_files_supports_hot_only_snapshot_when_manifest_missing(self):
        s3 = FakeS3()
        s3.put_bytes(BUCKET, "parquet_cp_hot/aws/2026/03/30/23-40.parquet")
        s3.put_bytes(BUCKET, "parquet_cp_hot/aws/2026/03/31/23-50.parquet")

        uris = enumerate_input_files(
            s3,
            None,
            "parquet_warm/aws/m8/2026/03",
            "aws",
            2026,
            3,
            "production",
        )

        self.assertEqual(
            uris,
            [
                "s3://titans-spotlake-data/parquet_cp_hot/aws/2026/03/30/23-40.parquet",
                "s3://titans-spotlake-data/parquet_cp_hot/aws/2026/03/31/23-50.parquet",
            ],
        )


if __name__ == "__main__":
    unittest.main()
