from __future__ import annotations

import argparse
import gzip
import io
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
import polars as pl
from botocore.exceptions import ClientError

LAMBDA_DIR = Path(__file__).resolve().parents[1]
SPOTLAKE_ROOT = Path(__file__).resolve().parents[5]
sys.path.insert(0, str(LAMBDA_DIR))
sys.path.insert(0, str(SPOTLAKE_ROOT))

from compare_data import compare


SNAPSHOT_COLUMNS = ["Time", "InstanceType", "Region", "OnDemand Price", "Spot Price", "Savings"]
HOT_COLUMNS = ["InstanceType", "Region", "Time", "OndemandPrice", "SpotPrice", "Savings", "Ceased"]


def parse_timestamp(value: str) -> datetime:
    if value.endswith("Z"):
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    parsed = datetime.fromisoformat(value)
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def load_json_df(s3_client, bucket: str, key: str) -> pl.DataFrame:
    response = s3_client.get_object(Bucket=bucket, Key=key)
    payload = json.loads(response["Body"].read())
    return pl.from_dicts(payload) if payload else pl.DataFrame()


def load_raw_df(s3_client, bucket: str, key: str) -> pl.DataFrame:
    response = s3_client.get_object(Bucket=bucket, Key=key)
    payload = gzip.decompress(response["Body"].read())
    return pl.read_csv(io.BytesIO(payload))


def load_hot_df(s3_client, bucket: str, key: str) -> pl.DataFrame:
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return pl.read_parquet(io.BytesIO(response["Body"].read()))


def object_exists(s3_client, bucket: str, key: str) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def normalize_snapshot_df(df: pl.DataFrame) -> pl.DataFrame:
    normalized = df
    if "time" in normalized.columns and "Time" not in normalized.columns:
        normalized = normalized.rename({"time": "Time"})
    if "id" in normalized.columns:
        normalized = normalized.drop("id")

    expressions = []
    for column in SNAPSHOT_COLUMNS:
        if column in normalized.columns:
            expressions.append(pl.col(column))
        else:
            expressions.append(pl.lit(None).alias(column))
    normalized = normalized.with_columns(expressions).select(SNAPSHOT_COLUMNS)
    if "Time" in normalized.columns:
        normalized = normalized.with_columns(
            pl.col("Time").cast(pl.Utf8).alias("Time")
        )
    return normalized.sort(["InstanceType", "Region"])


def canonicalize_snapshot_df(df: pl.DataFrame) -> pl.DataFrame:
    normalized = normalize_snapshot_df(df)
    if "Savings" in normalized.columns:
        normalized = normalized.with_columns(
            pl.col("Savings").round(0).cast(pl.Float64, strict=False).alias("Savings")
        )
    return normalized


def normalize_hot_df(df: pl.DataFrame) -> pl.DataFrame:
    normalized = df
    expressions = []
    for column in HOT_COLUMNS:
        if column in normalized.columns:
            expressions.append(pl.col(column))
        elif column == "Ceased":
            expressions.append(pl.lit(False).alias(column))
        else:
            expressions.append(pl.lit(None).alias(column))
    normalized = normalized.with_columns(expressions).select(HOT_COLUMNS)
    time_expr = pl.col("Time").cast(pl.Utf8)
    if normalized.schema.get("Time") != pl.Utf8:
        time_expr = pl.col("Time").dt.convert_time_zone("UTC").dt.strftime("%Y-%m-%d %H:%M:%S")
    normalized = normalized.with_columns(
        [
            time_expr.alias("Time"),
            pl.col("Ceased").fill_null(False).cast(pl.Boolean).alias("Ceased"),
        ]
    )
    return normalized.sort(["InstanceType", "Region", "Time", "Ceased"])


def assert_equal(name: str, left: pl.DataFrame, right: pl.DataFrame) -> None:
    if left.equals(right):
        print(f"[OK] {name}: {left.height} rows matched")
        return

    print(f"[FAIL] {name}")
    left_only = left.join(right, on=left.columns, how="anti")
    right_only = right.join(left, on=left.columns, how="anti")
    print(f"left_only_rows={left_only.height}")
    if left_only.height:
        print(left_only.head(5))
    print(f"right_only_rows={right_only.height}")
    if right_only.height:
        print(right_only.head(5))
    raise SystemExit(1)


def build_raw_key(raw_prefix: str, timestamp: datetime) -> str:
    return (
        f"{raw_prefix}/"
        f"{timestamp.strftime('%Y/%m/%d')}/"
        f"{timestamp.strftime('%H-%M-%S')}.csv.gz"
    )


def build_hot_key(prefix: str, timestamp: datetime) -> str:
    slot_minute = (timestamp.minute // 10) * 10
    return (
        f"{prefix}/"
        f"{timestamp.year}/{timestamp.month:02d}/{timestamp.day:02d}/"
        f"{timestamp.hour:02d}-{slot_minute:02d}.parquet"
    )


def build_manifest_key(prefix: str, timestamp: datetime) -> str:
    return f"{prefix}/{timestamp.year}/{timestamp.month:02d}/manifest.json"


def prepare_titans_upload(changed_df: pl.DataFrame, removed_df: pl.DataFrame) -> pl.DataFrame:
    rename_map = {"OnDemand Price": "OndemandPrice", "Spot Price": "SpotPrice"}
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
    if not frames:
        return pl.DataFrame()
    combined = pl.concat(frames, how="diagonal_relaxed")
    return (
        combined
        .sort(["InstanceType", "Region", "Time", "Ceased"])
        .unique(subset=["InstanceType", "Region", "Time"], keep="first")
        .sort(["InstanceType", "Region", "Time"])
    )


def expected_hot_df(prod_latest_df: pl.DataFrame, current_snapshot_df: pl.DataFrame, timestamp: datetime) -> pl.DataFrame:
    changed_df, removed_df = compare(
        prod_latest_df,
        current_snapshot_df,
        ["InstanceType", "Region"],
        ["OnDemand Price", "Spot Price"],
    )
    if not removed_df.is_empty() and "Time" in removed_df.columns:
        removed_df = removed_df.with_columns(pl.lit(timestamp.strftime("%Y-%m-%d %H:%M:%S")).alias("Time"))
    combined_df = prepare_titans_upload(changed_df, removed_df)
    return normalize_hot_df(combined_df)


def verify_manifest(s3_client, bucket: str, key: str, hot_key: str) -> None:
    response = s3_client.get_object(Bucket=bucket, Key=key)
    manifest = json.loads(response["Body"].read())
    level_zero = manifest.get("levels", {}).get("0", [])
    if any(item.get("file") == hot_key for item in level_zero):
        print(f"[OK] manifest includes hot key: {hot_key}")
        return
    if manifest.get("last_processed_time"):
        print(f"[OK] manifest updated last_processed_time={manifest['last_processed_time']}")
        return
    print(f"[FAIL] manifest did not record hot key: {hot_key}")
    raise SystemExit(1)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--timestamp", required=True, help="UTC timestamp, e.g. 2026-03-16T10:20:00Z")
    parser.add_argument("--prod-bucket", default="spotlake")
    parser.add_argument("--test-bucket", default="spotlake-test")
    parser.add_argument("--titans-bucket", default="titans-spotlake-data")
    parser.add_argument("--latest-read-path", default="latest_data/latest_gcp.json")
    parser.add_argument("--latest-write-path", default="latest_data/latest_gcp.json")
    parser.add_argument("--raw-prefix", default="rawdata/gcp")
    parser.add_argument("--test-hot-prefix", default="test/parquet_cp_hot/gcp")
    parser.add_argument("--test-warm-prefix", default="test/parquet_warm/gcp/m8")
    args = parser.parse_args()

    timestamp = parse_timestamp(args.timestamp)
    s3_client = boto3.client("s3")

    prod_latest = normalize_snapshot_df(load_json_df(s3_client, args.prod_bucket, args.latest_read_path))
    test_latest = load_json_df(s3_client, args.test_bucket, args.latest_write_path)
    test_latest_snapshot = normalize_snapshot_df(test_latest)
    raw_key = build_raw_key(args.raw_prefix, timestamp)
    test_raw = load_raw_df(s3_client, args.test_bucket, raw_key)

    assert_equal(
        "test latest vs test raw snapshot",
        canonicalize_snapshot_df(test_latest),
        canonicalize_snapshot_df(test_raw),
    )

    expected_hot = expected_hot_df(prod_latest, test_latest_snapshot, timestamp)
    hot_key = build_hot_key(args.test_hot_prefix, timestamp)
    manifest_key = build_manifest_key(args.test_warm_prefix, timestamp)

    if expected_hot.is_empty():
        if object_exists(s3_client, args.titans_bucket, hot_key):
            actual_hot = normalize_hot_df(load_hot_df(s3_client, args.titans_bucket, hot_key))
            print("[FAIL] expected no hot delta, but hot parquet exists")
            print(actual_hot.head(5))
            raise SystemExit(1)
        print("[OK] expected hot delta empty and no hot parquet uploaded")
        if object_exists(s3_client, args.titans_bucket, manifest_key):
            print(f"[OK] warm manifest exists: {manifest_key}")
        else:
            print(f"[FAIL] warm manifest missing: {manifest_key}")
            raise SystemExit(1)
    else:
        actual_hot = normalize_hot_df(load_hot_df(s3_client, args.titans_bucket, hot_key))
        assert_equal("expected hot delta vs uploaded hot parquet", expected_hot, actual_hot)
        verify_manifest(s3_client, args.titans_bucket, manifest_key, hot_key)

    print("[OK] shadow-run verification completed")


if __name__ == "__main__":
    main()
