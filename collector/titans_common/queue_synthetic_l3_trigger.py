#!/usr/bin/env python3
"""Seed a synthetic pre-L3 warm state and queue the next compaction request."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import io
import json
import os
from pathlib import Path
import sys
from typing import Any

import boto3
from botocore.exceptions import ClientError
import pandas as pd
import polars as pl

COLLECTOR_ROOT = Path(__file__).resolve().parents[1]
if str(COLLECTOR_ROOT) not in sys.path:
    sys.path.insert(0, str(COLLECTOR_ROOT))

from titans_common.config import get_config
from titans_common.upload_titans import upload_hot_tier

DEFAULT_M = 8
L3_TRIGGER_LAST_HOT_IDX = DEFAULT_M ** 3 - 2  # 510, so the next hot file is idx=511


def _parse_timestamp(raw: str) -> datetime:
    value = raw.strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    ts = datetime.fromisoformat(value)
    if ts.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    ts_utc = ts.astimezone(timezone.utc)
    if ts_utc.second != 0 or ts_utc.microsecond != 0:
        raise ValueError("timestamp must align to an exact minute")
    if ts_utc.minute % 10 != 0:
        raise ValueError("timestamp must align to a 10-minute slot")
    return ts_utc


def _make_row(config: Any, *, idx: int, ts: datetime, label: str) -> dict[str, Any]:
    row: dict[str, Any] = {
        config.time_column: ts.strftime("%Y-%m-%d %H:%M:%S"),
        "Ceased": False,
    }

    for pk_col in config.pk_columns:
        if pk_col == "InstanceType":
            row[pk_col] = f"synthetic.{label}"
        elif pk_col == "Region":
            row[pk_col] = "us-west-2"
        elif pk_col == "AZ":
            row[pk_col] = "us-west-2a"
        elif pk_col == "InstanceTier":
            row[pk_col] = "GeneralPurpose"
        else:
            row[pk_col] = f"{pk_col.lower()}-{label}"

    default_values = {
        "SPS": idx % 100,
        "Score": idx % 4,
        "T3": idx % 8,
        "T2": idx % 8,
        "IF": 1.0 + (idx % 5) * 0.1,
        "DesiredCount": 1.0 + (idx % 3),
        "OndemandPrice": 10.0 + (idx % 11),
        "SpotPrice": 5.0 + (idx % 7),
        "Savings": 50 if idx % 2 == 0 else 40,
    }
    for col in config.value_columns:
        row[col] = default_values.get(col, idx)

    return row


def _make_df(config: Any, *, idx: int, ts: datetime, label: str) -> pl.DataFrame:
    df = pl.from_dicts([_make_row(config, idx=idx, ts=ts, label=label)])
    for col, dtype in config.schema_dtypes.items():
        if col not in df.columns:
            if col == "Ceased":
                df = df.with_columns(pl.lit(False).alias(col))
            else:
                df = df.with_columns(pl.lit(None).alias(col))

    cast_exprs = []
    for col, dtype in config.schema_dtypes.items():
        if col == config.time_column:
            cast_exprs.append(
                pl.col(col)
                .str.to_datetime("%Y-%m-%d %H:%M:%S")
                .dt.replace_time_zone("UTC")
                .alias(col)
            )
        else:
            cast_exprs.append(pl.col(col).cast(dtype))
    df = df.with_columns(cast_exprs)
    return df.select(config.canonical_columns)


def _put_parquet(s3_client: Any, *, bucket: str, key: str, df: pl.DataFrame) -> None:
    buffer = io.BytesIO()
    df.write_parquet(buffer, compression="zstd")
    buffer.seek(0)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )


def _list_keys(s3_client: Any, *, bucket: str, prefix: str) -> list[str]:
    paginator = s3_client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def _delete_prefix(s3_client: Any, *, bucket: str, prefix: str) -> None:
    keys = _list_keys(s3_client, bucket=bucket, prefix=prefix)
    if not keys:
        return
    for i in range(0, len(keys), 1000):
        chunk = keys[i:i + 1000]
        s3_client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": key} for key in chunk]},
        )


def _ensure_month_empty(
    s3_client: Any,
    *,
    bucket: str,
    hot_month_prefix: str,
    warm_month_prefix: str,
    overwrite: bool,
) -> None:
    existing_hot = _list_keys(s3_client, bucket=bucket, prefix=hot_month_prefix)
    existing_warm = _list_keys(s3_client, bucket=bucket, prefix=warm_month_prefix)
    if not existing_hot and not existing_warm:
        return
    if not overwrite:
        raise RuntimeError(
            "target month already has data. Pass --overwrite-month to clear it first."
        )
    _delete_prefix(s3_client, bucket=bucket, prefix=hot_month_prefix)
    _delete_prefix(s3_client, bucket=bucket, prefix=warm_month_prefix)


def _queue_request(
    request_path: str,
    *,
    provider: str,
    hot_key: str,
    timestamp: datetime,
    timeout_seconds: float,
) -> None:
    request_file = Path(request_path)
    request_file.parent.mkdir(parents=True, exist_ok=True)
    request_file.write_text(
        json.dumps(
            {
                "provider": provider,
                "hot_key": hot_key,
                "timestamp": timestamp.isoformat(),
                "timeout_seconds": timeout_seconds,
            }
        ),
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider", default="aws")
    parser.add_argument("--env", default="test", choices=["test", "production"])
    parser.add_argument("--timestamp", required=True, help="10-minute slot that should become hot_idx=511")
    parser.add_argument("--request-path", required=True, help="Where to write the queued compaction request JSON")
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    parser.add_argument("--overwrite-month", action="store_true")
    args = parser.parse_args()

    os.environ["TITANS_ENV"] = args.env
    config = get_config(args.provider)
    trigger_ts = _parse_timestamp(args.timestamp)
    base_ts = trigger_ts - timedelta(minutes=(L3_TRIGGER_LAST_HOT_IDX + 1) * 10)
    if base_ts.year != trigger_ts.year or base_ts.month != trigger_ts.month:
        raise ValueError(
            "synthetic L3 seed must stay within one month; pick a trigger timestamp at least 3d 13h10m into the month"
        )

    s3_client = boto3.client("s3")
    bucket = config.titans_bucket
    hot_month_prefix = f"{config.hot_prefix}/{trigger_ts.year}/{trigger_ts.month:02d}/"
    warm_month_prefix = f"{config.warm_prefix}/{trigger_ts.year}/{trigger_ts.month:02d}/"
    _ensure_month_empty(
        s3_client,
        bucket=bucket,
        hot_month_prefix=hot_month_prefix,
        warm_month_prefix=warm_month_prefix,
        overwrite=args.overwrite_month,
    )

    warm_entries_l1 = []
    warm_entries_l2 = []
    hot_entries = []

    # Pre-existing L1 files: 448-503 (7 files)
    for i in range(DEFAULT_M - 1):
        start_idx = 448 + i * DEFAULT_M
        end_idx = start_idx + DEFAULT_M - 1
        ts = base_ts + timedelta(minutes=start_idx * 10)
        df = _make_df(config, idx=start_idx, ts=ts, label=f"l1-{start_idx:05d}")
        filename = f"L1_{i:04d}_{start_idx:05d}-{end_idx:05d}.parquet"
        key = f"{warm_month_prefix}{filename}"
        _put_parquet(s3_client, bucket=bucket, key=key, df=df)
        warm_entries_l1.append({"file": filename, "hot_range": [start_idx, end_idx]})

    # Pre-existing L2 files: 0-447 (7 files)
    for i in range(DEFAULT_M - 1):
        start_idx = i * (DEFAULT_M ** 2)
        end_idx = start_idx + (DEFAULT_M ** 2) - 1
        ts = base_ts + timedelta(minutes=start_idx * 10)
        df = _make_df(config, idx=start_idx, ts=ts, label=f"l2-{start_idx:05d}")
        filename = f"L2_{(DEFAULT_M - 1) + i:04d}_{start_idx:05d}-{end_idx:05d}.parquet"
        key = f"{warm_month_prefix}{filename}"
        _put_parquet(s3_client, bucket=bucket, key=key, df=df)
        warm_entries_l2.append({"file": filename, "hot_range": [start_idx, end_idx]})

    # Pre-existing L0 hot files: 504-510 (7 files)
    for idx in range(504, 511):
        ts = base_ts + timedelta(minutes=idx * 10)
        df = _make_df(config, idx=idx, ts=ts, label=f"l0-{idx:05d}")
        key = (
            f"{config.hot_prefix}/"
            f"{ts.year}/{ts.month:02d}/{ts.day:02d}/"
            f"{ts.hour:02d}-{ts.minute:02d}.parquet"
        )
        _put_parquet(s3_client, bucket=bucket, key=key, df=df)
        hot_entries.append({"file": key, "hot_range": [idx, idx]})

    manifest_key = f"{warm_month_prefix}manifest.json"
    manifest = {
        "m": DEFAULT_M,
        "provider": args.provider,
        "year": trigger_ts.year,
        "month": trigger_ts.month,
        "pk_columns": config.pk_columns,
        "next_file_id": 2 * (DEFAULT_M - 1),
        "last_hot_idx": L3_TRIGGER_LAST_HOT_IDX,
        "last_processed_time": (trigger_ts - timedelta(minutes=10)).isoformat(),
        "pending_deletions": [],
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "levels": {
            "0": hot_entries,
            "1": warm_entries_l1,
            "2": warm_entries_l2,
        },
    }
    s3_client.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=json.dumps(manifest, indent=2),
        ContentType="application/json",
    )

    trigger_df = pd.DataFrame(_make_df(
        config,
        idx=L3_TRIGGER_LAST_HOT_IDX + 1,
        ts=trigger_ts,
        label=f"trigger-{L3_TRIGGER_LAST_HOT_IDX + 1:05d}",
    ).to_dicts())
    hot_key = upload_hot_tier(trigger_df, trigger_ts, provider=args.provider, s3_client=s3_client)
    if not hot_key:
        raise RuntimeError("failed to upload synthetic trigger hot file")

    _queue_request(
        args.request_path,
        provider=args.provider,
        hot_key=hot_key,
        timestamp=trigger_ts,
        timeout_seconds=args.timeout_seconds,
    )

    print(
        f"[TITANS/{args.provider}] synthetic pre-L3 seed ready "
        f"timestamp={trigger_ts.isoformat()} hot_key={hot_key} request={args.request_path}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
