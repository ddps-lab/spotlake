"""Warm tier incremental compactor (Multi-provider support)."""
from __future__ import annotations

import io
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.exceptions import ClientError
import polars as pl

from .config import get_config, ProviderConfig

DEFAULT_M = 8
PROCESSED_KEYS_CAP = 5000  # Monthly max ~4500 Hot files


class ConcurrencyConflictError(Exception):
    """Concurrency conflict exception."""
    pass


@dataclass
class WarmFile:
    level: int
    hot_range: tuple[int, int]
    filename: str


@dataclass
class WarmCompactor:
    """S3-based m-way incremental compactor (multi-provider support)."""

    m: int
    year: int
    month: int
    provider: str = "aws"
    config: ProviderConfig = field(default=None, repr=False)
    s3_client: Any = field(default=None, repr=False)
    levels: dict[int, list[WarmFile]] = field(default_factory=dict)
    next_file_id: int = 0
    last_hot_idx: int = -1
    manifest_etag: str | None = None
    processed_keys: set[str] = field(default_factory=set)

    def __post_init__(self):
        self.config = get_config(self.provider)
        if self.s3_client is None:
            self.s3_client = boto3.client("s3")
        self.levels = {i: [] for i in range(10)}
        self._load_manifest()

    @property
    def warm_prefix(self) -> str:
        return f"{self.config.warm_prefix}/{self.year}/{self.month:02d}"

    @property
    def bucket(self) -> str:
        return self.config.titans_bucket

    def _load_manifest(self):
        """Load manifest.json from S3 + save ETag."""
        key = f"{self.warm_prefix}/manifest.json"
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            self.manifest_etag = response.get("ETag")
            data = json.loads(response["Body"].read())
            self.next_file_id = data.get("next_file_id", 0)
            self.last_hot_idx = data.get("last_hot_idx", -1)
            self.processed_keys = set(data.get("processed_keys", []))
            for level_str, files in data.get("levels", {}).items():
                level = int(level_str)
                self.levels[level] = [
                    WarmFile(level=level, hot_range=tuple(f["hot_range"]), filename=f["file"])
                    for f in files
                ]
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                self.manifest_etag = None  # First run
            else:
                raise

    def _save_manifest(self):
        """Save manifest.json to S3 (Optimistic Locking)."""
        # Keep only recent N processed_keys
        recent_keys = sorted(self.processed_keys)[-PROCESSED_KEYS_CAP:]

        data = {
            "m": self.m,
            "provider": self.provider,
            "year": self.year,
            "month": self.month,
            "pk_columns": self.config.pk_columns,
            "next_file_id": self.next_file_id,
            "last_hot_idx": self.last_hot_idx,
            "processed_keys": recent_keys,
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "levels": {
                str(level): [
                    {"file": wf.filename, "hot_range": list(wf.hot_range)}
                    for wf in files
                ]
                for level, files in self.levels.items()
                if files
            },
        }

        key = f"{self.warm_prefix}/manifest.json"

        try:
            if self.manifest_etag:
                # Existing manifest -> overwrite only if ETag matches
                self.s3_client.put_object(
                    Bucket=self.bucket,
                    Key=key,
                    Body=json.dumps(data, indent=2),
                    ContentType="application/json",
                    IfMatch=self.manifest_etag,
                )
            else:
                # First creation -> create only if file doesn't exist
                self.s3_client.put_object(
                    Bucket=self.bucket,
                    Key=key,
                    Body=json.dumps(data, indent=2),
                    ContentType="application/json",
                    IfNoneMatch="*",
                )
        except ClientError as e:
            if e.response["Error"]["Code"] == "PreconditionFailed":
                raise ConcurrencyConflictError(
                    "Manifest was modified by another process. Retry required."
                )
            raise

    def add_hot_file(self, hot_s3_key: str) -> list[str]:
        """Add Hot file - idempotency guaranteed + rollback on failure."""
        # Skip if already processed
        if hot_s3_key in self.processed_keys:
            print(f"[WARM/{self.provider}] Skipping already processed: {hot_s3_key}")
            return []

        self.processed_keys.add(hot_s3_key)
        self.last_hot_idx += 1
        hot_idx = self.last_hot_idx

        wf = WarmFile(level=0, hot_range=(hot_idx, hot_idx), filename=hot_s3_key)
        self.levels[0].append(wf)

        # Execute compaction (track created files)
        created_files = []
        try:
            deleted_files, created_files = self._compact_with_tracking(0)
        except Exception as e:
            # Rollback: delete created files
            for f in created_files:
                try:
                    self.s3_client.delete_object(Bucket=self.bucket, Key=f)
                except Exception:
                    pass
            raise

        # Save manifest
        self._save_manifest()

        return deleted_files

    def _compact_with_tracking(self, level: int) -> tuple[list[str], list[str]]:
        """Execute compaction - track created files."""
        deleted_files = []
        created_files = []

        while len(self.levels[level]) >= self.m:
            to_merge = self.levels[level][:self.m]
            self.levels[level] = self.levels[level][self.m:]

            merged = self._merge_files(to_merge, level + 1)
            created_files.append(f"{self.warm_prefix}/{merged.filename}")
            self.levels[level + 1].append(merged)

            for wf in to_merge:
                if wf.level > 0:
                    deleted_files.append(f"{self.warm_prefix}/{wf.filename}")

            sub_deleted, sub_created = self._compact_with_tracking(level + 1)
            deleted_files.extend(sub_deleted)
            created_files.extend(sub_created)

        return deleted_files, created_files

    def _merge_files(self, files: list[WarmFile], new_level: int) -> WarmFile:
        """Merge files to create new Warm file."""
        dfs = []
        schema = self.config.schema_dtypes

        for wf in files:
            key = wf.filename if wf.level == 0 else f"{self.warm_prefix}/{wf.filename}"
            response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            df = pl.read_parquet(io.BytesIO(response["Body"].read()))

            # Ensure dtype consistency
            cast_exprs = [pl.col(c).cast(schema[c]) for c in df.columns if c in schema]
            if cast_exprs:
                df = df.with_columns(cast_exprs)
            dfs.append(df)

        # Merge & sort (Provider-specific PK + Time)
        sort_cols = self.config.pk_columns + [self.config.time_column]
        combined = pl.concat(dfs, how="diagonal").sort(sort_cols)

        start_idx = min(wf.hot_range[0] for wf in files)
        end_idx = max(wf.hot_range[1] for wf in files)

        filename = f"L{new_level}_{self.next_file_id:04d}_{start_idx:05d}-{end_idx:05d}.parquet"
        self.next_file_id += 1

        buffer = io.BytesIO()
        combined.write_parquet(buffer, compression="zstd")
        buffer.seek(0)

        key = f"{self.warm_prefix}/{filename}"
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
        )

        print(f"[WARM/{self.provider}] Created {filename} (L{new_level}, {combined.height} rows)")

        return WarmFile(level=new_level, hot_range=(start_idx, end_idx), filename=filename)

    def cleanup_deleted_files(self, deleted_keys: list[str]):
        """Delete unnecessary files after compaction."""
        for key in deleted_keys:
            try:
                self.s3_client.delete_object(Bucket=self.bucket, Key=key)
                print(f"[WARM/{self.provider}] Deleted {key}")
            except Exception as e:
                print(f"[WARM/{self.provider}] Failed to delete {key}: {e}")


def run_compaction(
    hot_s3_key: str,
    timestamp: datetime,
    provider: str = "aws",
    timeout_seconds: float = 30.0,
) -> None:
    """Execute compaction after Hot file upload (with time limit)."""
    if timestamp.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware (use UTC)")

    ts_utc = timestamp.astimezone(timezone.utc)
    start_time = time.time()

    compactor = WarmCompactor(
        m=DEFAULT_M,
        year=ts_utc.year,
        month=ts_utc.month,
        provider=provider,
    )

    deleted_files = compactor.add_hot_file(hot_s3_key)

    elapsed = time.time() - start_time
    if elapsed > timeout_seconds:
        print(f"[WARN] Compaction took {elapsed:.1f}s, exceeds {timeout_seconds}s limit")

    if deleted_files:
        compactor.cleanup_deleted_files(deleted_files)

    total_files = sum(len(files) for files in compactor.levels.values())
    print(f"[WARM/{provider}] Current warm files: {total_files}")
