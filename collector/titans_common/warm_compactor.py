"""Warm tier incremental compactor (Multi-provider support).

Deletion policy: Deferred deletion.
  Files scheduled for deletion are recorded in manifest's `pending_deletions`
  and physically deleted on the NEXT compaction cycle (~10 min later).
  This guarantees that in-flight queries never encounter missing files.

Idempotency: last_processed_time (single datetime).
  Hot files arrive in strict chronological order from a single-writer collector.
  A file is skipped if its timestamp <= last_processed_time.

  IMPORTANT: This design assumes:
    1. Single writer (one collector process at a time)
    2. Strictly ordered arrival (10-min interval batches)
    3. No backfill of past timestamps
  If any of these assumptions change (e.g., multi-writer or backfill),
  a key-based deduplication mechanism must be reintroduced.
"""
from __future__ import annotations

import io
import json
import os
from pathlib import Path
import resource
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.exceptions import ClientError
import polars as pl

from .config import get_config, ProviderConfig

DEFAULT_M = 8
AZURE_PARTITIONED_MIN_LEVEL = int(os.environ.get("TITANS_AZURE_PARTITIONED_MIN_LEVEL", "3"))
AZURE_PARTITION_PREFIX_LEN = int(os.environ.get("TITANS_AZURE_PARTITION_PREFIX_LEN", "2"))
AZURE_PARTITION_DEFAULT_PREFIX_LEN = int(os.environ.get("TITANS_AZURE_PARTITION_DEFAULT_PREFIX_LEN", "1"))
AZURE_PARTITION_SPLIT_PREFIXES = {
    value.strip()
    for value in os.environ.get("TITANS_AZURE_PARTITION_SPLIT_PREFIXES", "D,E").split(",")
    if value.strip()
}


def _rss_mb() -> float:
    """Return current process RSS in MiB when available."""
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024.0
    except OSError:
        pass

    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


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
    last_processed_time: datetime | None = None
    pending_deletions: list[str] = field(default_factory=list)

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
            self.pending_deletions = list(data.get("pending_deletions", []))

            # last_processed_time: load directly
            lpt = data.get("last_processed_time")
            if lpt:
                try:
                    self.last_processed_time = datetime.fromisoformat(lpt)
                except (ValueError, TypeError):
                    self.last_processed_time = None

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
        print(
            f"[WARM/{self.provider}] manifest save start "
            f"rss_mb={_rss_mb():.1f}"
        )
        data = {
            "m": self.m,
            "provider": self.provider,
            "year": self.year,
            "month": self.month,
            "pk_columns": self.config.pk_columns,
            "next_file_id": self.next_file_id,
            "last_hot_idx": self.last_hot_idx,
            "last_processed_time": (
                self.last_processed_time.isoformat()
                if self.last_processed_time
                else None
            ),
            "pending_deletions": self.pending_deletions,
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

        body = json.dumps(data, indent=2)
        try:
            if self.manifest_etag:
                response = self.s3_client.put_object(
                    Bucket=self.bucket,
                    Key=key,
                    Body=body,
                    ContentType="application/json",
                    IfMatch=self.manifest_etag,
                )
            else:
                response = self.s3_client.put_object(
                    Bucket=self.bucket,
                    Key=key,
                    Body=body,
                    ContentType="application/json",
                    IfNoneMatch="*",
                )
            # Update ETag so subsequent saves use IfMatch
            self.manifest_etag = response.get("ETag")
            print(
                f"[WARM/{self.provider}] manifest save end "
                f"rss_mb={_rss_mb():.1f}"
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "PreconditionFailed":
                raise ConcurrencyConflictError(
                    "Manifest was modified by another process. Retry required."
                )
            raise

    def flush_pending_deletions(self):
        """Delete files recorded in the previous compaction cycle.

        Called at the START of each cycle, before new compaction.
        This ensures deleted files remain on S3 for at least one full
        cycle (~10 min), so in-flight queries never hit missing files.

        Only successfully deleted entries are removed from pending_deletions.
        Failed entries are retained for retry on the next cycle.
        """
        if not self.pending_deletions:
            return
        failed = []
        for key in self.pending_deletions:
            try:
                self.s3_client.delete_object(Bucket=self.bucket, Key=key)
                print(f"[WARM/{self.provider}] Deleted (deferred) {key}")
            except Exception as e:
                print(f"[WARM/{self.provider}] FAILED to delete {key}: {e}")
                failed.append(key)
        if failed:
            print(f"[WARM/{self.provider}] {len(failed)} deletions failed, will retry next cycle")
        self.pending_deletions = failed

    def add_hot_file(self, hot_s3_key: str) -> list[str]:
        """Add Hot file - idempotency via last_processed_time + rollback on failure."""
        print(
            f"[WARM/{self.provider}] add_hot_file start "
            f"hot_key={hot_s3_key} rss_mb={_rss_mb():.1f}"
        )
        # Idempotency: skip if already processed
        file_time = self._parse_time_from_key(hot_s3_key)
        if self.last_processed_time and file_time and file_time <= self.last_processed_time:
            print(f"[WARM/{self.provider}] Skipping already processed: {hot_s3_key}")
            return []

        self.last_hot_idx += 1
        hot_idx = self.last_hot_idx

        wf = WarmFile(level=0, hot_range=(hot_idx, hot_idx), filename=hot_s3_key)
        self.levels[0].append(wf)

        # Execute compaction (track created files for rollback)
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

        # Defer deletion: record for next cycle instead of deleting now
        self.pending_deletions.extend(deleted_files)

        # Update last_processed_time
        if file_time:
            self.last_processed_time = file_time

        # Save manifest (includes pending_deletions)
        self._save_manifest()

        print(
            f"[WARM/{self.provider}] add_hot_file end "
            f"hot_key={hot_s3_key} pending_deletions={len(self.pending_deletions)} rss_mb={_rss_mb():.1f}"
        )

        return deleted_files

    def _parse_time_from_key(self, hot_s3_key: str) -> datetime | None:
        """Parse timestamp from hot file path.

        e.g., 'test/parquet_cp_hot/aws/2026/01/23/14-20.parquet'
            → datetime(2026, 1, 23, 14, 20, tzinfo=UTC)

        Parses year/month/day from the key path itself (not self.year/month)
        to avoid incorrect datetime at month boundaries.
        """
        try:
            parts = hot_s3_key.removesuffix(".parquet").split("/")
            # parts: [..., YYYY, MM, DD, HH-MM]
            year = int(parts[-4])
            month = int(parts[-3])
            day = int(parts[-2])
            hour, minute = map(int, parts[-1].split("-"))
            return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
        except (ValueError, IndexError):
            return None

    def _compact_with_tracking(self, level: int) -> tuple[list[str], list[str]]:
        """Execute compaction - track created files."""
        deleted_files = []
        created_files = []

        while len(self.levels[level]) >= self.m:
            print(
                f"[WARM/{self.provider}] compact level={level} start "
                f"queue={len(self.levels[level])} rss_mb={_rss_mb():.1f}"
            )
            to_merge = self.levels[level][:self.m]
            self.levels[level] = self.levels[level][self.m:]

            merged = self._merge_files(to_merge, level + 1)
            created_files.append(f"{self.warm_prefix}/{merged.filename}")
            self.levels[level + 1].append(merged)

            for wf in to_merge:
                if wf.level == 0:
                    # Hot file: filename is full path (e.g., "test/parquet_cp_hot/...")
                    deleted_files.append(wf.filename)
                else:
                    # Warm file (L1+): prepend warm_prefix
                    deleted_files.append(f"{self.warm_prefix}/{wf.filename}")

            sub_deleted, sub_created = self._compact_with_tracking(level + 1)
            deleted_files.extend(sub_deleted)
            created_files.extend(sub_created)

            print(
                f"[WARM/{self.provider}] compact level={level} end "
                f"created={merged.filename} rss_mb={_rss_mb():.1f}"
            )

        return deleted_files, created_files

    def _merge_files(self, files: list[WarmFile], new_level: int) -> WarmFile:
        """Merge files to create new Warm file."""
        if self.provider == "azure" and new_level >= AZURE_PARTITIONED_MIN_LEVEL:
            return self._merge_files_partitioned(files, new_level)

        start_time = time.time()
        input_rows = 0
        print(
            f"[WARM/{self.provider}] merge start strategy=eager "
            f"level={new_level} files={len(files)} rss_mb={_rss_mb():.1f}"
        )

        dfs = []
        schema = self.config.schema_dtypes

        for wf in files:
            key = wf.filename if wf.level == 0 else f"{self.warm_prefix}/{wf.filename}"
            response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            df = pl.read_parquet(io.BytesIO(response["Body"].read()))
            input_rows += df.height

            # Ensure dtype consistency
            cast_exprs = [pl.col(c).cast(schema[c]) for c in df.columns if c in schema]
            if cast_exprs:
                df = df.with_columns(cast_exprs)
            dfs.append(df)

        print(
            f"[WARM/{self.provider}] merge eager loaded "
            f"level={new_level} input_rows={input_rows} rss_mb={_rss_mb():.1f}"
        )

        # Merge & sort (Provider-specific PK + Time)
        sort_cols = self.config.pk_columns + [self.config.time_column]
        combined = pl.concat(dfs, how="diagonal").sort(sort_cols)

        # Dedup: collector may emit both Ceased=true and Ceased=false for
        # the same PK+Time. Keep Ceased=false (real values) over Ceased=true.
        if "Ceased" in combined.columns:
            before = combined.height
            combined = (
                combined
                .sort(sort_cols + ["Ceased"])
                .unique(subset=sort_cols, keep="first")
                .sort(sort_cols)
            )
            deduped = before - combined.height
            if deduped > 0:
                print(f"[WARM/{self.provider}] Deduplicated {deduped} ceased duplicate rows")

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

        print(
            f"[WARM/{self.provider}] Created {filename} (L{new_level}, {combined.height} rows) "
            f"elapsed_s={time.time() - start_time:.2f} rss_mb={_rss_mb():.1f}"
        )

        return WarmFile(level=new_level, hot_range=(start_idx, end_idx), filename=filename)

    def _merge_files_partitioned(self, files: list[WarmFile], new_level: int) -> WarmFile:
        """Merge Azure warm files via range partition + subprocess eager merge."""
        from .partitioned_eager_merge import (
            concat_partition_outputs,
            materialize_azure_partitions,
        )

        start_time = time.time()
        print(
            f"[WARM/{self.provider}] merge start strategy=partitioned "
            f"level={new_level} files={len(files)} rss_mb={_rss_mb():.1f}"
        )
        with tempfile.TemporaryDirectory(prefix=f"warm_merge_{self.provider}_") as tmp:
            root = Path(tmp)
            input_dir = root / "inputs"
            partition_dir = root / "partitions"
            merged_dir = root / "merged"
            final_dir = root / "final"
            input_dir.mkdir(parents=True, exist_ok=True)
            merged_dir.mkdir(parents=True, exist_ok=True)
            final_dir.mkdir(parents=True, exist_ok=True)

            local_inputs: list[Path] = []
            download_started = time.time()
            for wf in files:
                key = wf.filename if wf.level == 0 else f"{self.warm_prefix}/{wf.filename}"
                response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
                local_path = input_dir / Path(key).name
                local_path.write_bytes(response["Body"].read())
                local_inputs.append(local_path)
            print(
                f"[WARM/{self.provider}] partitioned download complete "
                f"level={new_level} files={len(local_inputs)} elapsed_s={time.time() - download_started:.2f} "
                f"rss_mb={_rss_mb():.1f}"
            )

            materialize_started = time.time()
            partitions = materialize_azure_partitions(
                local_inputs,
                partition_dir,
                prefix_len=AZURE_PARTITION_PREFIX_LEN,
                split_prefixes=AZURE_PARTITION_SPLIT_PREFIXES,
                default_prefix_len=AZURE_PARTITION_DEFAULT_PREFIX_LEN,
            )
            print(
                f"[WARM/{self.provider}] partitioned materialize complete "
                f"level={new_level} partitions={len(partitions)} elapsed_s={time.time() - materialize_started:.2f} "
                f"rss_mb={_rss_mb():.1f}"
            )

            merge_script = Path(__file__).with_name("partitioned_eager_merge.py")
            partition_outputs: list[Path] = []
            total_rows = 0
            child_env = os.environ.copy()
            child_env.setdefault("POLARS_MAX_THREADS", "1")
            child_env.setdefault("RAYON_NUM_THREADS", "1")
            child_env.setdefault("OMP_NUM_THREADS", "1")

            merge_partitions_started = time.time()
            total_partitions = len(partitions)
            for idx, part in enumerate(partitions, start=1):
                part_dir = partition_dir / part.replace(":", "__")
                out_path = merged_dir / f"{part.replace(':', '__')}.parquet"
                part_started = time.time()
                print(
                    f"[WARM/{self.provider}] partitioned merge part start "
                    f"level={new_level} part={idx}/{total_partitions} key={part} rss_mb={_rss_mb():.1f}"
                )
                result = subprocess.run(
                    [sys.executable, str(merge_script), "--partition-dir", str(part_dir), "--output", str(out_path)],
                    check=True,
                    capture_output=True,
                    text=True,
                    env=child_env,
                )
                part_rows = 0
                for line in result.stdout.splitlines():
                    if line.startswith("rows="):
                        part_rows = int(line.split("=", 1)[1])
                        total_rows += part_rows
                        break
                print(
                    f"[WARM/{self.provider}] partitioned merge part end "
                    f"level={new_level} part={idx}/{total_partitions} key={part} rows={part_rows} "
                    f"elapsed_s={time.time() - part_started:.2f} rss_mb={_rss_mb():.1f}"
                )
                partition_outputs.append(out_path)
            print(
                f"[WARM/{self.provider}] partitioned merge complete "
                f"level={new_level} partitions={total_partitions} total_rows={total_rows} "
                f"elapsed_s={time.time() - merge_partitions_started:.2f} rss_mb={_rss_mb():.1f}"
            )

            start_idx = min(wf.hot_range[0] for wf in files)
            end_idx = max(wf.hot_range[1] for wf in files)
            filename = f"L{new_level}_{self.next_file_id:04d}_{start_idx:05d}-{end_idx:05d}.parquet"
            self.next_file_id += 1

            final_path = final_dir / filename
            concat_started = time.time()
            concat_partition_outputs(sorted(partition_outputs), final_path)
            print(
                f"[WARM/{self.provider}] partitioned concat complete "
                f"level={new_level} elapsed_s={time.time() - concat_started:.2f} rss_mb={_rss_mb():.1f}"
            )
            key = f"{self.warm_prefix}/{filename}"
            upload_started = time.time()
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=final_path.read_bytes(),
                ContentType="application/octet-stream",
            )
            print(
                f"[WARM/{self.provider}] Created {filename} "
                f"(L{new_level}, {total_rows} rows, partitioned) "
                f"upload_elapsed_s={time.time() - upload_started:.2f} "
                f"elapsed_s={time.time() - start_time:.2f} rss_mb={_rss_mb():.1f}"
            )
            return WarmFile(level=new_level, hot_range=(start_idx, end_idx), filename=filename)


def run_compaction(
    hot_s3_key: str,
    timestamp: datetime,
    provider: str = "aws",
    timeout_seconds: float = 30.0,
    s3_client=None,
) -> None:
    """Execute compaction after Hot file upload (with time limit)."""
    if timestamp.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware (use UTC)")

    ts_utc = timestamp.astimezone(timezone.utc)
    start_time = time.time()
    print(
        f"[WARM/{provider}] run_compaction start "
        f"hot_key={hot_s3_key} timestamp={ts_utc.isoformat()} rss_mb={_rss_mb():.1f}"
    )

    compactor = WarmCompactor(
        m=DEFAULT_M,
        year=ts_utc.year,
        month=ts_utc.month,
        provider=provider,
        s3_client=s3_client,
    )

    # 1. Delete files from previous cycle (deferred deletion)
    compactor.flush_pending_deletions()

    # 2. Add hot file + compact (new deletions recorded in pending_deletions)
    deleted_files = compactor.add_hot_file(hot_s3_key)

    elapsed = time.time() - start_time
    if elapsed > timeout_seconds:
        print(f"[WARN] Compaction took {elapsed:.1f}s, exceeds {timeout_seconds}s limit")

    total_files = sum(len(files) for files in compactor.levels.values())
    print(
        f"[WARM/{provider}] Current warm files: {total_files} "
        f"deleted_files={len(deleted_files)} elapsed_s={elapsed:.2f} rss_mb={_rss_mb():.1f}"
    )
