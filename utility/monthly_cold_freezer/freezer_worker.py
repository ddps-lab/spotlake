#!/usr/bin/env python3
"""Spotlake-local Warm+Hot -> Cold tier conversion worker."""

from __future__ import annotations

import argparse
import calendar
import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

import boto3
from botocore.exceptions import ClientError
import polars as pl

BUCKET = "titans-spotlake-data"
ROW_GROUP_SIZE = 250_000

PROVIDERS = {
    "aws": {"pk_columns": ["InstanceType", "Region", "AZ"]},
    "azure": {"pk_columns": ["InstanceTier", "InstanceType", "Region", "AZ"]},
    "gcp": {"pk_columns": ["InstanceType", "Region"]},
}


class FreezeError(RuntimeError):
    """Raised when monthly freeze cannot proceed safely."""


@dataclass(frozen=True)
class FreezeControls:
    ignore_completeness: bool
    overwrite_existing: bool


@dataclass(frozen=True)
class FreezeResult:
    provider: str
    year: int
    month: int
    next_year: int
    next_month: int
    cp_file: Path
    ap_file: Path
    cp_rows: int
    ap_rows: int


def parse_args():
    parser = argparse.ArgumentParser(
        description="Spotlake-local Warm+Hot -> Cold tier conversion (multi-provider)"
    )
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument(
        "--provider",
        choices=list(PROVIDERS.keys()),
        required=True,
        help="Cloud provider (aws, azure, gcp)",
    )
    parser.add_argument("--profile", help="AWS CLI profile name")
    parser.add_argument(
        "--env",
        choices=["test", "production"],
        default="production",
        help="Environment (test uses test/ S3 prefix for warm/hot)",
    )
    parser.add_argument(
        "--output-dir",
        help="Output directory (default: .tmp/freeze/{provider}/YYYY-MM)",
    )
    parser.add_argument("--skip-upload", action="store_true", help="Skip S3 upload")
    parser.add_argument(
        "--ignore-completeness",
        action="store_true",
        help="Skip month completeness checks and proceed immediately",
    )
    parser.add_argument(
        "--overwrite-existing",
        action="store_true",
        help="Allow overwriting existing cold outputs on S3",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Legacy alias for --ignore-completeness and --overwrite-existing",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Poll manifest until month is complete, then auto-convert and upload",
    )
    parser.add_argument(
        "--watch-interval",
        type=int,
        default=300,
        help="Seconds between polls in watch mode (default: 300)",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    try:
        run_freeze(
            year=args.year,
            month=args.month,
            provider=args.provider,
            profile=args.profile,
            env=args.env,
            output_dir=args.output_dir,
            skip_upload=args.skip_upload,
            watch=args.watch,
            watch_interval=args.watch_interval,
            ignore_completeness=args.ignore_completeness,
            overwrite_existing=args.overwrite_existing,
            force=args.force,
        )
    except FreezeError as exc:
        print(f"[ERROR] {exc}")
        sys.exit(1)


def run_freeze(
    *,
    year: int,
    month: int,
    provider: str,
    profile: str | None = None,
    env: str = "production",
    output_dir: str | Path | None = None,
    skip_upload: bool = False,
    watch: bool = False,
    watch_interval: int = 300,
    ignore_completeness: bool = False,
    overwrite_existing: bool = False,
    force: bool = False,
) -> FreezeResult:
    """Run monthly warm->cold conversion and return output metadata."""
    controls = resolve_control_flags(
        force=force,
        ignore_completeness=ignore_completeness,
        overwrite_existing=overwrite_existing,
    )

    pk_columns = PROVIDERS[provider]["pk_columns"]
    env_prefix = "test/" if env == "test" else ""
    output_dir_path = Path(output_dir or f".tmp/freeze/{provider}/{year}-{month:02d}")
    output_dir_path.mkdir(parents=True, exist_ok=True)

    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    s3 = session.client("s3")

    next_year, next_month = (year + 1, 1) if month == 12 else (year, month + 1)
    check_not_already_uploaded(
        s3,
        provider,
        year,
        month,
        next_year,
        next_month,
        overwrite_existing=controls.overwrite_existing,
    )

    warm_prefix = f"{env_prefix}parquet_warm/{provider}/m8/{year}/{month:02d}"
    if watch:
        manifest = watch_until_complete(s3, warm_prefix, year, month, watch_interval)
    else:
        manifest = load_manifest(s3, warm_prefix, required=False)
        if manifest is not None:
            check_completeness(
                manifest,
                year,
                month,
                ignore_completeness=controls.ignore_completeness,
            )

    s3_uris = enumerate_input_files(
        s3,
        manifest,
        warm_prefix,
        provider,
        year,
        month,
        env,
    )
    print(f"[freeze] Total files to read: {len(s3_uris)}")
    if not s3_uris:
        raise FreezeError("No warm/hot input files found for the target month.")

    all_cps = read_all_cps(s3_uris, profile)
    print(f"[freeze] Total CP rows (raw): {all_cps.height:,}")

    month_start = datetime(year, month, 1, tzinfo=timezone.utc)
    month_end = datetime(next_year, next_month, 1, tzinfo=timezone.utc)
    before_filter = all_cps.height
    all_cps = all_cps.filter(
        (pl.col("Time") >= month_start) & (pl.col("Time") < month_end)
    )
    filtered = before_filter - all_cps.height
    if filtered > 0:
        print(f"[freeze] Filtered {filtered} out-of-range rows")
    print(f"[freeze] CP rows: {all_cps.height:,}")

    sort_cols = pk_columns + ["Time"]
    if "Ceased" in all_cps.columns:
        before = all_cps.height
        all_cps = all_cps.sort(sort_cols + ["Ceased"]).unique(
            subset=sort_cols, keep="first"
        )
        deduped = before - all_cps.height
        if deduped > 0:
            print(f"[freeze] Deduplicated {deduped} ceased/resumed rows")

    all_cps = reorder_columns(all_cps, pk_columns)
    all_cps = all_cps.sort(sort_cols)

    cp_file = output_dir_path / f"{year}-{month:02d}.parquet"
    all_cps.write_parquet(cp_file, compression="zstd", row_group_size=ROW_GROUP_SIZE)
    print(f"[freeze] CP written: {cp_file} ({cp_file.stat().st_size / 1024**2:.1f} MB)")

    prev_ap_key = f"{provider}/{year}-{month:02d}_AP.parquet"
    prev_ap_uri = f"s3://{BUCKET}/{prev_ap_key}"
    print(f"[freeze] Loading previous AP: {prev_ap_uri}")
    try:
        prev_ap = pl.read_parquet(
            prev_ap_uri,
            storage_options=_storage_options(profile),
        )
    except Exception as exc:
        raise FreezeError(f"Failed to read previous AP {prev_ap_uri}: {exc}") from exc
    print(f"[freeze] Previous AP loaded: {prev_ap.height:,} rows")

    combined = pl.concat([prev_ap, all_cps], how="diagonal")
    if "Ceased" not in combined.columns:
        combined = combined.with_columns(pl.lit(False).alias("Ceased"))
    else:
        combined = combined.with_columns(pl.col("Ceased").fill_null(False))

    value_cols = [
        c for c in combined.columns
        if c not in pk_columns and c not in {"Time", "Ceased"}
    ]
    agg_exprs = [
        pl.col("Time").sort_by("Time").last(),
        pl.col("Ceased").sort_by("Time").last().alias("_last_ceased"),
    ]
    for col in value_cols:
        agg_exprs.append(pl.col(col).sort_by("Time").last())

    next_ap = (
        combined.group_by(pk_columns)
        .agg(agg_exprs)
        .filter(pl.col("_last_ceased") == False)  # noqa: E712
        .drop("_last_ceased")
        .sort(sort_cols)
    )
    ceased_pk_count = (
        combined.select(pk_columns).unique().height
        - next_ap.select(pk_columns).unique().height
    )
    if ceased_pk_count > 0:
        print(f"[freeze] Excluded {ceased_pk_count} ceased PKs from AP")

    next_ap = reorder_columns(next_ap, pk_columns)

    ap_file = output_dir_path / f"{next_year}-{next_month:02d}_AP.parquet"
    next_ap.write_parquet(ap_file, compression="zstd", row_group_size=ROW_GROUP_SIZE)
    print(f"[freeze] AP written: {ap_file} ({ap_file.stat().st_size / 1024**2:.1f} MB)")

    validate(all_cps, next_ap, year, month, pk_columns)

    if not skip_upload:
        upload(s3, cp_file, ap_file, provider, year, month, next_year, next_month)
    else:
        print("[freeze] Upload skipped (--skip-upload)")

    print("\n[freeze] Done!")
    return FreezeResult(
        provider=provider,
        year=year,
        month=month,
        next_year=next_year,
        next_month=next_month,
        cp_file=cp_file,
        ap_file=ap_file,
        cp_rows=all_cps.height,
        ap_rows=next_ap.height,
    )


def resolve_control_flags(
    *,
    force: bool,
    ignore_completeness: bool,
    overwrite_existing: bool,
) -> FreezeControls:
    if force:
        return FreezeControls(ignore_completeness=True, overwrite_existing=True)
    return FreezeControls(
        ignore_completeness=ignore_completeness,
        overwrite_existing=overwrite_existing,
    )


def reorder_columns(df: pl.DataFrame, pk_columns: list[str]) -> pl.DataFrame:
    ordered = pk_columns + ["Time"] + [c for c in df.columns if c not in pk_columns and c != "Time"]
    return df.select(ordered)


def _storage_options(profile: str | None) -> dict:
    opts = {"aws_region": "us-west-2"}
    if profile:
        opts["aws_profile"] = profile
    return opts


def load_manifest(s3, warm_prefix: str, *, required: bool = True) -> dict | None:
    key = f"{warm_prefix}/manifest.json"
    print(f"[freeze] Loading manifest: s3://{BUCKET}/{key}")
    try:
        response = s3.get_object(Bucket=BUCKET, Key=key)
        manifest = json.loads(response["Body"].read())
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "NoSuchKey":
            if required:
                raise FreezeError(f"Manifest not found: s3://{BUCKET}/{key}") from exc
            print(f"[freeze] Manifest not found: s3://{BUCKET}/{key} (hot-only snapshot)")
            return None
        raise

    total_files = sum(len(files) for files in manifest.get("levels", {}).values())
    print(f"[freeze] Manifest: {total_files} files across {len(manifest.get('levels', {}))} levels")
    print(f"[freeze] last_processed_time: {manifest.get('last_processed_time', 'N/A')}")
    return manifest


def completeness_threshold(year: int, month: int) -> datetime:
    last_day = calendar.monthrange(year, month)[1]
    return datetime(year, month, last_day, 23, 50, tzinfo=timezone.utc)


def is_complete(manifest: dict, year: int, month: int) -> tuple[bool, str | None]:
    lpt_str = manifest.get("last_processed_time")
    if not lpt_str:
        return False, None

    lpt = datetime.fromisoformat(lpt_str)
    threshold = completeness_threshold(year, month)
    return lpt >= threshold, lpt_str


def check_completeness(
    manifest: dict,
    year: int,
    month: int,
    *,
    ignore_completeness: bool,
):
    complete, lpt_str = is_complete(manifest, year, month)
    threshold = completeness_threshold(year, month)

    if complete:
        print(f"[freeze] Completeness check passed: {lpt_str}")
        return

    if not lpt_str:
        if ignore_completeness:
            print("[WARN] No last_processed_time in manifest (--ignore-completeness, continuing)")
            return
        raise FreezeError("No last_processed_time in manifest. Month may be empty.")

    if ignore_completeness:
        print(
            "[WARN] Month incomplete: "
            f"last_processed_time={lpt_str} < {threshold} (--ignore-completeness)"
        )
        return
    raise FreezeError(
        "Month appears incomplete: "
        f"last_processed_time={lpt_str}, threshold={threshold}"
    )


def _read_manifest_quiet(s3, warm_prefix: str) -> dict | None:
    key = f"{warm_prefix}/manifest.json"
    try:
        response = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(response["Body"].read())
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise


def watch_until_complete(s3, warm_prefix: str, year: int, month: int, interval: int) -> dict:
    threshold = completeness_threshold(year, month)
    print(f"[watch] Waiting for last_processed_time >= {threshold}")
    print(f"[watch] Polling every {interval}s. Ctrl+C to abort.\n")

    while True:
        manifest = _read_manifest_quiet(s3, warm_prefix)
        if manifest:
            complete, lpt_str = is_complete(manifest, year, month)
            if complete:
                print(f"[watch] Month complete! last_processed_time={lpt_str}")
                return manifest
        else:
            lpt_str = None

        now = datetime.now(timezone.utc).strftime("%H:%M:%S")
        print(f"[watch] {now} - last={lpt_str or 'N/A'}, sleeping {interval}s...")
        time.sleep(interval)


def check_not_already_uploaded(
    s3,
    provider: str,
    year: int,
    month: int,
    next_year: int,
    next_month: int,
    *,
    overwrite_existing: bool,
):
    cp_key = f"{provider}/{year}-{month:02d}.parquet"
    ap_key = f"{provider}/{next_year}-{next_month:02d}_AP.parquet"

    existing = []
    for key in [cp_key, ap_key]:
        try:
            s3.head_object(Bucket=BUCKET, Key=key)
            existing.append(key)
        except ClientError:
            pass

    if not existing:
        return

    msg = ", ".join(f"s3://{BUCKET}/{key}" for key in existing)
    if overwrite_existing:
        print(f"[WARN] Cold files already exist (--overwrite-existing, will overwrite): {msg}")
        return
    raise FreezeError(f"Cold files already exist on S3: {msg}")


def iter_s3_keys(s3, bucket: str, prefix: str):
    continuation = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if continuation:
            kwargs["ContinuationToken"] = continuation
        response = s3.list_objects_v2(**kwargs)
        for item in response.get("Contents", []):
            yield item["Key"]
        if not response.get("IsTruncated"):
            break
        continuation = response.get("NextContinuationToken")


def month_hot_prefix(provider: str, year: int, month: int, env: str) -> str:
    env_prefix = "test/" if env == "test" else ""
    return f"{env_prefix}parquet_cp_hot/{provider}/{year}/{month:02d}/"


def parse_hot_key_timestamp(key: str) -> datetime | None:
    path = PurePosixPath(key)
    try:
        year = int(path.parts[-4])
        month = int(path.parts[-3])
        day = int(path.parts[-2])
    except (ValueError, IndexError):
        return None

    stem = path.stem
    try:
        if stem.startswith("slot_"):
            hhmm = stem.split("_", 2)[1]
            hour = int(hhmm[:2])
            minute = int(hhmm[2:4])
        else:
            hour_str, minute_str = stem.split("-", 1)
            hour = int(hour_str)
            minute = int(minute_str)
    except (IndexError, ValueError):
        return None

    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


def list_month_hot_keys(
    s3,
    provider: str,
    year: int,
    month: int,
    env: str,
) -> list[tuple[str, datetime]]:
    prefix = month_hot_prefix(provider, year, month, env)
    entries = []
    for key in iter_s3_keys(s3, BUCKET, prefix):
        ts = parse_hot_key_timestamp(key)
        if ts is not None:
            entries.append((key, ts))
    entries.sort(key=lambda item: (item[1], item[0]))
    return entries


def _manifest_last_processed_time(manifest: dict) -> datetime:
    lpt_str = manifest.get("last_processed_time")
    if not lpt_str:
        raise FreezeError(
            "Manifest is missing last_processed_time. Cannot determine hot tail boundary."
        )
    return datetime.fromisoformat(lpt_str)


def enumerate_input_files(
    s3,
    manifest: dict | None,
    warm_prefix: str,
    provider: str,
    year: int,
    month: int,
    env: str,
) -> list[str]:
    uris: list[str] = []
    manifest_hot_keys: set[str] = set()
    manifest_lpt: datetime | None = None

    if manifest is not None:
        manifest_lpt = _manifest_last_processed_time(manifest)
        print("[freeze] Files by level:")
        for level_str in sorted(manifest.get("levels", {}).keys()):
            files = manifest["levels"][level_str]
            print(f"  L{level_str}: {len(files)} files")
            level = int(level_str)
            for file_entry in files:
                filename = file_entry["file"]
                if level == 0:
                    manifest_hot_keys.add(filename)
                    uris.append(f"s3://{BUCKET}/{filename}")
                else:
                    uris.append(f"s3://{BUCKET}/{warm_prefix}/{filename}")
    else:
        print("[freeze] No manifest snapshot available; using hot-only month snapshot")

    tail_hot_uris = []
    for key, ts in list_month_hot_keys(s3, provider, year, month, env):
        if key in manifest_hot_keys:
            continue
        if manifest_lpt is not None and ts <= manifest_lpt:
            continue
        tail_hot_uris.append(f"s3://{BUCKET}/{key}")

    if manifest_lpt is not None:
        print(
            "[freeze] Tail hot overlay: "
            f"{len(tail_hot_uris)} files newer than manifest last_processed_time "
            f"({manifest_lpt.isoformat()})"
        )
    else:
        print(f"[freeze] Hot-only snapshot files: {len(tail_hot_uris)}")

    return uris + tail_hot_uris


def read_all_cps(s3_uris: list[str], profile: str | None = None) -> pl.DataFrame:
    print(f"[freeze] Reading {len(s3_uris)} files from S3...")
    opts = _storage_options(profile)
    lazy_frames = [pl.scan_parquet(uri, storage_options=opts) for uri in s3_uris]
    return pl.concat(lazy_frames, how="diagonal").collect(engine="streaming")


def validate(cp_df: pl.DataFrame, ap_df: pl.DataFrame, year: int, month: int, pk_columns: list[str]):
    print("\n[validate] Running integrity checks...")
    errors = []

    next_year, next_month = (year + 1, 1) if month == 12 else (year, month + 1)
    month_start = datetime(year, month, 1, tzinfo=timezone.utc)
    next_month_start = datetime(next_year, next_month, 1, tzinfo=timezone.utc)

    if cp_df.height > 0:
        cp_min_time = cp_df["Time"].min()
        cp_max_time = cp_df["Time"].max()
        if cp_min_time < month_start:
            errors.append(f"CP has rows before month start: {cp_min_time}")
        if cp_max_time >= next_month_start:
            errors.append(f"CP has rows at/after next month: {cp_max_time}")

    unique_pks = ap_df.select(pk_columns).unique().height
    if ap_df.height != unique_pks:
        errors.append(f"AP PK not unique: {ap_df.height} rows, {unique_pks} unique PKs")

    time_dtype = cp_df.schema.get("Time")
    if time_dtype != pl.Datetime("us", "UTC"):
        errors.append(f"CP Time dtype: {time_dtype} (expected Datetime(us, UTC))")

    sort_cols = pk_columns + ["Time"]
    if cp_df.height > 0:
        sorted_cp = cp_df.sort(sort_cols)
        if not cp_df.equals(sorted_cp):
            errors.append("CP not sorted by PK+Time")

    if cp_df.height > 0:
        pk_time_cols = pk_columns + ["Time"]
        unique_pk_time = cp_df.select(pk_time_cols).unique().height
        if cp_df.height != unique_pk_time:
            dup_count = cp_df.height - unique_pk_time
            errors.append(f"CP has {dup_count} PK+Time duplicates")

    if errors:
        joined = "\n".join(f"  - {err}" for err in errors)
        raise FreezeError(f"Validation failed:\n{joined}")

    print("[validate] All checks passed")
    if cp_df.height > 0:
        print(f"  CP: {cp_df.height:,} rows, Time range: {cp_df['Time'].min()} ~ {cp_df['Time'].max()}")
    else:
        print("  CP: 0 rows")
    print(f"  AP: {ap_df.height:,} rows ({unique_pks:,} unique PKs)")


def upload(
    s3,
    cp_file: Path,
    ap_file: Path,
    provider: str,
    year: int,
    month: int,
    next_year: int,
    next_month: int,
):
    cp_key = f"{provider}/{year}-{month:02d}.parquet"
    ap_key = f"{provider}/{next_year}-{next_month:02d}_AP.parquet"

    print(f"[upload] Uploading CP -> s3://{BUCKET}/{cp_key}")
    s3.upload_file(str(cp_file), BUCKET, cp_key)

    print(f"[upload] Uploading AP -> s3://{BUCKET}/{ap_key}")
    s3.upload_file(str(ap_file), BUCKET, ap_key)

    print("[upload] Done!")


if __name__ == "__main__":
    main()
