"""Cleanup orphan files from TITANS Hot/Warm tiers.

Orphan files are S3 objects that exist on disk but are NOT referenced
in the current manifest. They accumulate when deferred deletions fail
(e.g., missing s3:DeleteObject IAM permission).

Usage:
    # Dry run (default) - shows what would be deleted
    uv run python -m titans_common.cleanup_orphans --year 2026 --month 2

    # Actually delete orphans
    uv run python -m titans_common.cleanup_orphans --year 2026 --month 2 --execute

    # Specific provider
    uv run python -m titans_common.cleanup_orphans --year 2026 --month 2 --provider aws

    # Production environment
    TITANS_ENV=production uv run python -m titans_common.cleanup_orphans --year 2026 --month 2
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

import boto3

from .config import get_config, is_test_env


def collect_active_keys(manifest: dict, warm_prefix: str) -> set[str]:
    """Extract all active S3 keys from manifest.

    Returns:
        Set of full S3 keys that should NOT be deleted.
    """
    active = set()

    for level_str, files in manifest.get("levels", {}).items():
        level = int(level_str)
        for f in files:
            if level == 0:
                # L0 (hot): filename is already a full S3 key
                active.add(f["file"])
            else:
                # L1+: filename is relative, prepend warm_prefix
                active.add(f"{warm_prefix}/{f['file']}")

    # pending_deletions are scheduled for deletion but not yet deleted.
    # Do NOT protect them - they should be deleted.

    return active


def list_s3_keys(s3_client, bucket: str, prefix: str) -> list[dict]:
    """List all S3 objects under a prefix. Returns list of {Key, Size}."""
    objects = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            objects.append({"Key": obj["Key"], "Size": obj["Size"]})
    return objects


def _parse_time_from_key(hot_s3_key: str) -> datetime | None:
    """Parse timestamp from hot file path.

    e.g., 'test/parquet_cp_hot/aws/2026/01/23/14-20.parquet'
        → datetime(2026, 1, 23, 14, 20, tzinfo=UTC)
    """
    try:
        parts = hot_s3_key.removesuffix(".parquet").split("/")
        year = int(parts[-4])
        month = int(parts[-3])
        day = int(parts[-2])
        hour, minute = map(int, parts[-1].split("-"))
        return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
    except (ValueError, IndexError):
        return None


def find_orphans(
    s3_client,
    bucket: str,
    hot_prefix: str,
    warm_prefix: str,
    year: int,
    month: int,
    manifest: dict,
) -> tuple[list[dict], list[dict], int]:
    """Find orphan files in Hot and Warm tiers.

    Safety: Hot files newer than last_processed_time are NEVER considered
    orphans, because they may have been uploaded but not yet compacted
    into the manifest (race condition protection).

    Returns:
        (hot_orphans, warm_orphans, skipped_hot_count)
    """
    active_keys = collect_active_keys(manifest, f"{warm_prefix}/{year}/{month:02d}")

    # Safety cutoff: only consider hot files at or before last_processed_time
    lpt_str = manifest.get("last_processed_time")
    last_processed_time = None
    if lpt_str:
        try:
            last_processed_time = datetime.fromisoformat(lpt_str)
        except (ValueError, TypeError):
            pass

    # 1. Hot tier orphans: list all hot files for this month
    hot_month_prefix = f"{hot_prefix}/{year}/{month:02d}/"
    hot_files = list_s3_keys(s3_client, bucket, hot_month_prefix)

    hot_orphans = []
    skipped_hot = 0
    for f in hot_files:
        if f["Key"] in active_keys:
            continue  # Active in manifest, not an orphan

        # Safety guard: protect files newer than last_processed_time
        file_time = _parse_time_from_key(f["Key"])
        if file_time is None:
            # Can't parse time → skip to be safe
            skipped_hot += 1
            continue
        if last_processed_time is None:
            # No last_processed_time in manifest → skip all hot files to be safe
            skipped_hot += 1
            continue
        if file_time > last_processed_time:
            # Newer than what manifest knows about → might not be orphan
            skipped_hot += 1
            continue

        hot_orphans.append(f)

    # 2. Warm tier orphans: list all warm files for this month
    warm_month_prefix = f"{warm_prefix}/{year}/{month:02d}/"
    warm_files = list_s3_keys(s3_client, bucket, warm_month_prefix)
    warm_orphans = [
        f for f in warm_files
        if f["Key"] not in active_keys
        and not f["Key"].endswith("manifest.json")
    ]

    return hot_orphans, warm_orphans, skipped_hot


def delete_keys(s3_client, bucket: str, keys: list[str]) -> tuple[int, int]:
    """Batch delete S3 keys. Returns (success_count, fail_count)."""
    success = 0
    fail = 0
    # S3 delete_objects supports up to 1000 keys per call
    for i in range(0, len(keys), 1000):
        batch = keys[i:i + 1000]
        response = s3_client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True},
        )
        errors = response.get("Errors", [])
        fail += len(errors)
        success += len(batch) - len(errors)
        for err in errors:
            print(f"  FAILED: {err['Key']} - {err['Code']}: {err['Message']}")
    return success, fail


def format_size(size_bytes: int) -> str:
    """Format bytes to human-readable string."""
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} TB"


def main():
    parser = argparse.ArgumentParser(
        description="Cleanup orphan files from TITANS Hot/Warm tiers."
    )
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--provider", default="aws", choices=["aws", "azure", "gcp"])
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually delete orphans. Default is dry-run.",
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="AWS profile name (e.g., spotrank). Uses default credentials if not set.",
    )
    args = parser.parse_args()

    env = "test" if is_test_env() else "production"
    config = get_config(args.provider)

    session = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
    s3_client = session.client("s3")

    print(f"=== TITANS Orphan Cleanup ===")
    print(f"Environment : {env}")
    print(f"Provider    : {args.provider}")
    print(f"Period      : {args.year}/{args.month:02d}")
    print(f"Bucket      : {config.titans_bucket}")
    print(f"Hot prefix  : {config.hot_prefix}")
    print(f"Warm prefix : {config.warm_prefix}")
    print(f"Mode        : {'EXECUTE' if args.execute else 'DRY RUN'}")
    print()

    # Load manifest
    manifest_key = f"{config.warm_prefix}/{args.year}/{args.month:02d}/manifest.json"
    try:
        response = s3_client.get_object(Bucket=config.titans_bucket, Key=manifest_key)
        manifest = json.loads(response["Body"].read())
        print(f"Manifest loaded: {manifest_key}")
        print(f"  last_hot_idx       : {manifest.get('last_hot_idx', -1)}")
        print(f"  last_processed_time: {manifest.get('last_processed_time', 'N/A')}")
        print(f"  pending_deletions  : {len(manifest.get('pending_deletions', []))}")
        levels_summary = {
            f"L{k}": len(v) for k, v in manifest.get("levels", {}).items()
        }
        print(f"  active files       : {levels_summary}")
    except s3_client.exceptions.NoSuchKey:
        print(f"ERROR: Manifest not found at {manifest_key}")
        print("Cannot determine active files without a manifest. Aborting.")
        sys.exit(1)
    print()

    # Find orphans
    hot_orphans, warm_orphans, skipped_hot = find_orphans(
        s3_client,
        config.titans_bucket,
        config.hot_prefix,
        config.warm_prefix,
        args.year,
        args.month,
        manifest,
    )

    hot_size = sum(f["Size"] for f in hot_orphans)
    warm_size = sum(f["Size"] for f in warm_orphans)

    print(f"Hot tier orphans  : {len(hot_orphans):,} files ({format_size(hot_size)})")
    print(f"Warm tier orphans : {len(warm_orphans):,} files ({format_size(warm_size)})")
    print(f"Total             : {len(hot_orphans) + len(warm_orphans):,} files ({format_size(hot_size + warm_size)})")
    if skipped_hot:
        print(f"Skipped (safety)  : {skipped_hot:,} hot files newer than last_processed_time")
    print()

    if not hot_orphans and not warm_orphans:
        print("No orphans found. Nothing to do.")
        return

    if not args.execute:
        # Show sample of orphans
        if hot_orphans:
            print("Sample hot orphans (first 5):")
            for f in hot_orphans[:5]:
                print(f"  {f['Key']}  ({format_size(f['Size'])})")
            if len(hot_orphans) > 5:
                print(f"  ... and {len(hot_orphans) - 5} more")
        if warm_orphans:
            print("Sample warm orphans (first 5):")
            for f in warm_orphans[:5]:
                print(f"  {f['Key']}  ({format_size(f['Size'])})")
            if len(warm_orphans) > 5:
                print(f"  ... and {len(warm_orphans) - 5} more")
        print()
        print("Re-run with --execute to delete these files.")
        return

    # Execute deletion
    print("Deleting orphan files...")
    all_orphan_keys = [f["Key"] for f in hot_orphans + warm_orphans]
    success, fail = delete_keys(s3_client, config.titans_bucket, all_orphan_keys)
    print(f"Done: {success:,} deleted, {fail:,} failed")


if __name__ == "__main__":
    main()
