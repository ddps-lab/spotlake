"""Retroactive cleanup: remove -1 sentinel rows from existing Warm/Hot files.

Sentinel values (-1) come from fillna(-1) in merge_data.py for missing data.
They create false change points (e.g., 10 → -1 → 10) that inflate storage
and pollute query results. New data is filtered at upload time, but files
created before the filter was deployed still contain these rows.

This script rewrites each active file IN-PLACE (same S3 key) to remove
sentinel rows. This is safe because:
  - S3 PUT is atomic (readers see complete old or new version)
  - File names don't change, so no manifest update needed
  - Collector only creates new keys, never overwrites existing ones

Usage:
    # Dry run (default) - shows what would be cleaned
    TITANS_ENV=test uv run python -m titans_common.clean_sentinel_data \
        --year 2026 --month 2 --profile spotrank

    # Actually rewrite files
    TITANS_ENV=test uv run python -m titans_common.clean_sentinel_data \
        --year 2026 --month 2 --profile spotrank --execute
"""
from __future__ import annotations

import argparse
import io
import json
import sys

import boto3
import polars as pl

from .config import get_config, is_test_env


def clean_file(
    s3_client,
    bucket: str,
    key: str,
    value_columns: list[str],
    execute: bool,
    sentinel: int = -1,
) -> tuple[int, int]:
    """Remove sentinel rows from a single S3 Parquet file.

    Returns:
        (original_rows, removed_rows)
    """
    response = s3_client.get_object(Bucket=bucket, Key=key)
    df = pl.read_parquet(io.BytesIO(response["Body"].read()))

    original = df.height

    cols = [c for c in value_columns if c in df.columns]
    if not cols:
        return original, 0

    mask = pl.lit(False)
    for c in cols:
        mask = mask | (pl.col(c) == sentinel)
    cleaned = df.filter(~mask)

    removed = original - cleaned.height
    if removed == 0:
        return original, 0

    if execute:
        buf = io.BytesIO()
        cleaned.write_parquet(buf, compression="zstd")
        buf.seek(0)
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=buf.getvalue(),
            ContentType="application/octet-stream",
        )

    return original, removed


def main():
    parser = argparse.ArgumentParser(
        description="Remove -1 sentinel rows from existing TITANS Warm/Hot files."
    )
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--provider", default="aws", choices=["aws", "azure", "gcp"])
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually rewrite files. Default is dry-run.",
    )
    parser.add_argument("--profile", default=None)
    args = parser.parse_args()

    env = "test" if is_test_env() else "production"
    config = get_config(args.provider)

    session = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
    s3_client = session.client("s3")

    print(f"=== TITANS Sentinel Data Cleanup ===")
    print(f"Environment : {env}")
    print(f"Provider    : {args.provider}")
    print(f"Period      : {args.year}/{args.month:02d}")
    print(f"Bucket      : {config.titans_bucket}")
    print(f"Mode        : {'EXECUTE' if args.execute else 'DRY RUN'}")
    print(f"Value cols  : {config.value_columns}")
    print()

    # Load manifest
    manifest_key = f"{config.warm_prefix}/{args.year}/{args.month:02d}/manifest.json"
    try:
        response = s3_client.get_object(Bucket=config.titans_bucket, Key=manifest_key)
        manifest = json.loads(response["Body"].read())
    except s3_client.exceptions.NoSuchKey:
        print(f"ERROR: Manifest not found at {manifest_key}")
        sys.exit(1)

    # Collect all active file keys
    warm_prefix = f"{config.warm_prefix}/{args.year}/{args.month:02d}"
    files_to_clean = []

    for level_str, files in manifest.get("levels", {}).items():
        level = int(level_str)
        for f in files:
            if level == 0:
                files_to_clean.append({"key": f["file"], "level": 0})
            else:
                files_to_clean.append({"key": f"{warm_prefix}/{f['file']}", "level": level})

    print(f"Active files: {len(files_to_clean)}")
    print()

    # Clean each file
    total_original = 0
    total_removed = 0

    for f in files_to_clean:
        original, removed = clean_file(
            s3_client, config.titans_bucket, f["key"],
            config.value_columns, args.execute,
        )
        total_original += original
        total_removed += removed

        status = f"  {removed:,} rows removed" if removed > 0 else "  clean"
        print(f"  L{f['level']}  {f['key']}  ({original:,} rows){status}")

    print()
    print(f"Total rows scanned : {total_original:,}")
    print(f"Sentinel rows found: {total_removed:,}")
    if total_removed > 0:
        pct = total_removed / total_original * 100
        print(f"Reduction          : {pct:.1f}%")

    if total_removed > 0 and not args.execute:
        print()
        print("Re-run with --execute to rewrite these files.")


if __name__ == "__main__":
    main()
