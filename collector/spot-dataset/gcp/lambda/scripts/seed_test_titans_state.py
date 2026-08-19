from __future__ import annotations

import argparse
import json
from datetime import datetime

import boto3


def prod_hot_prefix(provider: str, year: int, month: int) -> str:
    return f"parquet_cp_hot/{provider}/{year}/{month:02d}/"


def test_hot_prefix(provider: str, year: int, month: int) -> str:
    return f"test/parquet_cp_hot/{provider}/{year}/{month:02d}/"


def prod_warm_prefix(provider: str, year: int, month: int) -> str:
    return f"parquet_warm/{provider}/m8/{year}/{month:02d}/"


def test_warm_prefix(provider: str, year: int, month: int) -> str:
    return f"test/parquet_warm/{provider}/m8/{year}/{month:02d}/"


def list_keys(s3_client, bucket: str, prefix: str) -> list[str]:
    paginator = s3_client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            keys.append(item["Key"])
    return keys


def copy_key(s3_client, src_bucket: str, src_key: str, dst_bucket: str, dst_key: str) -> None:
    s3_client.copy_object(
        Bucket=dst_bucket,
        CopySource={"Bucket": src_bucket, "Key": src_key},
        Key=dst_key,
    )


def rewrite_manifest(manifest: dict, provider: str, year: int, month: int) -> dict:
    src_hot_root = f"parquet_cp_hot/{provider}/"
    dst_hot_root = f"test/parquet_cp_hot/{provider}/"
    src_warm_root = f"parquet_warm/{provider}/m8/{year}/{month:02d}/"
    dst_warm_root = f"test/parquet_warm/{provider}/m8/{year}/{month:02d}/"

    rewritten = json.loads(json.dumps(manifest))

    for files in rewritten.get("levels", {}).values():
        for item in files:
            file_name = item.get("file")
            if isinstance(file_name, str) and file_name.startswith(src_hot_root):
                item["file"] = file_name.replace(src_hot_root, dst_hot_root, 1)

    pending = []
    for key in rewritten.get("pending_deletions", []):
        if key.startswith(src_hot_root):
            pending.append(key.replace(src_hot_root, dst_hot_root, 1))
        elif key.startswith(src_warm_root):
            pending.append(key.replace(src_warm_root, dst_warm_root, 1))
        else:
            pending.append(key)
    rewritten["pending_deletions"] = pending
    return rewritten


def seed_month(
    s3_client,
    *,
    bucket: str,
    provider: str,
    year: int,
    month: int,
    latest_source_bucket: str,
    copy_latest_to_test_bucket: str | None,
) -> None:
    src_warm = prod_warm_prefix(provider, year, month)
    dst_warm = test_warm_prefix(provider, year, month)

    response = s3_client.get_object(Bucket=bucket, Key=f"{src_warm}manifest.json")
    manifest = json.loads(response["Body"].read())
    rewritten_manifest = rewrite_manifest(manifest, provider, year, month)

    copied = 0
    active_hot_keys: set[str] = set()
    active_warm_keys: set[str] = set()
    pending_keys: set[str] = set(rewritten_manifest.get("pending_deletions", []))

    for level, files in rewritten_manifest.get("levels", {}).items():
        for item in files:
            file_name = item["file"]
            if str(level) == "0":
                active_hot_keys.add(file_name)
            else:
                active_warm_keys.add(f"{dst_warm}{file_name}")

    for key in sorted(active_hot_keys | active_warm_keys | pending_keys):
        if key.endswith("manifest.json"):
            continue
        if key.startswith("test/"):
            src_key = key[len("test/"):]
        else:
            src_key = key
        copy_key(s3_client, bucket, src_key, bucket, key)
        copied += 1

    s3_client.put_object(
        Bucket=bucket,
        Key=f"{dst_warm}manifest.json",
        Body=json.dumps(rewritten_manifest, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

    if copy_latest_to_test_bucket:
        latest_key = "latest_data/latest_gcp.json"
        copy_key(
            s3_client,
            latest_source_bucket,
            latest_key,
            copy_latest_to_test_bucket,
            latest_key,
        )

    print(
        f"Seeded {provider} {year}-{month:02d}: copied {copied} TITANS objects "
        f"and wrote {dst_warm}manifest.json"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", required=True, help="YYYY-MM")
    parser.add_argument("--provider", default="gcp")
    parser.add_argument("--titans-bucket", default="titans-spotlake-data")
    parser.add_argument("--latest-source-bucket", default="spotlake")
    parser.add_argument("--copy-latest-to-test-bucket", default="")
    parser.add_argument("--profile", default="")
    args = parser.parse_args()

    year, month = map(int, args.month.split("-"))
    session = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
    s3_client = session.client("s3")

    seed_month(
        s3_client,
        bucket=args.titans_bucket,
        provider=args.provider,
        year=year,
        month=month,
        latest_source_bucket=args.latest_source_bucket,
        copy_latest_to_test_bucket=args.copy_latest_to_test_bucket or None,
    )


if __name__ == "__main__":
    main()
