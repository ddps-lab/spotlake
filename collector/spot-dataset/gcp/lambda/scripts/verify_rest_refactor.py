from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

LAMBDA_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(LAMBDA_DIR))

from collector_core import (
    MACHINE_TYPE_SCHEMA,
    SNAPSHOT_SCHEMA,
    build_snapshot_df,
    fetch_billing_catalog,
    get_access_token,
    list_regions_and_machine_types_rest,
    list_regions_and_machine_types_sdk,
    load_service_account_info,
    service_account_path,
)


def parse_timestamp(value: str | None) -> datetime:
    if not value:
        return datetime.now(timezone.utc).replace(second=0, microsecond=0)
    if value.endswith("Z"):
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    parsed = datetime.fromisoformat(value)
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def rows_to_frame(rows: list[dict], schema: dict[str, pl.DataType]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema=schema)
    df = pl.from_dicts(rows)
    expressions = []
    for column, dtype in schema.items():
        if column in df.columns:
            expressions.append(pl.col(column).cast(dtype, strict=False))
        else:
            expressions.append(pl.lit(None, dtype=dtype).alias(column))
    return df.with_columns(expressions).select(list(schema.keys()))


def normalize_machine_types(rows: list[dict]) -> pl.DataFrame:
    return rows_to_frame(rows, MACHINE_TYPE_SCHEMA).sort(["region", "machineType"])


def normalize_snapshot(df: pl.DataFrame) -> pl.DataFrame:
    expressions = []
    for column, dtype in SNAPSHOT_SCHEMA.items():
        if column in df.columns:
            expressions.append(pl.col(column).cast(dtype, strict=False))
        else:
            expressions.append(pl.lit(None, dtype=dtype).alias(column))
    return (
        df.with_columns(expressions)
        .select(list(SNAPSHOT_SCHEMA.keys()))
        .sort(["InstanceType", "Region"])
    )


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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--timestamp",
        help="UTC timestamp, e.g. 2026-03-16T10:20:00Z. Defaults to current UTC minute.",
    )
    args = parser.parse_args()

    timestamp = parse_timestamp(args.timestamp)
    credential_path = service_account_path()
    project_id = load_service_account_info(credential_path)["project_id"]
    access_token = get_access_token(credential_path)

    sku_infos, gpu_sku_infos, price_infos, gpu_price_infos = fetch_billing_catalog(access_token)
    gpu_families = sorted(
        {
            gpu_info["gpuType"]
            for gpu_info in gpu_sku_infos
            if gpu_info.get("gpuType")
        }
    )

    sdk_machine_types = list_regions_and_machine_types_sdk(
        gpu_families,
        service_account_file=credential_path,
    )
    rest_machine_types = list_regions_and_machine_types_rest(
        gpu_families,
        access_token=access_token,
        project_id=project_id,
    )
    assert_equal(
        "machine type inventory",
        normalize_machine_types(sdk_machine_types),
        normalize_machine_types(rest_machine_types),
    )

    sdk_snapshot = normalize_snapshot(
        build_snapshot_df(
            sku_infos,
            gpu_sku_infos,
            price_infos,
            gpu_price_infos,
            sdk_machine_types,
            timestamp=timestamp,
        )
    )
    rest_snapshot = normalize_snapshot(
        build_snapshot_df(
            sku_infos,
            gpu_sku_infos,
            price_infos,
            gpu_price_infos,
            rest_machine_types,
            timestamp=timestamp,
        )
    )
    assert_equal("final collector snapshot", sdk_snapshot, rest_snapshot)
    print("[OK] REST refactor verification completed")


if __name__ == "__main__":
    main()
