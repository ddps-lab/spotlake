"""Tests for adaptive Azure partition planning."""
from __future__ import annotations

import os
from datetime import datetime, timezone

import polars as pl
import pytest

os.environ["TITANS_ENV"] = "test"

from titans_common.partitioned_eager_merge import (
    OversizedPartitionError,
    materialize_azure_partitions,
    merge_partition,
)


def _azure_row(instance_type: str, region: str, az: str, minute: int) -> dict:
    return {
        "InstanceTier": "Standard",
        "InstanceType": instance_type,
        "Region": region,
        "AZ": az,
        "Time": datetime(2026, 1, 1, 0, minute, tzinfo=timezone.utc),
        "DesiredCount": 1.0,
        "Score": 3,
        "T3": 1,
        "T2": 1,
        "IF": 1.0,
        "OndemandPrice": 1.0,
        "SpotPrice": 0.5,
        "Savings": 0.5,
        "Ceased": False,
    }


def _write_input(path, rows: list[dict]) -> None:
    df = pl.DataFrame(rows).with_columns([
        pl.col("Time").cast(pl.Datetime("us", "UTC")),
        pl.col("DesiredCount").cast(pl.Float64),
        pl.col("Score").cast(pl.Int64),
        pl.col("T3").cast(pl.Int64),
        pl.col("T2").cast(pl.Int64),
        pl.col("IF").cast(pl.Float64),
        pl.col("OndemandPrice").cast(pl.Float64),
        pl.col("SpotPrice").cast(pl.Float64),
        pl.col("Savings").cast(pl.Float64),
        pl.col("Ceased").cast(pl.Boolean),
    ]).sort(["InstanceTier", "InstanceType", "Region", "AZ", "Time"])
    df.write_parquet(path, compression="zstd")


def test_materialize_azure_partitions_recursively_refines_instance_type_prefixes(tmp_path):
    input_path = tmp_path / "input.parquet"
    _write_input(
        input_path,
        [
            _azure_row("B1ms", "eastus", "az1", 0),
            _azure_row("B1ms", "eastus", "az1", 10),
            _azure_row("B2ms", "eastus", "az1", 0),
            _azure_row("B2ms", "eastus", "az1", 10),
            _azure_row("B2s", "westus", "az2", 0),
            _azure_row("B2s", "westus", "az2", 10),
        ],
    )

    partitions = materialize_azure_partitions(
        [input_path],
        tmp_path / "parts",
        prefix_len=2,
        split_prefixes=set(),
        default_prefix_len=1,
        max_rows_per_partition=3,
        max_prefix_len=8,
    )

    assert [part.key for part in partitions] == [
        "Standard:B1",
        "Standard:B2m",
        "Standard:B2s",
    ]
    assert max(part.row_count for part in partitions) <= 3


def test_materialize_azure_partitions_falls_back_to_region_after_exact_type(tmp_path):
    input_path = tmp_path / "input.parquet"
    _write_input(
        input_path,
        [
            _azure_row("B1ms", "eastus", "az1", 0),
            _azure_row("B1ms", "eastus", "az1", 10),
            _azure_row("B1ms", "westus", "az2", 0),
            _azure_row("B1ms", "westus", "az2", 10),
        ],
    )

    partitions = materialize_azure_partitions(
        [input_path],
        tmp_path / "parts",
        prefix_len=2,
        split_prefixes=set(),
        default_prefix_len=1,
        max_rows_per_partition=2,
        max_prefix_len=8,
    )

    assert [part.key for part in partitions] == [
        "Standard:B1:eastus",
        "Standard:B1:westus",
    ]
    assert max(part.row_count for part in partitions) <= 2


def test_materialize_azure_partitions_raises_when_full_pk_is_still_oversized(tmp_path):
    input_path = tmp_path / "input.parquet"
    _write_input(
        input_path,
        [
            _azure_row("B1ms", "eastus", "az1", 0),
            _azure_row("B1ms", "eastus", "az1", 10),
        ],
    )

    with pytest.raises(OversizedPartitionError, match="Unable to refine Azure partition"):
        materialize_azure_partitions(
            [input_path],
            tmp_path / "parts",
            prefix_len=2,
            split_prefixes=set(),
            default_prefix_len=1,
            max_rows_per_partition=1,
            max_prefix_len=8,
        )


def test_merge_partition_streaming_matches_eager(tmp_path):
    partition_dir = tmp_path / "partition"
    partition_dir.mkdir()
    rows_a = [
        _azure_row("B1ms", "eastus", "az1", 0),
        {
            **_azure_row("B1ms", "eastus", "az1", 10),
            "Ceased": True,
        },
        _azure_row("D2s", "westus", "az2", 0),
    ]
    rows_b = [
        _azure_row("B1ms", "eastus", "az1", 10),
        _azure_row("B1ms", "eastus", "az1", 20),
        _azure_row("D2s", "westus", "az2", 10),
    ]
    _write_input(partition_dir / "a.parquet", rows_a)
    _write_input(partition_dir / "b.parquet", rows_b)

    eager_out = tmp_path / "eager.parquet"
    streaming_out = tmp_path / "streaming.parquet"

    eager_rows, _ = merge_partition(partition_dir, eager_out, strategy="eager")
    streaming_rows, _ = merge_partition(partition_dir, streaming_out, strategy="streaming")

    eager_df = pl.read_parquet(eager_out)
    streaming_df = pl.read_parquet(streaming_out)

    assert eager_rows == streaming_rows == 5
    assert eager_df.equals(streaming_df)
