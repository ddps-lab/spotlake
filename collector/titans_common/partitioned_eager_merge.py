"""Range-partitioned eager merge helpers for warm compaction."""
from __future__ import annotations

import argparse
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl


def build_partition_key(instance_tier: str, instance_type: str, prefix_len: int = 2) -> str:
    """Build a lexicographic range-partition key for Azure warm compaction."""
    return f"{instance_tier}:{instance_type[:prefix_len]}"


def build_partition_key_adaptive(
    instance_tier: str,
    instance_type: str,
    *,
    default_prefix_len: int,
    deep_prefix_len: int,
    split_prefixes: set[str],
) -> str:
    """Build a partition key with selective deeper splitting for large prefixes."""
    prefix_len = default_prefix_len
    if instance_type[:default_prefix_len] in split_prefixes:
        prefix_len = deep_prefix_len
    return f"{instance_tier}:{instance_type[:prefix_len]}"


def build_output_schema(sample_file: Path, provider: str = "azure") -> pa.Schema:
    """Build normalized Arrow schema for materialized partitions."""
    schema = pq.ParquetFile(sample_file).schema_arrow
    if provider != "azure":
        return schema

    fields = []
    for field in schema:
        if field.name == "Score":
            fields.append(pa.field("Score", pa.int64()))
        else:
            fields.append(field)
    return pa.schema(fields)


def materialize_azure_partitions(
    input_paths: list[Path],
    temp_dir: Path,
    *,
    prefix_len: int = 2,
    split_prefixes: set[str] | None = None,
    default_prefix_len: int = 1,
) -> list[str]:
    """Materialize range partitions from sorted Azure parquet inputs."""
    temp_dir.mkdir(parents=True, exist_ok=True)
    all_partitions: set[str] = set()
    split_prefixes = split_prefixes or set()
    if not split_prefixes:
        default_prefix_len = prefix_len

    for src in sorted(input_paths):
        pf = pq.ParquetFile(src)
        schema = build_output_schema(src, provider="azure")
        writers: dict[str, pq.ParquetWriter] = {}

        try:
            for batch in pf.iter_batches(batch_size=65536):
                batch = batch.cast(schema)
                table = pa.Table.from_batches([batch])
                tiers = table["InstanceTier"].to_pylist()
                types = table["InstanceType"].to_pylist()
                if not tiers:
                    continue

                start = 0
                current_part = build_partition_key_adaptive(
                    str(tiers[0]),
                    str(types[0]),
                    default_prefix_len=default_prefix_len,
                    deep_prefix_len=prefix_len,
                    split_prefixes=split_prefixes,
                )
                for idx in range(1, len(tiers)):
                    next_part = build_partition_key_adaptive(
                        str(tiers[idx]),
                        str(types[idx]),
                        default_prefix_len=default_prefix_len,
                        deep_prefix_len=prefix_len,
                        split_prefixes=split_prefixes,
                    )
                    if next_part == current_part:
                        continue

                    sliced = table.slice(start, idx - start)
                    safe_part = current_part.replace(":", "__")
                    part_dir = temp_dir / safe_part
                    part_dir.mkdir(parents=True, exist_ok=True)
                    out_path = part_dir / src.name

                    if current_part not in writers:
                        writers[current_part] = pq.ParquetWriter(out_path, schema, compression="zstd")
                    writers[current_part].write_table(sliced)
                    all_partitions.add(current_part)

                    start = idx
                    current_part = next_part

                sliced = table.slice(start, len(tiers) - start)
                safe_part = current_part.replace(":", "__")
                part_dir = temp_dir / safe_part
                part_dir.mkdir(parents=True, exist_ok=True)
                out_path = part_dir / src.name

                if current_part not in writers:
                    writers[current_part] = pq.ParquetWriter(out_path, schema, compression="zstd")
                writers[current_part].write_table(sliced)
                all_partitions.add(current_part)
        finally:
            for writer in writers.values():
                writer.close()

    return sorted(all_partitions)


def eager_merge_partition(partition_dir: Path, output_path: Path) -> tuple[int, int]:
    """Merge one materialized partition with Polars eager path."""
    schema = {
        "InstanceTier": pl.Utf8,
        "InstanceType": pl.Utf8,
        "Region": pl.Utf8,
        "AZ": pl.Utf8,
        "Time": pl.Datetime("us", "UTC"),
        "DesiredCount": pl.Float64,
        "Score": pl.Int64,
        "T3": pl.Int64,
        "T2": pl.Int64,
        "IF": pl.Float64,
        "OndemandPrice": pl.Float64,
        "SpotPrice": pl.Float64,
        "Savings": pl.Float64,
        "Ceased": pl.Boolean,
    }

    dfs = []
    for path in sorted(partition_dir.glob("*.parquet")):
        df = pl.read_parquet(path)
        cast_exprs = [pl.col(c).cast(schema[c], strict=False) for c in df.columns if c in schema]
        if cast_exprs:
            df = df.with_columns(cast_exprs)
        dfs.append(df)

    combined = pl.concat(dfs, how="diagonal").sort(["InstanceTier", "InstanceType", "Region", "AZ", "Time"])
    combined = (
        combined
        .sort(["InstanceTier", "InstanceType", "Region", "AZ", "Time", "Ceased"])
        .unique(subset=["InstanceTier", "InstanceType", "Region", "AZ", "Time"], keep="first")
        .sort(["InstanceTier", "InstanceType", "Region", "AZ", "Time"])
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    combined.write_parquet(output_path, compression="zstd")
    return combined.height, output_path.stat().st_size


def concat_partition_outputs(partition_outputs: list[Path], final_output: Path) -> tuple[int, int]:
    """Concatenate already-sorted partition outputs in partition-key order."""
    if not partition_outputs:
        raise ValueError("partition_outputs must not be empty")

    writer: pq.ParquetWriter | None = None
    total_rows = 0
    try:
        for path in partition_outputs:
            pf = pq.ParquetFile(path)
            if writer is None:
                writer = pq.ParquetWriter(final_output, pf.schema_arrow, compression="zstd")
            total_rows += pf.metadata.num_rows
            for batch in pf.iter_batches(batch_size=65536):
                writer.write_batch(batch)
    finally:
        if writer is not None:
            writer.close()

    return total_rows, final_output.stat().st_size


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--partition-dir", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    rows, size = eager_merge_partition(Path(args.partition_dir), Path(args.output))
    print(f"rows={rows}")
    print(f"size={size}")


if __name__ == "__main__":
    main()
