"""Adaptive range-partitioned eager merge helpers for Azure warm compaction."""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import tempfile
from typing import Callable

import pyarrow as pa
import pyarrow.parquet as pq
import polars as pl

try:
    from .streaming_parquet_merge import merge_sorted_parquet_files
except ImportError:  # pragma: no cover - script entrypoint
    from streaming_parquet_merge import merge_sorted_parquet_files


_MULTIPLE = object()


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


@dataclass(frozen=True)
class AzurePartitionSpec:
    """Current partition refinement state for one materialized Azure partition."""

    type_prefix_len: int
    split_region: bool = False
    split_az: bool = False

    def describe(self) -> str:
        parts = [f"type_prefix={self.type_prefix_len}"]
        if self.split_region:
            parts.append("region")
        if self.split_az:
            parts.append("az")
        return ",".join(parts)


@dataclass(frozen=True)
class MaterializedAzurePartition:
    """One materialized Azure partition leaf."""

    sort_key: tuple[str, ...]
    dir_path: Path
    row_count: int
    spec: AzurePartitionSpec
    instance_type_exact: str | None
    region_exact: str | None
    az_exact: str | None

    @property
    def key(self) -> str:
        return ":".join(self.sort_key)

    @property
    def safe_name(self) -> str:
        return "__".join(_sanitize_key_part(value) for value in self.sort_key)


class OversizedPartitionError(RuntimeError):
    """Raised when an Azure partition cannot be refined under the row budget."""


def _sanitize_key_part(value: str) -> str:
    safe = value.replace("/", "_").replace(":", "_")
    return safe or "_"


def _partition_dir_name(sort_key: tuple[str, ...]) -> str:
    return "__".join(_sanitize_key_part(value) for value in sort_key)


def _extend_exact_value(current: str | object | None, value: str | object) -> str | object:
    if current is None:
        return value
    if current is _MULTIPLE or current != value:
        return _MULTIPLE
    return current


def _exact_value_or_none(value: str | object | None) -> str | None:
    if value is None or value is _MULTIPLE:
        return None
    return value


def _slice_first_last_equal(table: pa.Table, column_name: str) -> str | object:
    column = table[column_name]
    if len(column) == 0:
        return _MULTIPLE
    first = str(column[0].as_py())
    last = str(column[len(column) - 1].as_py())
    if first == last:
        return first
    return _MULTIPLE


def _slice_partition_exactness(table: pa.Table) -> tuple[str | object, str | object, str | object]:
    """Return exactness hints using the fact that rows are globally sort-key ordered."""
    instance_type = _slice_first_last_equal(table, "InstanceType")
    if instance_type is _MULTIPLE:
        return _MULTIPLE, _MULTIPLE, _MULTIPLE

    region = _slice_first_last_equal(table, "Region")
    if region is _MULTIPLE:
        return instance_type, _MULTIPLE, _MULTIPLE

    az = _slice_first_last_equal(table, "AZ")
    if az is _MULTIPLE:
        return instance_type, region, _MULTIPLE

    return instance_type, region, az


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


def _build_key_parts_for_spec(
    instance_tier: str,
    instance_type: str,
    region: str,
    az: str,
    spec: AzurePartitionSpec,
) -> tuple[str, ...]:
    parts = [instance_tier, instance_type[:spec.type_prefix_len]]
    if spec.split_region:
        parts.append(region)
    if spec.split_az:
        parts.append(az)
    return tuple(parts)


def _initial_spec_for_type(
    instance_type: str,
    *,
    prefix_len: int,
    split_prefixes: set[str],
    default_prefix_len: int,
) -> AzurePartitionSpec:
    type_prefix_len = default_prefix_len
    if instance_type[:default_prefix_len] in split_prefixes:
        type_prefix_len = prefix_len
    return AzurePartitionSpec(type_prefix_len=type_prefix_len)


def _write_partition_slice(
    *,
    writers: dict[tuple[str, ...], pq.ParquetWriter],
    table: pa.Table,
    start: int,
    end: int,
    sort_key: tuple[str, ...],
    schema: pa.Schema,
    src_name: str,
    temp_dir: Path,
    row_counts: dict[tuple[str, ...], int],
    specs: dict[tuple[str, ...], AzurePartitionSpec],
    exact_values: dict[tuple[str, ...], dict[str, str | object | None]],
    spec: AzurePartitionSpec,
) -> None:
    sliced = table.slice(start, end - start)
    part_dir = temp_dir / _partition_dir_name(sort_key)
    part_dir.mkdir(parents=True, exist_ok=True)
    out_path = part_dir / src_name

    if sort_key not in writers:
        writers[sort_key] = pq.ParquetWriter(out_path, schema, compression="zstd")
    writers[sort_key].write_table(sliced)

    row_counts[sort_key] = row_counts.get(sort_key, 0) + sliced.num_rows
    specs[sort_key] = spec
    state = exact_values.setdefault(
        sort_key,
        {"instance_type": None, "region": None, "az": None},
    )
    slice_instance_type, slice_region, slice_az = _slice_partition_exactness(sliced)
    state["instance_type"] = _extend_exact_value(
        state["instance_type"],
        slice_instance_type,
    )
    state["region"] = _extend_exact_value(
        state["region"],
        slice_region,
    )
    state["az"] = _extend_exact_value(
        state["az"],
        slice_az,
    )


def _materialize_partitions(
    input_paths: list[Path],
    temp_dir: Path,
    partition_assigner: Callable[[str, str, str, str], tuple[tuple[str, ...], AzurePartitionSpec]],
) -> list[MaterializedAzurePartition]:
    temp_dir.mkdir(parents=True, exist_ok=True)
    row_counts: dict[tuple[str, ...], int] = {}
    specs: dict[tuple[str, ...], AzurePartitionSpec] = {}
    exact_values: dict[tuple[str, ...], dict[str, str | object | None]] = {}

    for src in sorted(input_paths):
        pf = pq.ParquetFile(src)
        schema = build_output_schema(src, provider="azure")
        writers: dict[tuple[str, ...], pq.ParquetWriter] = {}

        try:
            for batch in pf.iter_batches(batch_size=65536):
                batch = batch.cast(schema)
                table = pa.Table.from_batches([batch])
                tiers = table["InstanceTier"].to_pylist()
                types = table["InstanceType"].to_pylist()
                regions = table["Region"].to_pylist()
                azs = table["AZ"].to_pylist()
                if not tiers:
                    continue

                start = 0
                current_key, current_spec = partition_assigner(
                    str(tiers[0]),
                    str(types[0]),
                    str(regions[0]),
                    str(azs[0]),
                )

                for idx in range(1, len(tiers)):
                    next_key, next_spec = partition_assigner(
                        str(tiers[idx]),
                        str(types[idx]),
                        str(regions[idx]),
                        str(azs[idx]),
                    )
                    if next_key == current_key:
                        continue

                    _write_partition_slice(
                        writers=writers,
                        table=table,
                        start=start,
                        end=idx,
                        sort_key=current_key,
                        schema=schema,
                        src_name=src.name,
                        temp_dir=temp_dir,
                        row_counts=row_counts,
                        specs=specs,
                        exact_values=exact_values,
                        spec=current_spec,
                    )

                    start = idx
                    current_key = next_key
                    current_spec = next_spec

                _write_partition_slice(
                    writers=writers,
                    table=table,
                    start=start,
                    end=len(tiers),
                    sort_key=current_key,
                    schema=schema,
                    src_name=src.name,
                    temp_dir=temp_dir,
                    row_counts=row_counts,
                    specs=specs,
                    exact_values=exact_values,
                    spec=current_spec,
                )
        finally:
            for writer in writers.values():
                writer.close()

    partitions = []
    for sort_key, row_count in row_counts.items():
        state = exact_values[sort_key]
        partitions.append(
            MaterializedAzurePartition(
                sort_key=sort_key,
                dir_path=temp_dir / _partition_dir_name(sort_key),
                row_count=row_count,
                spec=specs[sort_key],
                instance_type_exact=_exact_value_or_none(state["instance_type"]),
                region_exact=_exact_value_or_none(state["region"]),
                az_exact=_exact_value_or_none(state["az"]),
            )
        )
    return sorted(partitions, key=lambda part: part.sort_key)


def _materialize_initial_partitions(
    input_paths: list[Path],
    temp_dir: Path,
    *,
    prefix_len: int,
    split_prefixes: set[str],
    default_prefix_len: int,
) -> list[MaterializedAzurePartition]:
    def assigner(
        instance_tier: str,
        instance_type: str,
        region: str,
        az: str,
    ) -> tuple[tuple[str, ...], AzurePartitionSpec]:
        spec = _initial_spec_for_type(
            instance_type,
            prefix_len=prefix_len,
            split_prefixes=split_prefixes,
            default_prefix_len=default_prefix_len,
        )
        return _build_key_parts_for_spec(instance_tier, instance_type, region, az, spec), spec

    return _materialize_partitions(input_paths, temp_dir, assigner)


def _materialize_with_spec(
    input_paths: list[Path],
    temp_dir: Path,
    spec: AzurePartitionSpec,
) -> list[MaterializedAzurePartition]:
    def assigner(
        instance_tier: str,
        instance_type: str,
        region: str,
        az: str,
    ) -> tuple[tuple[str, ...], AzurePartitionSpec]:
        return _build_key_parts_for_spec(instance_tier, instance_type, region, az, spec), spec

    return _materialize_partitions(input_paths, temp_dir, assigner)


def _next_refinement_specs(
    partition: MaterializedAzurePartition,
    *,
    max_prefix_len: int,
) -> list[AzurePartitionSpec]:
    spec = partition.spec
    candidates: list[AzurePartitionSpec] = []

    if partition.instance_type_exact is None:
        if spec.type_prefix_len >= max_prefix_len:
            return []
        candidates.append(
            AzurePartitionSpec(
                type_prefix_len=spec.type_prefix_len + 1,
                split_region=False,
                split_az=False,
            )
        )
        return candidates

    if partition.region_exact is None and not spec.split_region:
        candidates.append(
            AzurePartitionSpec(
                type_prefix_len=spec.type_prefix_len,
                split_region=True,
                split_az=False,
            )
        )
        return candidates

    if partition.az_exact is None and not spec.split_az:
        candidates.append(
            AzurePartitionSpec(
                type_prefix_len=spec.type_prefix_len,
                split_region=True,
                split_az=True,
            )
        )

    return candidates


def _refinement_progressed(
    parent: MaterializedAzurePartition,
    children: list[MaterializedAzurePartition],
) -> bool:
    if len(children) > 1:
        return True
    return bool(children) and children[0].sort_key != parent.sort_key


def _refine_partition(
    partition: MaterializedAzurePartition,
    *,
    root_temp_dir: Path,
    max_rows_per_partition: int,
    max_prefix_len: int,
) -> list[MaterializedAzurePartition]:
    if partition.row_count <= max_rows_per_partition:
        return [partition]

    input_paths = sorted(partition.dir_path.glob("*.parquet"))
    candidates = _next_refinement_specs(partition, max_prefix_len=max_prefix_len)

    for candidate in candidates:
        child_root = Path(
            tempfile.mkdtemp(
                dir=root_temp_dir,
                prefix=f"refine_{partition.safe_name}_",
            )
        )
        children = _materialize_with_spec(input_paths, child_root, candidate)
        if not _refinement_progressed(partition, children):
            continue

        leaves: list[MaterializedAzurePartition] = []
        for child in children:
            leaves.extend(
                _refine_partition(
                    child,
                    root_temp_dir=root_temp_dir,
                    max_rows_per_partition=max_rows_per_partition,
                    max_prefix_len=max_prefix_len,
                )
            )
        return leaves

    raise OversizedPartitionError(
        "Unable to refine Azure partition under row budget: "
        f"key={partition.key} rows={partition.row_count} spec={partition.spec.describe()}"
    )


def materialize_azure_partitions(
    input_paths: list[Path],
    temp_dir: Path,
    *,
    prefix_len: int = 2,
    split_prefixes: set[str] | None = None,
    default_prefix_len: int = 1,
    max_rows_per_partition: int | None = None,
    max_prefix_len: int = 32,
) -> list[MaterializedAzurePartition]:
    """Materialize Azure partitions and recursively refine oversized leaves.

    The planner starts with the existing adaptive prefix rule, then recursively
    refines only oversized partitions deeper along the sort key:
    InstanceType prefix -> Region -> AZ.
    This guarantees the planner can converge toward full PK granularity without
    hard-coding workload-specific prefixes.
    """
    temp_dir.mkdir(parents=True, exist_ok=True)
    split_prefixes = split_prefixes or set()
    if not split_prefixes:
        default_prefix_len = prefix_len

    partitions = _materialize_initial_partitions(
        input_paths,
        temp_dir,
        prefix_len=prefix_len,
        split_prefixes=split_prefixes,
        default_prefix_len=default_prefix_len,
    )
    if max_rows_per_partition is None:
        return partitions

    refined: list[MaterializedAzurePartition] = []
    for partition in partitions:
        refined.extend(
            _refine_partition(
                partition,
                root_temp_dir=temp_dir,
                max_rows_per_partition=max_rows_per_partition,
                max_prefix_len=max_prefix_len,
            )
        )
    return sorted(refined, key=lambda part: part.sort_key)


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


def streaming_merge_partition(
    partition_dir: Path,
    output_path: Path,
    *,
    batch_size: int = 8192,
    row_group_size: int = 10_000,
) -> tuple[int, int]:
    """Merge one materialized partition with low-memory streaming path."""
    input_paths = sorted(partition_dir.glob("*.parquet"))
    if not input_paths:
        raise ValueError(f"no parquet inputs found under {partition_dir}")

    result = merge_sorted_parquet_files(
        [str(path) for path in input_paths],
        str(output_path),
        key_columns=["InstanceTier", "InstanceType", "Region", "AZ", "Time"],
        batch_size=batch_size,
        row_group_size=row_group_size,
        output_schema=build_output_schema(input_paths[0], provider="azure"),
    )
    return int(result["rows_written"]), int(result["output_size_bytes"])


def merge_partition(
    partition_dir: Path,
    output_path: Path,
    *,
    strategy: str = "streaming",
    batch_size: int = 8192,
    row_group_size: int = 10_000,
) -> tuple[int, int]:
    """Merge one materialized partition using the selected strategy."""
    if strategy == "streaming":
        return streaming_merge_partition(
            partition_dir,
            output_path,
            batch_size=batch_size,
            row_group_size=row_group_size,
        )
    if strategy == "eager":
        return eager_merge_partition(partition_dir, output_path)
    raise ValueError(f"unsupported partition merge strategy: {strategy}")


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
    parser.add_argument("--strategy", choices=["eager", "streaming"], default="streaming")
    parser.add_argument("--batch-size", type=int, default=8192)
    parser.add_argument("--row-group-size", type=int, default=10_000)
    args = parser.parse_args()

    rows, size = merge_partition(
        Path(args.partition_dir),
        Path(args.output),
        strategy=args.strategy,
        batch_size=args.batch_size,
        row_group_size=args.row_group_size,
    )
    print(f"rows={rows}")
    print(f"size={size}")


if __name__ == "__main__":
    main()
