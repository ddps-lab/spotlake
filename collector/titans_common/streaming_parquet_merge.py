"""Streaming merge for sorted parquet files with duplicate suppression."""
from __future__ import annotations

from dataclasses import dataclass
import heapq
from pathlib import Path
from datetime import datetime
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


@dataclass
class _StreamState:
    """Batch-backed row stream over a sorted parquet file."""

    path: str
    columns: list[str]
    key_columns: list[str]
    ceased_column: str
    batch_size: int
    stream_id: int
    target_schema: pa.Schema

    def __post_init__(self) -> None:
        self._parquet = pq.ParquetFile(self.path)
        self._batches = self._parquet.iter_batches(batch_size=self.batch_size, columns=self.columns)
        self._batch: pa.RecordBatch | None = None
        self._key_values: list[list[Any]] | None = None
        self._ceased_values: list[Any] | None = None
        self._sort_keys: list[tuple] | None = None
        self._dedup_keys: list[tuple] | None = None
        self._size = 0
        self._row_idx = 0
        self.exhausted = False
        self._load_next_batch()

    def _load_next_batch(self) -> None:
        try:
            batch = next(self._batches)
        except StopIteration:
            self._batch = None
            self._key_values = None
            self._ceased_values = None
            self._sort_keys = None
            self._dedup_keys = None
            self._size = 0
            self._row_idx = 0
            self.exhausted = True
            return

        batch = batch.cast(self.target_schema)
        self._batch = batch
        self._key_values = [batch.column(batch.schema.get_field_index(col)).to_pylist() for col in self.key_columns]
        self._ceased_values = batch.column(batch.schema.get_field_index(self.ceased_column)).to_pylist()
        self._size = batch.num_rows
        self._row_idx = 0
        self.exhausted = self._size == 0
        if not self.exhausted:
            self._dedup_keys = [tuple(values[i] for values in self._key_values) for i in range(self._size)]
            self._sort_keys = [
                (self._dedup_keys[i], 1 if bool(self._ceased_values[i]) else 0, self.stream_id)
                for i in range(self._size)
            ]
        if self.exhausted:
            self._load_next_batch()

    def current_sort_key(self) -> tuple:
        if self.exhausted or self._sort_keys is None:
            raise RuntimeError("stream is exhausted")
        return self._sort_keys[self._row_idx]

    def current_dedup_key(self) -> tuple:
        if self.exhausted or self._dedup_keys is None:
            raise RuntimeError("stream is exhausted")
        return self._dedup_keys[self._row_idx]

    def emit_until(
        self,
        threshold_sort_key: tuple | None,
        out_batches: list[pa.RecordBatch],
    ) -> int:
        """Emit rows whose sort key is strictly less than threshold_sort_key."""
        rows_emitted = 0
        last_dedup_key = None

        while not self.exhausted:
            assert self._sort_keys is not None
            assert self._dedup_keys is not None
            assert self._batch is not None

            if threshold_sort_key is not None and self._sort_keys[self._row_idx] >= threshold_sort_key:
                break

            end = self._size
            if threshold_sort_key is not None:
                while end > self._row_idx and self._sort_keys[end - 1] >= threshold_sort_key:
                    end -= 1
                if end == self._row_idx:
                    break

            selected: list[int] = []
            for i in range(self._row_idx, end):
                dedup_key = self._dedup_keys[i]
                if dedup_key != last_dedup_key:
                    selected.append(i)
                    last_dedup_key = dedup_key

            if selected:
                if len(selected) == (end - self._row_idx):
                    out_batches.append(self._batch.slice(self._row_idx, end - self._row_idx))
                else:
                    out_batches.append(self._batch.take(pa.array(selected, type=pa.int32())))
                rows_emitted += len(selected)

            self._row_idx = end
            if self._row_idx >= self._size:
                self._load_next_batch()

        return rows_emitted

    def advance(self) -> None:
        if self.exhausted:
            return
        self._row_idx += 1
        if self._row_idx >= self._size:
            self._load_next_batch()


def merge_sorted_parquet_files(
    input_paths: list[str],
    output_path: str,
    *,
    key_columns: list[str],
    ceased_column: str = "Ceased",
    batch_size: int = 8192,
    row_group_size: int = 100_000,
    output_schema: pa.Schema | None = None,
) -> dict[str, Any]:
    """Merge sorted parquet files while deduplicating by key_columns.

    The input files must already be sorted by `key_columns`. If multiple rows
    share the same key, `Ceased=False` wins over `Ceased=True`, matching the
    existing warm compactor semantics.
    """
    if not input_paths:
        raise ValueError("input_paths must not be empty")

    first_schema = pq.ParquetFile(input_paths[0]).schema_arrow
    target_schema = output_schema or first_schema
    columns = target_schema.names
    if ceased_column not in columns:
        raise ValueError(f"missing ceased column: {ceased_column}")
    for col in key_columns:
        if col not in columns:
            raise ValueError(f"missing key column: {col}")

    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    streams = [
        _StreamState(
            path=path,
            columns=columns,
            key_columns=key_columns,
            ceased_column=ceased_column,
            batch_size=batch_size,
            stream_id=i,
            target_schema=target_schema,
        )
        for i, path in enumerate(input_paths)
    ]

    heap: list[tuple[tuple, int, int]] = []
    for stream in streams:
        if not stream.exhausted:
            heapq.heappush(heap, stream.current_sort_key())

    writer = pq.ParquetWriter(output_file, target_schema, compression="zstd")
    out_batches: list[pa.RecordBatch] = []
    rows_written = 0
    buffered_rows = 0

    try:
        while heap:
            current_sort_key = heapq.heappop(heap)
            _dedup_key, _ceased_sort, stream_id = current_sort_key
            stream = streams[stream_id]
            next_threshold = heap[0] if heap else None
            emitted = stream.emit_until(next_threshold, out_batches)
            rows_written += emitted
            buffered_rows += emitted
            if not stream.exhausted:
                heapq.heappush(heap, stream.current_sort_key())

            while heap and not stream.exhausted and heap[0][0] == stream.current_dedup_key():
                _dup_sort_key = heapq.heappop(heap)
                dup_stream = streams[_dup_sort_key[2]]
                dup_stream.advance()
                if not dup_stream.exhausted:
                    heapq.heappush(heap, dup_stream.current_sort_key())

            if buffered_rows >= row_group_size:
                table = pa.Table.from_batches(out_batches, schema=target_schema)
                writer.write_table(table)
                out_batches = []
                buffered_rows = 0

        if out_batches:
            table = pa.Table.from_batches(out_batches, schema=target_schema)
            writer.write_table(table)
    finally:
        writer.close()

    return {
        "rows_written": rows_written,
        "output_path": str(output_file),
        "output_size_bytes": output_file.stat().st_size,
    }
