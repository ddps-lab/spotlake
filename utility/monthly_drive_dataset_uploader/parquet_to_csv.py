#!/usr/bin/env python3
"""Convert SpotLake daily Parquet files or monthly ZIPs to CSV.

Install: python -m pip install 'polars>=1.44.2,<2'
Combined CSV: python parquet_to_csv.py aws-2025-03.zip -o month.csv.gz
Per observation: python parquet_to_csv.py aws-2025-03.zip --mode tick -o snapshots

Tick mode writes <provider>-YYYY-MM/DD/HH-MM-SS.csv.gz under the output
folder. It retains the current dataset columns; removed raw columns are not
reconstructed. Timestamps and missing intervals are preserved, not resampled.
"""
import argparse
import contextlib
import glob
import gzip
import re
import shutil
import sys
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path, PurePosixPath

import polars as pl

CSV_FORMAT = {"datetime_format": "%Y-%m-%d %H:%M:%S"}
PROVIDER_RE = re.compile(r"(?:^|/)(aws|azure|gcp)-\d{4}-\d{2}(?:-|/|\.)")


@contextlib.contextmanager
def resolve(sources):
    """Expand quoted globs and safely extract only Parquet ZIP members."""
    sources = [sources] if isinstance(sources, (str, Path)) else sources
    paths = []
    seen = set()
    for source in sources:
        matches = sorted(glob.glob(str(source)))
        if not matches:
            raise FileNotFoundError(f"No input matches: {source}")
        for match in matches:
            path = Path(match).resolve()
            if path not in seen:
                seen.add(path)
                paths.append(path)
    if not paths:
        raise ValueError("No input files")
    with tempfile.TemporaryDirectory(prefix="spotlake-parquet-") as temporary:
        parquet = []
        for index, path in enumerate(paths):
            if path.suffix.lower() == ".parquet":
                parquet.append(path)
            elif path.suffix.lower() == ".zip":
                with zipfile.ZipFile(path) as archive:
                    names = [n for n in archive.namelist() if n.endswith(".parquet")]
                    if not names:
                        raise ValueError(f"No Parquet files in {path.name}")
                    if len(names) != len(set(names)):
                        raise ValueError(f"Duplicate ZIP members in {path.name}")
                    for name in sorted(names):
                        relative = PurePosixPath(name)
                        if relative.is_absolute() or ".." in relative.parts or "\\" in name:
                            raise ValueError(f"Unsafe ZIP path: {name}")
                        dest = Path(temporary) / str(index) / relative
                        dest.parent.mkdir(parents=True, exist_ok=True)
                        with archive.open(name) as src, dest.open("wb") as dst:
                            shutil.copyfileobj(src, dst)
                        parquet.append(dest)
            else:
                raise ValueError(f"Expected a .parquet or .zip file: {path}")
        yield parquet


def provider_of(path, explicit=None):
    match = PROVIDER_RE.search(path.as_posix())
    inferred = match.group(1) if match else None
    if explicit and inferred and explicit != inferred:
        raise ValueError(f"Provider disagrees with input filename: {path.name}")
    if not (explicit or inferred):
        raise ValueError(f"Cannot infer provider from {path.name}; use --provider")
    return explicit or inferred


def _combined(paths, out_path, sort_by_time):
    # Scan separately, then reconcile historical Int64/Float64 and text scores.
    frames = [pl.scan_parquet(path) for path in paths]
    frame = pl.concat(frames, how="diagonal_relaxed")
    if sort_by_time:
        frame = frame.sort("Time")
    if str(out_path).endswith(".gz"):
        with gzip.open(out_path, "wb") as output:
            frame.sink_csv(output, **CSV_FORMAT)
    else:
        frame.sink_csv(out_path, **CSV_FORMAT)


def _ticks(paths, output_dir, provider, batch_size):
    written = set()
    total_rows = 0
    for path in paths:
        vendor = provider_of(path, provider)
        scan = pl.scan_parquet(path)
        schema = scan.collect_schema()
        if "Time" not in schema or not isinstance(schema["Time"], pl.Datetime):
            raise ValueError(f"{path.name}: Time must be a Parquet datetime column")
        if schema["Time"].time_zone not in (None, "UTC"):
            raise ValueError(f"{path.name}: Time must be in UTC")
        handles = {}
        try:
            # Bounded input batches: never gather an entire month or day in RAM.
            for batch in scan.collect_batches(chunk_size=batch_size, maintain_order=True):
                if batch["Time"].null_count():
                    raise ValueError(f"{path.name}: Time contains nulls")
                if batch.filter(pl.col("Time") != pl.col("Time").dt.truncate("1s")).height:
                    raise ValueError(f"{path.name}: subsecond timestamps cannot use HH-MM-SS filenames")
                for key, part in batch.partition_by("Time", as_dict=True, maintain_order=True).items():
                    tick = key[0]
                    relative = Path(f"{vendor}-{tick:%Y-%m}") / f"{tick:%d}" / f"{tick:%H-%M-%S}.csv.gz"
                    if relative not in handles:
                        if relative in written:
                            raise ValueError(f"Overlapping input snapshots: {relative}")
                        dest = output_dir / relative
                        dest.parent.mkdir(parents=True, exist_ok=True)
                        handles[relative] = gzip.open(dest, "wb")
                        part.write_csv(handles[relative], **CSV_FORMAT)
                        written.add(relative)
                    else:
                        part.write_csv(handles[relative], include_header=False, **CSV_FORMAT)
                    total_rows += part.height
        finally:
            for handle in handles.values():
                handle.close()
        print(f"{path.name}: converted; {len(written):,} snapshots / {total_rows:,} rows so far", file=sys.stderr)
    return {"files": len(written), "rows": total_rows}


def convert(sources, out_path, sort_by_time=False, mode="combined", provider=None, batch_size=100_000):
    """Create a single CSV, or a new directory of gzip CSV snapshots atomically."""
    if mode not in ("combined", "tick"):
        raise ValueError(f"Unknown mode: {mode}")
    if mode == "tick" and sort_by_time:
        raise ValueError("--sort-by-time is only used in combined mode")
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    out_path = Path(out_path)
    if out_path.exists():
        raise FileExistsError(f"Output already exists: {out_path}")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with resolve(sources) as paths:
        # A failed conversion leaves no apparently complete output behind.
        with tempfile.TemporaryDirectory(prefix=".spotlake-csv-", dir=out_path.parent) as staging:
            staged = Path(staging) / out_path.name
            if mode == "tick":
                staged.mkdir()
                result = _ticks(paths, staged, provider, batch_size)
            else:
                _combined(paths, staged, sort_by_time)
                result = None
            if out_path.exists():
                raise FileExistsError(f"Output appeared during conversion: {out_path}")
            staged.rename(out_path)
            return result


def _selftest():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source = root / "aws-2025-03-15.parquet"
        pl.DataFrame({"Time": [datetime(2025, 3, 15, 0, 10), datetime(2025, 3, 15)],
                      "SPS": [3, 5]}).write_parquet(source)
        convert(source, root / "out.csv.gz", sort_by_time=True)
        result = convert(source, root / "ticks", mode="tick", batch_size=1)
        assert result == {"files": 2, "rows": 2}
        assert pl.read_csv(root / "out.csv.gz")["SPS"].to_list() == [5, 3]
    print("selftest OK")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("source", nargs="*", help="Parquet/ZIP paths or wildcard patterns")
    parser.add_argument("-o", "--out", help="New CSV path, or new output directory for tick mode")
    parser.add_argument("--mode", choices=("combined", "tick"), default="combined")
    parser.add_argument("--provider", choices=("aws", "azure", "gcp"), help="Provider for inputs without standard filenames")
    parser.add_argument("--sort-by-time", action="store_true", help="Sort combined CSV by timestamp")
    parser.add_argument("--selftest", action="store_true")
    args = parser.parse_args()
    if args.selftest:
        _selftest()
        return
    if not args.source or not args.out:
        parser.error("Provide input files and -o output")
    try:
        convert(args.source, args.out, args.sort_by_time, args.mode, args.provider)
    except (ValueError, FileNotFoundError, FileExistsError) as exc:
        parser.exit(1, f"Error: {exc}\n")
    print(f"Created {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
