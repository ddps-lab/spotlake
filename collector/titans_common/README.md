# titans_common

[English](#english) | [한국어](README_ko.md)

---

<a name="english"></a>

## Overview

**titans_common** is a shared Python module for TITANS Hot/Warm tier integration in SpotLake collectors.

It provides:
- **Hot tier writer**: Upload change points to S3 Parquet with idempotency guarantees
- **Warm tier compactor**: m-way incremental compaction with ETag-based optimistic locking
- **Multi-provider support**: AWS, Azure, GCP with extensible configuration

## Quick Start

### Basic Usage

```python
import os
from datetime import datetime, timezone

# Set environment BEFORE imports
os.environ["TITANS_ENV"] = "test"  # or "production"

from titans_common import prepare_for_upload, upload_hot_tier, run_compaction

# 1. Prepare DataFrames (changed + removed)
combined_df = prepare_for_upload(changed_df, removed_df)

# 2. Upload to Hot tier (10-minute Parquet files)
timestamp = datetime(2026, 1, 23, 14, 20, tzinfo=timezone.utc)
hot_key = upload_hot_tier(combined_df, timestamp, provider="aws")

# 3. Run Warm compaction (m-way incremental merge)
if hot_key:
    run_compaction(hot_key, timestamp, provider="aws")
```

### Environment Configuration

| Environment | `TITANS_ENV` | S3 Prefix | Use Case |
|-------------|--------------|-----------|----------|
| Test | `test` | `test/parquet_cp_hot/...` | Validation before production |
| Production | `production` or unset | `parquet_cp_hot/...` | Live system |

**Important**: Set `TITANS_ENV` before importing titans_common modules.

```python
# batch-test/merge_data.py
os.environ.setdefault("TITANS_ENV", "test")  # First line

# batch/merge_data.py (Production)
os.environ.setdefault("TITANS_ENV", "production")  # First line
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         merge_data.py                                │
│  ┌─────────────┐    ┌──────────────┐    ┌────────────────────────┐  │
│  │ Timestream  │    │ titans_common│    │ S3 (csv.gz backup)     │  │
│  │  (existing) │    │   (NEW)      │    │                        │  │
│  └─────────────┘    └──────────────┘    └────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        titans_common                                 │
│  ┌─────────────┐    ┌──────────────┐    ┌────────────────────────┐  │
│  │  config.py  │───▶│upload_titans │───▶│   Hot Tier (S3)        │  │
│  │  (Provider  │    │    .py       │    │ 10-min Parquet files   │  │
│  │   Config)   │    └──────────────┘    └────────────────────────┘  │
│  └─────────────┘            │                      │                 │
│        │                    ▼                      ▼                 │
│        │           ┌──────────────┐    ┌────────────────────────┐   │
│        └──────────▶│warm_compactor│───▶│   Warm Tier (S3)       │   │
│                    │    .py       │    │ Compacted Parquet      │   │
│                    └──────────────┘    │ + manifest.json        │   │
│                                        └────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

### S3 Path Structure

```
s3://titans-spotlake-data/
├── test/                              # Test environment
│   ├── parquet_cp_hot/aws/
│   │   └── 2026/01/23/14-20.parquet   # Hot tier (10-min slots)
│   └── parquet_warm/aws/m8/
│       └── 2026/01/
│           ├── L1_0001_00000-00007.parquet
│           ├── L2_0002_00000-00063.parquet
│           └── manifest.json
│
├── parquet_cp_hot/aws/...             # Production Hot tier
└── parquet_warm/aws/m8/...            # Production Warm tier
```

## API Reference

### `prepare_for_upload(changed_df, removed_df)`

Merge changed and removed DataFrames with proper Ceased column handling.

**Parameters:**
- `changed_df` (pd.DataFrame): Records with changed values
- `removed_df` (pd.DataFrame): Records that were removed (Ceased=True)

**Returns:**
- `pd.DataFrame`: Combined DataFrame ready for upload

**Example:**
```python
combined = prepare_for_upload(changed_df, removed_df)
# combined has Ceased=False for changed, Ceased=True for removed
```

---

### `upload_hot_tier(changed_df, timestamp, provider="aws")`

Upload change points to TITANS Hot tier as Parquet.

**Parameters:**
- `changed_df` (pd.DataFrame): Change points (use `prepare_for_upload` first)
- `timestamp` (datetime): Collection time (**must be timezone-aware, UTC recommended**)
- `provider` (str): Cloud provider (`"aws"`, `"azure"`, `"gcp"`)

**Returns:**
- `str`: Uploaded S3 key (empty string if DataFrame is empty)

**Raises:**
- `ValueError`: If timestamp is timezone-naive

**Features:**
- **Idempotent**: Uses `put_object` + `IfNoneMatch="*"` for conditional writes
- **Deterministic keys**: 10-minute slot normalization (14:23 → 14-20.parquet)
- **Schema normalization**: dtype casting via `config.schema_dtypes`
- **Compression**: zstd for optimal storage

**Example:**
```python
from datetime import datetime, timezone

ts = datetime(2026, 1, 23, 14, 23, tzinfo=timezone.utc)
key = upload_hot_tier(df, ts, provider="aws")
# Returns: "test/parquet_cp_hot/aws/2026/01/23/14-20.parquet"
```

---

### `run_compaction(hot_s3_key, timestamp, provider="aws", timeout_seconds=30.0)`

Run Warm tier compaction after Hot file upload.

**Parameters:**
- `hot_s3_key` (str): S3 key from `upload_hot_tier`
- `timestamp` (datetime): Collection time (**must be timezone-aware**)
- `provider` (str): Cloud provider
- `timeout_seconds` (float): Warning threshold for long compaction

**Raises:**
- `ValueError`: If timestamp is timezone-naive
- `ConcurrencyConflictError`: If manifest was modified by another process

**Features:**
- **m-way compaction**: 8 files at each level → merge to next level
- **ETag locking**: Optimistic locking for manifest.json
- **Idempotent**: `processed_keys` set prevents duplicate processing
- **Rollback on failure**: Cleans up partially created files

**Example:**
```python
try:
    run_compaction(hot_key, ts, provider="aws")
except ConcurrencyConflictError:
    print("Will retry next cycle")
```

---

### `ConcurrencyConflictError`

Exception raised when manifest.json was modified by another process during compaction.

**Usage:**
```python
from titans_common import ConcurrencyConflictError

try:
    run_compaction(hot_key, ts, provider="aws")
except ConcurrencyConflictError as e:
    # Another process modified manifest - retry next cycle
    print(f"Conflict: {e}")
```

---

### `get_config(provider)`

Get provider-specific configuration.

**Parameters:**
- `provider` (str): `"aws"`, `"azure"`, or `"gcp"`

**Returns:**
- `ProviderConfig`: Configuration dataclass with:
  - `pk_columns`: Primary key columns
  - `value_columns`: Value columns
  - `hot_prefix`: S3 prefix for Hot tier
  - `warm_prefix`: S3 prefix for Warm tier
  - `schema_dtypes`: Polars dtype mapping
  - `canonical_columns`: Column order for normalization

**Example:**
```python
from titans_common.config import get_config

config = get_config("aws")
print(config.pk_columns)  # ['InstanceType', 'Region', 'AZ']
print(config.hot_prefix)  # 'test/parquet_cp_hot/aws' or 'parquet_cp_hot/aws'
```

## Provider Support

| Provider | PK Columns | Score Column | Status |
|----------|------------|--------------|--------|
| AWS | `InstanceType, Region, AZ` | `SPS` | **Active** |
| Azure | `InstanceType, Region, AvailabilityZone` | `Score` | Planned |
| GCP | `InstanceType, Region` | `Score` | Planned |

### Adding a New Provider

1. Add configuration to `config.py`:
```python
PROVIDER_CONFIGS["newcloud"] = ProviderConfig(
    name="newcloud",
    pk_columns=["InstanceType", "Region"],
    value_columns=["Score", "Price"],
    score_column="Score",
)
```

2. Use in collector:
```python
upload_hot_tier(df, ts, provider="newcloud")
run_compaction(key, ts, provider="newcloud")
```

## Testing

### Run Unit Tests

```bash
cd spotlake/collector/titans_common
uv run pytest tests/ -v
```

### Test Coverage

```bash
uv run pytest tests/ -v --cov=titans_common --cov-report=term-missing
```

### Test Structure

```
titans_common/tests/
├── conftest.py           # Shared fixtures (moto S3 mock)
├── test_config.py        # Provider config tests (16 tests)
├── test_upload_titans.py # Hot tier upload tests (11 tests)
└── test_warm_compactor.py# Compaction tests (10 tests)
```

### Manual E2E Test (Test Environment)

```bash
# 1. Set test environment
export TITANS_ENV=test

# 2. Run merge_data.py in batch-test
cd spotlake/collector/spot-dataset/aws/batch-test/merge
uv run python merge_data.py --sps_key "rawdata/aws/sps/2026/01/23/14-20_sps_50.pkl.gz"

# 3. Verify Hot tier file
aws s3 ls s3://titans-spotlake-data/test/parquet_cp_hot/aws/2026/01/23/ --profile spotrank

# 4. Verify Warm tier manifest
aws s3 cp s3://titans-spotlake-data/test/parquet_warm/aws/m8/2026/01/manifest.json - --profile spotrank | jq .
```

## Key Design Decisions

### Idempotency

- **Hot tier**: `put_object` + `IfNoneMatch="*"` ensures file is created only once
- **Warm tier**: `processed_keys` set in manifest tracks processed Hot files

### Concurrency Control

- **Manifest locking**: ETag-based optimistic locking (read ETag → write with `IfMatch`)
- **Conflict handling**: `ConcurrencyConflictError` bubbles up for retry

### Error Isolation

```python
# In merge_data.py - TITANS errors don't affect Timestream
try:
    hot_key = upload_hot_tier(combined_df, ts, provider=PROVIDER)
    if hot_key:
        run_compaction(hot_key, ts, provider=PROVIDER)
except ConcurrencyConflictError as e:
    print(f"Conflict, will retry: {e}")
except Exception as e:
    print(f"TITANS failed (non-fatal): {e}")
```

### Timezone Requirements

All timestamps **must be timezone-aware**:

```python
# Correct
ts = datetime(2026, 1, 23, 14, 0, tzinfo=timezone.utc)

# Wrong - raises ValueError
ts = datetime(2026, 1, 23, 14, 0)  # naive datetime
```

## Dependencies

```
polars>=1.37.0      # Parquet generation with zstd (native, no pyarrow needed)
boto3               # S3 operations
pandas              # Input DataFrame format
```

## Troubleshooting

### "timestamp must be timezone-aware"

Ensure your timestamp has tzinfo:
```python
from datetime import timezone
ts = datetime.now(timezone.utc)
# or
ts = naive_datetime.replace(tzinfo=timezone.utc)
```

### "PreconditionFailed" in upload

This is expected behavior for idempotency - the file already exists from a previous run.

### "Manifest was modified by another process"

`ConcurrencyConflictError` - another collector modified the manifest. The operation will be retried on the next cycle automatically.

### Import errors for titans_common

Ensure the collector root is in sys.path:
```python
import sys
from pathlib import Path
COLLECTOR_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(COLLECTOR_ROOT))
```

## Related Documentation

- [TITANS Architecture](../../AGENTS.md)
- [Warm Tier Implementation](../../agent_docs/warm_tier_implementation.md)
- [Hot Tier Analysis](../../agent_docs/hot_tier_analysis.md)
- [Plan Document](../../spotlake_plan.md)
