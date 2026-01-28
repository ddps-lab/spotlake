"""TITANS Hot tier Parquet uploader (Multi-provider support)."""
from datetime import datetime, timezone
import io

import boto3
from botocore.exceptions import ClientError
import pandas as pd
import polars as pl

from .config import get_config, ProviderConfig


def upload_hot_tier(
    changed_df: pd.DataFrame,
    timestamp: datetime,
    provider: str = "aws",
) -> str:
    """Save change points to TITANS Hot tier Parquet.

    Args:
        changed_df: pandas DataFrame (change points or removed records)
        timestamp: Collection time (MUST be timezone-aware, UTC recommended)
        provider: Cloud provider (aws, azure, gcp)

    Returns:
        Uploaded S3 key (empty string if DataFrame is empty)

    Raises:
        ValueError: If timestamp is timezone-naive
    """
    if changed_df.empty:
        return ""

    # Validate timezone and normalize to UTC
    if timestamp.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware (use UTC)")
    ts_utc = timestamp.astimezone(timezone.utc)

    config = get_config(provider)

    # 1. Convert pandas to polars
    df = pl.from_pandas(changed_df)

    # 2. Normalize schema (including dtype casting)
    df = _normalize_schema(df, config)

    # 3. Sort by PK + Time (compression efficiency + query optimization)
    sort_cols = config.pk_columns + [config.time_column]
    df = df.sort(sort_cols)

    # 4. Serialize to Parquet (zstd compression)
    buffer = io.BytesIO()
    df.write_parquet(buffer, compression="zstd")
    buffer.seek(0)

    # 5. S3 Conditional PUT (idempotency guarantee)
    s3_key = _build_s3_key(ts_utc, config)
    s3_client = boto3.client("s3")

    try:
        # Atomic conditional write with put_object + IfNoneMatch
        # Note: upload_fileobj uses multipart upload which doesn't support conditional headers!
        s3_client.put_object(
            Bucket=config.titans_bucket,
            Key=s3_key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
            IfNoneMatch="*",  # Create only if file doesn't exist
        )
        print(f"[TITANS/{provider}] Uploaded {len(df)} rows to s3://{config.titans_bucket}/{s3_key}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "PreconditionFailed":
            print(f"[TITANS/{provider}] Key already exists (idempotent skip): {s3_key}")
        else:
            raise

    return s3_key


def _normalize_schema(df: pl.DataFrame, config: ProviderConfig) -> pl.DataFrame:
    """Normalize schema (including dtype casting)."""
    schema = config.schema_dtypes

    # 1. Cast each column dtype
    cast_exprs = []
    for col, dtype in schema.items():
        if col in df.columns:
            # Special handling for Time column: str -> datetime requires str.to_datetime()
            if col == config.time_column and df.schema.get(col) == pl.Utf8:
                cast_exprs.append(
                    pl.col(col)
                    .str.to_datetime("%Y-%m-%d %H:%M:%S")
                    .dt.replace_time_zone("UTC")
                    .alias(col)
                )
            else:
                cast_exprs.append(pl.col(col).cast(dtype))

    if cast_exprs:
        df = df.with_columns(cast_exprs)

    # 2. Add default Ceased column
    if "Ceased" not in df.columns:
        df = df.with_columns(pl.lit(False).alias("Ceased"))

    # 3. Select columns in canonical order
    existing_cols = [c for c in config.canonical_columns if c in df.columns]
    return df.select(existing_cols)


def _build_s3_key(timestamp: datetime, config: ProviderConfig) -> str:
    """Build S3 key - deterministic key based on 10-minute slots.

    Normalizes to 10-minute slots so retries generate the same key.
    Example: 14:23 -> 14-20.parquet, 14:37 -> 14-30.parquet
    """
    # Normalize to 10-minute slot (same key on retry)
    slot_minute = (timestamp.minute // 10) * 10
    return (
        f"{config.hot_prefix}/"
        f"{timestamp.year}/{timestamp.month:02d}/{timestamp.day:02d}/"
        f"{timestamp.hour:02d}-{slot_minute:02d}.parquet"
    )
