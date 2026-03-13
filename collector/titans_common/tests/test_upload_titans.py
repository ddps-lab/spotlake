"""Tests for titans_common.upload_titans module."""
import os
import pytest
import pandas as pd
from datetime import datetime, timezone

# Force test environment (set before imports)
os.environ["TITANS_ENV"] = "test"

from titans_common.upload_titans import upload_hot_tier, _build_s3_key, _normalize_schema
from titans_common.config import get_config


class TestUploadHotTier:
    """Tests for upload_hot_tier function."""

    def test_timezone_required(self):
        """Naive timestamp raises ValueError."""
        df = pd.DataFrame({"InstanceType": ["m5.large"], "Region": ["us-west-2"], "AZ": ["us-west-2a"]})
        naive_ts = datetime(2026, 1, 23, 12, 0)  # no tzinfo

        with pytest.raises(ValueError, match="timezone-aware"):
            upload_hot_tier(df, naive_ts, provider="aws")

    def test_empty_df_returns_empty(self):
        """Empty DataFrame returns empty string."""
        df = pd.DataFrame()
        ts = datetime(2026, 1, 23, 12, 0, tzinfo=timezone.utc)
        result = upload_hot_tier(df, ts, provider="aws")
        assert result == ""

    def test_empty_df_with_columns_returns_empty(self):
        """DataFrame with only columns also returns empty string."""
        df = pd.DataFrame(columns=["InstanceType", "Region", "AZ"])
        ts = datetime(2026, 1, 23, 12, 0, tzinfo=timezone.utc)
        result = upload_hot_tier(df, ts, provider="aws")
        assert result == ""


class TestBuildS3Key:
    """Tests for _build_s3_key function."""

    def test_10min_slot_00(self):
        """10-minute slot - 00 range."""
        config = get_config("aws")
        ts = datetime(2026, 1, 23, 14, 5, tzinfo=timezone.utc)
        key = _build_s3_key(ts, config)
        assert "14-00.parquet" in key

    def test_10min_slot_20(self):
        """10-minute slot - 20 range."""
        config = get_config("aws")
        ts = datetime(2026, 1, 23, 14, 23, tzinfo=timezone.utc)
        key = _build_s3_key(ts, config)
        assert "14-20.parquet" in key

    def test_10min_slot_30(self):
        """10-minute slot - 30 range."""
        config = get_config("aws")
        ts = datetime(2026, 1, 23, 14, 37, tzinfo=timezone.utc)
        key = _build_s3_key(ts, config)
        assert "14-30.parquet" in key

    def test_10min_slot_50(self):
        """10-minute slot - 50 range."""
        config = get_config("aws")
        ts = datetime(2026, 1, 23, 14, 59, tzinfo=timezone.utc)
        key = _build_s3_key(ts, config)
        assert "14-50.parquet" in key

    def test_key_contains_correct_path_structure(self):
        """Verify key path structure."""
        os.environ["TITANS_ENV"] = "test"  # Ensure test env
        config = get_config("aws")
        ts = datetime(2026, 1, 23, 14, 23, tzinfo=timezone.utc)
        key = _build_s3_key(ts, config)

        # Should contain: test/parquet_cp_hot/aws/2026/01/23/14-20.parquet
        assert "test/parquet_cp_hot/aws" in key
        assert "2026/01/23" in key
        assert "14-20.parquet" in key

    def test_key_deterministic(self):
        """Same time slot generates identical key."""
        config = get_config("aws")
        ts1 = datetime(2026, 1, 23, 14, 23, tzinfo=timezone.utc)
        ts2 = datetime(2026, 1, 23, 14, 28, tzinfo=timezone.utc)

        key1 = _build_s3_key(ts1, config)
        key2 = _build_s3_key(ts2, config)

        assert key1 == key2  # Same 10-min slot


class TestNormalizeSchema:
    """Tests for _normalize_schema function."""

    def test_ceased_column_added_if_missing(self):
        """Add default Ceased column if missing."""
        import polars as pl

        config = get_config("aws")
        df = pl.DataFrame({
            "InstanceType": ["m5.large"],
            "Region": ["us-west-2"],
            "AZ": ["us-west-2a"],
            "Time": [datetime(2026, 1, 23, 12, 0, tzinfo=timezone.utc)],
            "SPS": [1],
        })

        result = _normalize_schema(df, config)
        assert "Ceased" in result.columns
        assert result["Ceased"][0] == False

    def test_column_order_normalized(self):
        """Column order is normalized."""
        import polars as pl

        config = get_config("aws")
        df = pl.DataFrame({
            "SPS": [1],
            "AZ": ["us-west-2a"],
            "Time": [datetime(2026, 1, 23, 12, 0, tzinfo=timezone.utc)],
            "Region": ["us-west-2"],
            "InstanceType": ["m5.large"],
        })

        result = _normalize_schema(df, config)

        # PK columns should come first
        assert result.columns[0] == "InstanceType"
        assert result.columns[1] == "Region"
        assert result.columns[2] == "AZ"
        assert result.columns[3] == "Time"
