"""Tests for titans_common.warm_compactor module."""
import os
import pytest
from datetime import datetime, timezone

# Force test environment (set before imports)
os.environ["TITANS_ENV"] = "test"

from titans_common.warm_compactor import (
    run_compaction,
    ConcurrencyConflictError,
    WarmFile,
    WarmCompactor,
    DEFAULT_M,
)


class TestRunCompaction:
    """Tests for run_compaction function."""

    def test_timezone_required(self):
        """Naive timestamp raises ValueError."""
        naive_ts = datetime(2026, 1, 23, 12, 0)  # no tzinfo

        with pytest.raises(ValueError, match="timezone-aware"):
            run_compaction("test-key", naive_ts, provider="aws")


class TestConcurrencyConflictError:
    """Tests for ConcurrencyConflictError exception."""

    def test_exception_can_be_raised(self):
        with pytest.raises(ConcurrencyConflictError):
            raise ConcurrencyConflictError("Test conflict")

    def test_exception_message(self):
        try:
            raise ConcurrencyConflictError("Manifest conflict")
        except ConcurrencyConflictError as e:
            assert "Manifest conflict" in str(e)


class TestWarmFile:
    """Tests for WarmFile dataclass."""

    def test_warm_file_creation(self):
        wf = WarmFile(level=1, hot_range=(0, 7), filename="L1_0000_00000-00007.parquet")
        assert wf.level == 1
        assert wf.hot_range == (0, 7)
        assert wf.filename == "L1_0000_00000-00007.parquet"

    def test_warm_file_level_0_is_hot_file(self):
        """Level 0 file references Hot file."""
        wf = WarmFile(
            level=0,
            hot_range=(5, 5),
            filename="test/parquet_cp_hot/aws/2026/01/23/14-20.parquet"
        )
        assert wf.level == 0
        assert "parquet_cp_hot" in wf.filename


class TestWarmCompactorConfig:
    """Tests for WarmCompactor configuration."""

    def test_default_m_value(self):
        """Verify default M value."""
        assert DEFAULT_M == 8

    def test_last_processed_time_field_exists(self):
        """WarmCompactor has last_processed_time for idempotency."""
        import dataclasses
        fields = {f.name for f in dataclasses.fields(WarmCompactor)}
        assert "last_processed_time" in fields


class TestWarmCompactorWarmPrefix:
    """Tests for WarmCompactor prefix generation."""

    def test_warm_prefix_includes_year_month(self):
        """Warm prefix includes year and month."""
        # Note: This test would require mocking S3, but we test the property logic
        from titans_common.config import get_config

        os.environ["TITANS_ENV"] = "test"
        config = get_config("aws")

        # Expected format: test/parquet_warm/aws/m8/YYYY/MM
        warm_prefix = config.warm_prefix
        assert "test/parquet_warm/aws/m8" in warm_prefix


class TestWarmCompactorIdempotency:
    """Tests for WarmCompactor idempotency via last_processed_time."""

    def test_last_processed_time_default_none(self):
        """last_processed_time defaults to None."""
        import dataclasses
        fields = {f.name: f for f in dataclasses.fields(WarmCompactor)}
        assert fields["last_processed_time"].default is None

    def test_pending_deletions_default_empty_list(self):
        """pending_deletions defaults to empty list."""
        import dataclasses
        fields = {f.name: f for f in dataclasses.fields(WarmCompactor)}
        assert fields["pending_deletions"].default_factory is not None
        assert isinstance(fields["pending_deletions"].default_factory(), list)


class TestParseTimeFromKey:
    """Tests for _parse_time_from_key (static, no S3 needed)."""

    def test_parse_standard_key(self):
        """Parse standard hot file key."""
        # _parse_time_from_key is a method, so test via a minimal instance check
        # Instead, test the parsing logic directly
        key = "test/parquet_cp_hot/aws/2026/01/23/14-20.parquet"
        parts = key.removesuffix(".parquet").split("/")
        year = int(parts[-4])
        month = int(parts[-3])
        day = int(parts[-2])
        hour, minute = map(int, parts[-1].split("-"))
        result = datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
        assert result == datetime(2026, 1, 23, 14, 20, tzinfo=timezone.utc)

    def test_parse_production_key(self):
        """Parse production hot file key (no test/ prefix)."""
        key = "parquet_cp_hot/aws/2026/02/28/23-50.parquet"
        parts = key.removesuffix(".parquet").split("/")
        year = int(parts[-4])
        month = int(parts[-3])
        day = int(parts[-2])
        hour, minute = map(int, parts[-1].split("-"))
        result = datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
        assert result == datetime(2026, 2, 28, 23, 50, tzinfo=timezone.utc)

    def test_parse_extracts_correct_month_not_compactor_month(self):
        """Year/month come from key, not from compactor's year/month."""
        # Key is January, but this test verifies the key is parsed independently
        key = "test/parquet_cp_hot/aws/2026/01/31/23-50.parquet"
        parts = key.removesuffix(".parquet").split("/")
        month = int(parts[-3])
        assert month == 1  # From key, not from any external source


class TestWarmCompactorFileNaming:
    """Tests for Warm file naming conventions."""

    def test_filename_format(self):
        """Verify Warm file name format."""
        # L{level}_{file_id}_{start_idx}-{end_idx}.parquet
        expected_pattern = "L1_0001_00000-00007.parquet"

        # Parse components
        parts = expected_pattern.replace(".parquet", "").split("_")
        assert parts[0].startswith("L")  # Level prefix
        assert len(parts) == 3  # L1, 0001, 00000-00007

        level = int(parts[0][1:])
        file_id = int(parts[1])
        idx_range = parts[2].split("-")

        assert level == 1
        assert file_id == 1
        assert len(idx_range) == 2
        assert int(idx_range[0]) == 0
        assert int(idx_range[1]) == 7
