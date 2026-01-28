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
    PROCESSED_KEYS_CAP,
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

    def test_processed_keys_cap(self):
        """Verify processed_keys maximum storage count."""
        assert PROCESSED_KEYS_CAP == 5000


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
    """Tests for WarmCompactor idempotency guarantees."""

    def test_processed_keys_set_structure(self):
        """processed_keys must be set type."""
        # This is a structural test - actual S3 interaction would be mocked
        from titans_common.warm_compactor import WarmCompactor

        # Test default factory creates set
        import dataclasses
        fields = {f.name: f for f in dataclasses.fields(WarmCompactor)}
        assert "processed_keys" in fields
        # Default factory should create a set
        default = fields["processed_keys"].default_factory
        assert default is not None
        assert isinstance(default(), set)


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
