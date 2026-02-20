"""Tests for titans_common.utils."""
import pandas as pd
import pytest

from titans_common.utils import filter_sentinel_rows, prepare_for_upload


class TestFilterSentinelRows:
    """Tests for filter_sentinel_rows()."""

    def test_drops_rows_with_minus_one(self):
        df = pd.DataFrame({
            "InstanceType": ["a", "b", "c"],
            "SPS": [3, -1, 5],
            "SpotPrice": [0.05, 0.03, 0.04],
        })
        result, dropped = filter_sentinel_rows(df, ["SPS", "SpotPrice"])
        assert dropped == 1
        assert len(result) == 2
        assert list(result["InstanceType"]) == ["a", "c"]

    def test_drops_when_any_column_is_sentinel(self):
        df = pd.DataFrame({
            "SPS": [3, 5],
            "SpotPrice": [0.05, -1],
            "OndemandPrice": [0.10, 0.10],
        })
        result, dropped = filter_sentinel_rows(df, ["SPS", "SpotPrice", "OndemandPrice"])
        assert dropped == 1
        assert len(result) == 1

    def test_zero_is_not_sentinel(self):
        """T3/T2 use fillna(0), so 0 should NOT be filtered."""
        df = pd.DataFrame({
            "SPS": [3, 5],
            "T3": [0, 50],
            "T2": [0, 0],
        })
        result, dropped = filter_sentinel_rows(df, ["SPS", "T3", "T2"])
        assert dropped == 0
        assert len(result) == 2

    def test_empty_df(self):
        df = pd.DataFrame()
        result, dropped = filter_sentinel_rows(df, ["SPS"])
        assert dropped == 0
        assert result.empty

    def test_no_matching_columns(self):
        df = pd.DataFrame({"InstanceType": ["a", "b"], "Region": ["us", "eu"]})
        result, dropped = filter_sentinel_rows(df, ["SPS", "SpotPrice"])
        assert dropped == 0
        assert len(result) == 2

    def test_all_rows_filtered(self):
        df = pd.DataFrame({"SPS": [-1, -1], "IF": [-1, -1]})
        result, dropped = filter_sentinel_rows(df, ["SPS", "IF"])
        assert dropped == 2
        assert result.empty

    def test_custom_sentinel(self):
        df = pd.DataFrame({"val": [0, 99, 1]})
        result, dropped = filter_sentinel_rows(df, ["val"], sentinel=99)
        assert dropped == 1
        assert len(result) == 2


class TestPrepareForUploadWithSentinelFiltering:
    """Tests for prepare_for_upload() with value_columns parameter."""

    def _make_changed(self):
        return pd.DataFrame({
            "InstanceType": ["a", "b", "c"],
            "Region": ["us", "us", "us"],
            "AZ": ["az1", "az1", "az1"],
            "Time": ["2026-01-01"] * 3,
            "SPS": [3, -1, 5],
            "SpotPrice": [0.05, 0.03, 0.04],
            "OndemandPrice": [0.10, -1, 0.08],
        })

    def _make_removed(self):
        return pd.DataFrame({
            "InstanceType": ["d"],
            "Region": ["us"],
            "AZ": ["az1"],
            "Time": ["2026-01-01"],
            "SPS": [0],
            "SpotPrice": [0],
            "OndemandPrice": [0],
            "Ceased": [True],
        })

    def test_filters_sentinel_from_changed(self):
        changed = self._make_changed()
        removed = self._make_removed()
        result = prepare_for_upload(changed, removed, value_columns=["SPS", "SpotPrice", "OndemandPrice"])
        # b has SPS=-1 and OndemandPrice=-1 → filtered
        instance_types = list(result["InstanceType"])
        assert "b" not in instance_types
        assert "a" in instance_types
        assert "c" in instance_types
        assert "d" in instance_types  # removed row preserved

    def test_removed_df_preserved(self):
        """removed_df rows (Ceased=True, features=0) are never filtered."""
        changed = self._make_changed()
        removed = self._make_removed()
        result = prepare_for_upload(changed, removed, value_columns=["SPS", "SpotPrice", "OndemandPrice"])
        ceased_rows = result[result["Ceased"] == True]
        assert len(ceased_rows) == 1
        assert ceased_rows.iloc[0]["InstanceType"] == "d"

    def test_no_filtering_without_value_columns(self):
        """Default behavior (value_columns=None) keeps all rows."""
        changed = self._make_changed()
        removed = self._make_removed()
        result = prepare_for_upload(changed, removed)
        assert len(result) == 4  # 3 changed + 1 removed, no filtering

    def test_all_changed_filtered_only_removed_remains(self):
        changed = pd.DataFrame({
            "InstanceType": ["a"],
            "SPS": [-1],
            "SpotPrice": [-1],
        })
        removed = self._make_removed()
        result = prepare_for_upload(changed, removed, value_columns=["SPS", "SpotPrice"])
        # changed fully filtered → only removed remains
        assert len(result) == 1
        assert result.iloc[0]["Ceased"] == True


class TestPrepareForUploadCeasedDedup:
    """Tests for Ceased duplicate dedup with explicit pk_columns."""

    PK = ["InstanceType", "Region", "AZ"]

    def test_dedup_keeps_ceased_false(self):
        """Same PK+Time with Ceased=True and False → keep False."""
        changed = pd.DataFrame({
            "InstanceType": ["a"], "Region": ["us"], "AZ": ["az1"],
            "Time": ["2026-01-01"], "SPS": [5], "SpotPrice": [0.05],
        })
        removed = pd.DataFrame({
            "InstanceType": ["a"], "Region": ["us"], "AZ": ["az1"],
            "Time": ["2026-01-01"], "SPS": [0], "SpotPrice": [0.0],
            "Ceased": [True],
        })
        result = prepare_for_upload(changed, removed, pk_columns=self.PK)
        assert len(result) == 1
        assert result.iloc[0]["Ceased"] == False
        assert result.iloc[0]["SPS"] == 5

    def test_no_dedup_without_pk_columns(self):
        """Without pk_columns, duplicates are NOT deduped (backward compat)."""
        changed = pd.DataFrame({
            "InstanceType": ["a"], "Region": ["us"], "AZ": ["az1"],
            "Time": ["2026-01-01"], "SPS": [5], "SpotPrice": [0.05],
        })
        removed = pd.DataFrame({
            "InstanceType": ["a"], "Region": ["us"], "AZ": ["az1"],
            "Time": ["2026-01-01"], "SPS": [0], "SpotPrice": [0.0],
            "Ceased": [True],
        })
        result = prepare_for_upload(changed, removed)
        assert len(result) == 2  # no dedup

    def test_dedup_different_pk_not_deduped(self):
        """Different PKs with same Time → both kept."""
        changed = pd.DataFrame({
            "InstanceType": ["a"], "Region": ["us"], "AZ": ["az1"],
            "Time": ["2026-01-01"], "SPS": [5], "SpotPrice": [0.05],
        })
        removed = pd.DataFrame({
            "InstanceType": ["b"], "Region": ["us"], "AZ": ["az1"],
            "Time": ["2026-01-01"], "SPS": [0], "SpotPrice": [0.0],
            "Ceased": [True],
        })
        result = prepare_for_upload(changed, removed, pk_columns=self.PK)
        assert len(result) == 2

    def test_dedup_different_time_not_deduped(self):
        """Same PK with different Times → both kept (normal case)."""
        changed = pd.DataFrame({
            "InstanceType": ["a"], "Region": ["us"], "AZ": ["az1"],
            "Time": ["2026-01-01 00:10:00"], "SPS": [5], "SpotPrice": [0.05],
        })
        removed = pd.DataFrame({
            "InstanceType": ["a"], "Region": ["us"], "AZ": ["az1"],
            "Time": ["2026-01-01 00:00:00"], "SPS": [0], "SpotPrice": [0.0],
            "Ceased": [True],
        })
        result = prepare_for_upload(changed, removed, pk_columns=self.PK)
        assert len(result) == 2

    def test_dedup_multiple_duplicates(self):
        """Multiple PKs with duplicates → all deduped correctly."""
        changed = pd.DataFrame({
            "InstanceType": ["a", "b"], "Region": ["us", "eu"], "AZ": ["az1", "az2"],
            "Time": ["2026-01-01"] * 2, "SPS": [5, 3], "SpotPrice": [0.05, 0.03],
        })
        removed = pd.DataFrame({
            "InstanceType": ["a", "b"], "Region": ["us", "eu"], "AZ": ["az1", "az2"],
            "Time": ["2026-01-01"] * 2, "SPS": [0, 0], "SpotPrice": [0.0, 0.0],
            "Ceased": [True, True],
        })
        result = prepare_for_upload(changed, removed, pk_columns=self.PK)
        assert len(result) == 2
        assert all(result["Ceased"] == False)
