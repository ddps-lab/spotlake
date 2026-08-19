from datetime import datetime, timezone
import importlib.util
from pathlib import Path

import pandas as pd


def _load_partial_snapshot_module():
    module_path = Path(__file__).parents[1] / "merge" / "partial_snapshot.py"
    spec = importlib.util.spec_from_file_location(
        "azure_partial_snapshot_test", module_path
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_partial_snapshot_keeps_price_and_if_and_nulls_sps_fields():
    partial_snapshot = _load_partial_snapshot_module()
    collected = pd.DataFrame(
        {
            "InstanceTier": ["Standard"],
            "InstanceType": ["D2s_v5"],
            "Region": ["East US"],
            "OndemandPrice": [0.12],
            "SpotPrice": [0.03],
            "Savings": [75.0],
            "IF": [4.2],
        }
    )

    result = partial_snapshot.build_sps_unavailable_snapshot(
        collected,
        datetime(2026, 8, 3, 2, 20, tzinfo=timezone.utc),
    )

    assert result.loc[0, "OndemandPrice"] == 0.12
    assert result.loc[0, "SpotPrice"] == 0.03
    assert result.loc[0, "Savings"] == 75.0
    assert result.loc[0, "IF"] == 4.2
    assert result.loc[0, "Time"] == "2026-08-03 02:20:00"
    assert result.loc[0, "InstanceType"] == "D2s_v5"

    for column in partial_snapshot.SPS_DEPENDENT_COLUMNS:
        assert pd.isna(result.loc[0, column]), column


def test_price_and_if_sources_merge_with_real_collector_columns():
    partial_snapshot = _load_partial_snapshot_module()
    price_df = pd.DataFrame(
        {
            "InstanceTier": ["Standard"],
            "InstanceType": ["D2s_v5"],
            "Region": ["East US"],
            "armRegionName": ["eastus"],
            "OndemandPrice": [0.12],
            "SpotPrice": [0.03],
            "Savings": [75.0],
        }
    )
    if_df = pd.DataFrame(
        {
            "InstanceTier": ["standard"],
            "InstanceType": ["d2s_v5"],
            "Region": ["eastus"],
            "OndemandPrice": [-1.0],
            "SpotPrice": [-1.0],
            "Savings": [1.0],
            "IF": [2.5],
        }
    )

    result = partial_snapshot.merge_price_and_if(price_df, if_df)

    assert result.to_dict(orient="records") == [
        {
            "InstanceTier": "Standard",
            "InstanceType": "D2s_v5",
            "Region": "East US",
            "OndemandPrice": 0.12,
            "SpotPrice": 0.03,
            "Savings": 75.0,
            "IF": 2.5,
        }
    ]


def test_price_only_source_is_preserved_with_missing_if():
    partial_snapshot = _load_partial_snapshot_module()
    price_df = pd.DataFrame(
        {
            "InstanceTier": ["Standard"],
            "InstanceType": ["D2s_v5"],
            "Region": ["East US"],
            "armRegionName": ["eastus"],
            "OndemandPrice": [0.12],
            "SpotPrice": [0.03],
            "Savings": [75.0],
        }
    )

    merged = partial_snapshot.merge_price_and_if(price_df, None)
    result = partial_snapshot.build_sps_unavailable_snapshot(
        merged,
        datetime(2026, 8, 3, 2, 20, tzinfo=timezone.utc),
    )

    assert result.loc[0, "SpotPrice"] == 0.03
    assert pd.isna(result.loc[0, "IF"])


def test_if_only_source_does_not_keep_dummy_price_values():
    partial_snapshot = _load_partial_snapshot_module()
    if_df = pd.DataFrame(
        {
            "InstanceTier": ["Standard"],
            "InstanceType": ["D2s_v5"],
            "Region": ["eastus"],
            "OndemandPrice": [-1.0],
            "SpotPrice": [-1.0],
            "Savings": [1.0],
            "IF": [2.5],
        }
    )

    merged = partial_snapshot.merge_price_and_if(None, if_df)
    result = partial_snapshot.build_sps_unavailable_snapshot(
        merged,
        datetime(2026, 8, 3, 2, 20, tzinfo=timezone.utc),
    )

    assert result.loc[0, "IF"] == 2.5
    assert pd.isna(result.loc[0, "OndemandPrice"])
    assert pd.isna(result.loc[0, "SpotPrice"])
    assert pd.isna(result.loc[0, "Savings"])


def test_partial_snapshot_converts_missing_source_sentinels_to_null():
    partial_snapshot = _load_partial_snapshot_module()
    collected = pd.DataFrame(
        {
            "InstanceTier": ["Standard"],
            "InstanceType": ["D2s_v5"],
            "Region": ["East US"],
            "OndemandPrice": [-1],
            "SpotPrice": [0.03],
            "Savings": [-1],
            "IF": [-1],
        }
    )

    result = partial_snapshot.build_sps_unavailable_snapshot(
        collected,
        datetime(2026, 8, 3, 2, 20, tzinfo=timezone.utc),
    )

    assert pd.isna(result.loc[0, "OndemandPrice"])
    assert result.loc[0, "SpotPrice"] == 0.03
    assert pd.isna(result.loc[0, "Savings"])
    assert pd.isna(result.loc[0, "IF"])


def test_partial_snapshot_rejects_empty_price_and_if_result():
    partial_snapshot = _load_partial_snapshot_module()

    try:
        partial_snapshot.build_sps_unavailable_snapshot(
            pd.DataFrame(),
            datetime(2026, 8, 3, 2, 20, tzinfo=timezone.utc),
        )
    except ValueError as error:
        assert "Price or IF" in str(error)
    else:
        raise AssertionError("empty partial snapshot must fail")


def test_partial_sps_snapshot_keeps_successes_and_nulls_only_failed_request_range():
    partial_snapshot = _load_partial_snapshot_module()
    price_and_if = pd.DataFrame(
        {
            "InstanceTier": ["Standard", "Standard"],
            "InstanceType": ["D2s_v5", "D4s_v5"],
            "Region": ["East US", "West US"],
            "OndemandPrice": [0.12, 0.24],
            "SpotPrice": [0.03, 0.06],
            "Savings": [75.0, 75.0],
            "IF": [2.5, 3.5],
        }
    )
    partial_sps = pd.DataFrame(
        {
            "InstanceTier": ["Standard", "Standard"],
            "InstanceType": ["D2s_v5", "D4s_v5"],
            "Region": ["East US", "West US"],
            "RegionCodeSPS": ["eastus", "westus"],
            "InstanceTypeSPS": ["Standard_D2s_v5", "Standard_D4s_v5"],
            "DesiredCount": [40, 40],
            "AvailabilityZone": ["1", pd.NA],
            "Score": [3, pd.NA],
            "T2": [40, pd.NA],
            "T3": [40, pd.NA],
            "time": ["2026-08-04 10:40:00", "2026-08-04 10:40:00"],
        }
    )

    result = partial_snapshot.build_partial_sps_snapshot(
        price_and_if,
        partial_sps,
        datetime(2026, 8, 4, 10, 40, tzinfo=timezone.utc),
    ).sort_values("InstanceType").reset_index(drop=True)

    successful = result.loc[result["InstanceType"] == "D2s_v5"].iloc[0]
    missing = result.loc[result["InstanceType"] == "D4s_v5"].iloc[0]

    assert successful["Score"] == 3
    assert successful["T2"] == 40
    assert successful["T3"] == 40
    assert successful["AvailabilityZone"] == "1"
    assert missing["OndemandPrice"] == 0.24
    assert missing["SpotPrice"] == 0.06
    assert missing["IF"] == 3.5
    assert missing["DesiredCount"] == 40
    assert pd.isna(missing["Score"])
    assert pd.isna(missing["T2"])
    assert pd.isna(missing["T3"])
    assert pd.isna(missing["AvailabilityZone"])
