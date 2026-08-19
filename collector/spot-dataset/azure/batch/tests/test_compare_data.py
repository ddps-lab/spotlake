import importlib.util
from pathlib import Path

import pandas as pd


def _load_compare_data_module():
    module_path = Path(__file__).parents[1] / "merge" / "compare_data.py"
    spec = importlib.util.spec_from_file_location("azure_compare_data_test", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_compare_sps_treats_current_snapshot_as_changed_after_partial_snapshot():
    compare_data = _load_compare_data_module()
    previous_partial = pd.DataFrame(
        [
            {
                "InstanceTier": "Standard",
                "InstanceType": "D2s_v5",
                "Region": "eastus",
                "AvailabilityZone": pd.NA,
                "OndemandPrice": 0.10,
                "SpotPrice": 0.03,
                "IF": pd.NA,
                "T2": pd.NA,
                "T3": pd.NA,
                "DesiredCount": pd.NA,
                "Score": pd.NA,
                "Time": "2026-08-03 13:00:00",
            }
        ]
    )
    current_complete = pd.DataFrame(
        [
            {
                "InstanceTier": "Standard",
                "InstanceType": "D2s_v5",
                "Region": "eastus",
                "AvailabilityZone": "1",
                "OndemandPrice": 0.10,
                "SpotPrice": 0.03,
                "IF": 5.0,
                "T2": 40,
                "T3": 35,
                "DesiredCount": 45,
                "Score": 3,
                "Time": "2026-08-03 13:10:00",
            }
        ]
    )

    result = compare_data.compare_sps(
        previous_partial,
        current_complete,
        ["InstanceTier", "InstanceType", "Region", "AvailabilityZone"],
        ["OndemandPrice", "SpotPrice", "IF", "T2", "T3"],
    )

    assert len(result) == 1
    assert result.iloc[0]["AvailabilityZone"] == "1"
    assert result.iloc[0]["Score"] == 3
