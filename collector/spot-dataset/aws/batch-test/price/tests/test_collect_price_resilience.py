import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd


MODULE_PATH = Path(__file__).resolve().parents[1] / "collect_price.py"


def load_module():
    utility_module = types.ModuleType("utility")
    slack_module = types.ModuleType("utility.slack_msg_sender")

    def _noop(_msg):
        return None

    slack_module.send_slack_message = _noop
    utility_module.slack_msg_sender = slack_module

    load_price_module = types.ModuleType("load_price")
    load_price_module.get_spot_price = lambda region: pd.DataFrame(
        {"InstanceType": ["c6a.large"], "AZ": [f"{region}-az1"], "SpotPrice": [0.1]}
    )
    load_price_module.get_regions = lambda _session: ["us-east-1", "eu-west-1"]

    sys.modules.setdefault("utility", utility_module)
    sys.modules["utility.slack_msg_sender"] = slack_module
    sys.modules["load_price"] = load_price_module

    spec = importlib.util.spec_from_file_location("collect_price_batch_test", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class TestCollectPriceResilience(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.mod = load_module()

    def test_apply_region_fallback_applies_snapshot_rows(self):
        current_df = pd.DataFrame(
            {
                "Region": ["us-east-1"],
                "InstanceType": ["c6a.large"],
                "OndemandPrice": [0.2],
            }
        )
        snapshot_df = pd.DataFrame(
            {
                "Region": ["eu-west-1", "us-east-1"],
                "InstanceType": ["c6a.large", "c6a.large"],
                "OndemandPrice": [0.3, 9.9],
            }
        )

        merged, applied, unresolved = self.mod.apply_region_fallback(
            current_df=current_df,
            snapshot_df=snapshot_df,
            failed_regions=["eu-west-1"],
            dataset_name="ondemand_price",
            key_columns=["Region", "InstanceType"],
        )

        self.assertEqual(applied, ["eu-west-1"])
        self.assertEqual(unresolved, [])
        self.assertEqual(len(merged), 2)
        self.assertIn("eu-west-1", merged["Region"].tolist())

    def test_apply_region_fallback_unresolved_when_snapshot_empty(self):
        current_df = pd.DataFrame(
            {"Region": ["us-east-1"], "InstanceType": ["c6a.large"], "OndemandPrice": [0.2]}
        )
        snapshot_df = pd.DataFrame()

        merged, applied, unresolved = self.mod.apply_region_fallback(
            current_df=current_df,
            snapshot_df=snapshot_df,
            failed_regions=["eu-west-1"],
            dataset_name="ondemand_price",
            key_columns=["Region", "InstanceType"],
        )

        self.assertEqual(len(merged), 1)
        self.assertEqual(applied, [])
        self.assertEqual(unresolved, ["eu-west-1"])

    def test_apply_region_fallback_prefers_current_rows(self):
        current_df = pd.DataFrame(
            {
                "Region": ["us-east-1"],
                "InstanceType": ["c6a.large"],
                "AZ": ["use1-az1"],
                "SpotPrice": [0.25],
            }
        )
        snapshot_df = pd.DataFrame(
            {
                "Region": ["us-east-1"],
                "InstanceType": ["c6a.large"],
                "AZ": ["use1-az1"],
                "SpotPrice": [0.99],
            }
        )

        merged, _, _ = self.mod.apply_region_fallback(
            current_df=current_df,
            snapshot_df=snapshot_df,
            failed_regions=["us-east-1"],
            dataset_name="spot_price",
            key_columns=["Region", "InstanceType", "AZ"],
        )

        self.assertEqual(len(merged), 1)
        self.assertEqual(float(merged.iloc[0]["SpotPrice"]), 0.25)

    def test_retry_with_backoff_retries_before_success(self):
        state = {"count": 0}

        def flaky():
            state["count"] += 1
            if state["count"] < 3:
                raise RuntimeError("temporary")
            return "ok"

        with mock.patch("time.sleep", return_value=None):
            result = self.mod.retry_with_backoff(flaky, label="flaky", max_attempts=5)

        self.assertEqual(result, "ok")
        self.assertEqual(state["count"], 3)

    def test_retry_with_backoff_returns_attempts(self):
        state = {"count": 0}

        def flaky():
            state["count"] += 1
            if state["count"] < 2:
                raise RuntimeError("temporary")
            return "ok"

        with mock.patch("time.sleep", return_value=None):
            result, attempts = self.mod.retry_with_backoff(
                flaky,
                label="flaky",
                max_attempts=3,
                return_attempts=True,
            )

        self.assertEqual(result, "ok")
        self.assertEqual(attempts, 2)

    def test_throttling_backoff_multiplier_applied(self):
        class FakeThrottleError(Exception):
            def __init__(self):
                self.response = {"Error": {"Code": "ThrottlingException"}}
                super().__init__("throttled")

        calls = {"count": 0}

        def flaky():
            calls["count"] += 1
            if calls["count"] < 2:
                raise FakeThrottleError()
            return "ok"

        with mock.patch("random.uniform", return_value=0.0):
            with mock.patch("time.sleep", return_value=None) as sleep_mock:
                result = self.mod.retry_with_backoff(flaky, label="throttle", max_attempts=3)

        self.assertEqual(result, "ok")
        sleep_mock.assert_called_once_with(2.0)


if __name__ == "__main__":
    unittest.main()
