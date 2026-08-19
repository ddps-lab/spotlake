import sys
import unittest
from pathlib import Path

UTILITY_PARENT = Path(__file__).resolve().parents[2]
if str(UTILITY_PARENT) not in sys.path:
    sys.path.insert(0, str(UTILITY_PARENT))

from monthly_cold_freezer.benchmark_runtime import (
    TargetMonth,
    compute_lane_costs,
    coverage_ratio,
    extrapolate_full_month_seconds,
)


class BenchmarkRuntimeTests(unittest.TestCase):
    def test_coverage_ratio_is_clamped(self):
        target = TargetMonth(year=2026, month=3)
        self.assertEqual(coverage_ratio(target, None), 0.0)
        self.assertAlmostEqual(
            coverage_ratio(target, target.warm_manifest_threshold.replace(day=1, hour=0, minute=0)),
            0.0,
        )

    def test_extrapolation_scales_by_coverage(self):
        self.assertAlmostEqual(extrapolate_full_month_seconds(75.0, 0.5), 150.0)
        self.assertAlmostEqual(extrapolate_full_month_seconds(75.0, 0.0), 75.0)

    def test_compute_lane_costs_uses_max_small_runtime(self):
        measurements = {
            "aws": {"estimated_full_month_seconds": 75.0},
            "gcp": {"estimated_full_month_seconds": 8.0},
            "azure": {"estimated_full_month_seconds": 110.0},
        }
        rate_book = {
            "r7g.xlarge": {"on_demand_hourly_usd": 0.2, "spot": {"latest_avg_hourly_usd": 0.05}},
            "x2gd.2xlarge": {"on_demand_hourly_usd": 0.7, "spot": {"latest_avg_hourly_usd": 0.2}},
            "x2gd.xlarge": {"on_demand_hourly_usd": 0.3, "spot": {"latest_avg_hourly_usd": 0.1}},
            "r8g.4xlarge": {"on_demand_hourly_usd": 0.9, "spot": {"latest_avg_hourly_usd": 0.4}},
        }

        costs = compute_lane_costs(measurements, rate_book)

        self.assertAlmostEqual(costs["small_runtime_seconds"], 75.0)
        self.assertAlmostEqual(costs["heavy_runtime_seconds"], 110.0)
        self.assertGreater(costs["primary"]["total_on_demand_usd"], 0.0)
        self.assertGreater(costs["fallback"]["total_recent_spot_avg_usd"], 0.0)


if __name__ == "__main__":
    unittest.main()
