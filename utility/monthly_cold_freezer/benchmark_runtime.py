"""Benchmark monthly freezer runtime and estimate Batch cost."""

from __future__ import annotations

import argparse
import json
import resource
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3


def _ensure_import_paths():
    package_root = Path(__file__).parent
    utility_parent = package_root.parent
    if str(utility_parent) not in sys.path:
        sys.path.insert(0, str(utility_parent))


_ensure_import_paths()

from monthly_cold_freezer.freezer_worker import run_freeze  # noqa: E402
from monthly_cold_freezer.signals import (  # noqa: E402
    RAW_BUCKET,
    TITANS_BUCKET,
    TargetMonth,
    parse_target_month,
    previous_month_target,
    read_manifest_last_processed_time,
    read_progress_last_processed_raw_time,
)

AWS_REGION = "us-west-2"
PRICING_REGION = "us-east-1"
LOCATION_NAME = "US West (Oregon)"
PRIMARY_HOSTS = {
    "small": "r7g.xlarge",
    "heavy": "x2gd.2xlarge",
}
FALLBACK_HOSTS = {
    "small": "x2gd.xlarge",
    "heavy": "r8g.4xlarge",
}


def parse_args():
    parser = argparse.ArgumentParser(description="Benchmark monthly cold freezer runtime")
    parser.add_argument(
        "--month",
        default=None,
        help="Target month in YYYY-MM. Defaults to previous UTC month.",
    )
    parser.add_argument(
        "--providers",
        nargs="+",
        choices=["aws", "azure", "gcp"],
        default=["aws", "azure", "gcp"],
    )
    parser.add_argument("--profile")
    parser.add_argument(
        "--env",
        choices=["production", "test"],
        default="production",
    )
    parser.add_argument(
        "--output-root",
        default=".tmp/monthly_freeze_bench",
        help="Root directory for local parquet outputs",
    )
    parser.add_argument(
        "--spot-window-hours",
        type=int,
        default=24,
        help="Window for recent Spot price history averaging",
    )
    parser.add_argument(
        "--json-out",
        help="Optional path to write the benchmark summary JSON",
    )
    parser.add_argument("--child-provider", choices=["aws", "azure", "gcp"], help=argparse.SUPPRESS)
    return parser.parse_args()


def target_month_from_args(month: str | None) -> TargetMonth:
    return parse_target_month(month) if month else previous_month_target()


def month_bounds(target: TargetMonth) -> tuple[datetime, datetime]:
    start = datetime(target.year, target.month, 1, tzinfo=timezone.utc)
    end = datetime(target.next_year, target.next_month, 1, tzinfo=timezone.utc)
    return start, end


def coverage_ratio(target: TargetMonth, completed_until: datetime | None) -> float:
    if completed_until is None:
        return 0.0
    start, end = month_bounds(target)
    clamped = min(max(completed_until, start), end)
    total_seconds = (end - start).total_seconds()
    covered_seconds = (clamped - start).total_seconds()
    if total_seconds <= 0:
        return 0.0
    return max(0.0, min(1.0, covered_seconds / total_seconds))


def extrapolate_full_month_seconds(elapsed_seconds: float, ratio: float) -> float:
    if ratio <= 0:
        return elapsed_seconds
    return elapsed_seconds / ratio


def benchmark_timestamp_for_provider(
    s3,
    target: TargetMonth,
    provider: str,
    *,
    env: str,
    titans_bucket: str = TITANS_BUCKET,
) -> datetime | None:
    if provider == "gcp":
        progress_time = read_progress_last_processed_raw_time(
            s3,
            target,
            provider,
            env=env,
            titans_bucket=titans_bucket,
        )
        if progress_time is not None:
            return progress_time
    return read_manifest_last_processed_time(
        s3,
        target,
        provider,
        env=env,
        titans_bucket=titans_bucket,
    )


def child_measure(provider: str, target: TargetMonth, *, profile: str | None, env: str, output_root: str):
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    s3 = session.client("s3")
    completed_until = benchmark_timestamp_for_provider(
        s3,
        target,
        provider,
        env=env,
    )
    ratio = coverage_ratio(target, completed_until)

    start = time.perf_counter()
    result = run_freeze(
        year=target.year,
        month=target.month,
        provider=provider,
        profile=profile,
        env=env,
        output_dir=Path(output_root) / f"{provider}_{target.label}",
        skip_upload=True,
        ignore_completeness=True,
        overwrite_existing=True,
    )
    elapsed = time.perf_counter() - start
    peak_rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

    payload = {
        "provider": provider,
        "month": target.label,
        "coverage_timestamp": completed_until.isoformat() if completed_until else None,
        "coverage_ratio": ratio,
        "elapsed_seconds": elapsed,
        "peak_rss_kb": peak_rss_kb,
        "peak_rss_gib": peak_rss_kb / 1024 / 1024,
        "cp_rows": result.cp_rows,
        "ap_rows": result.ap_rows,
        "cp_file": str(result.cp_file),
        "ap_file": str(result.ap_file),
        "estimated_full_month_seconds": extrapolate_full_month_seconds(elapsed, ratio),
    }
    print(json.dumps(payload))


def run_child(args):
    target = target_month_from_args(args.month)
    child_measure(
        args.child_provider,
        target,
        profile=args.profile,
        env=args.env,
        output_root=args.output_root,
    )


def run_measurement_subprocess(provider: str, args) -> dict:
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--child-provider",
        provider,
        "--env",
        args.env,
        "--output-root",
        args.output_root,
    ]
    if args.month:
        command.extend(["--month", args.month])
    if args.profile:
        command.extend(["--profile", args.profile])
    completed = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
    )
    lines = [line for line in completed.stdout.splitlines() if line.strip()]
    return json.loads(lines[-1])


def fetch_on_demand_hourly_rate(pricing, instance_type: str) -> float:
    response = pricing.get_products(
        ServiceCode="AmazonEC2",
        Filters=[
            {"Type": "TERM_MATCH", "Field": "instanceType", "Value": instance_type},
            {"Type": "TERM_MATCH", "Field": "location", "Value": LOCATION_NAME},
            {"Type": "TERM_MATCH", "Field": "operatingSystem", "Value": "Linux"},
            {"Type": "TERM_MATCH", "Field": "tenancy", "Value": "Shared"},
            {"Type": "TERM_MATCH", "Field": "preInstalledSw", "Value": "NA"},
            {"Type": "TERM_MATCH", "Field": "capacitystatus", "Value": "Used"},
            {"Type": "TERM_MATCH", "Field": "licenseModel", "Value": "No License required"},
        ],
        MaxResults=10,
    )
    products = [json.loads(item) for item in response["PriceList"]]
    chosen = products[0]
    on_demand = next(iter(chosen["terms"]["OnDemand"].values()))
    dimension = next(iter(on_demand["priceDimensions"].values()))
    return float(dimension["pricePerUnit"]["USD"])


def fetch_recent_spot_stats(ec2, instance_type: str, window_hours: int) -> dict:
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=window_hours)
    response = ec2.describe_spot_price_history(
        InstanceTypes=[instance_type],
        ProductDescriptions=["Linux/UNIX"],
        StartTime=start_time,
        EndTime=end_time,
        MaxResults=1000,
    )
    history = response.get("SpotPriceHistory", [])
    latest_by_az = {}
    for item in history:
        az = item["AvailabilityZone"]
        if az not in latest_by_az or item["Timestamp"] > latest_by_az[az]["Timestamp"]:
            latest_by_az[az] = item

    latest_prices = [float(item["SpotPrice"]) for item in latest_by_az.values()]
    all_prices = [float(item["SpotPrice"]) for item in history]
    return {
        "sample_count": len(history),
        "latest_avg_hourly_usd": sum(latest_prices) / len(latest_prices) if latest_prices else None,
        "window_min_hourly_usd": min(all_prices) if all_prices else None,
        "window_max_hourly_usd": max(all_prices) if all_prices else None,
        "latest_by_az_hourly_usd": sorted(latest_prices),
        "window_hours": window_hours,
    }


def fetch_rate_book(profile: str | None, window_hours: int) -> dict:
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    pricing = session.client("pricing", region_name=PRICING_REGION)
    ec2 = session.client("ec2", region_name=AWS_REGION)

    rate_book = {}
    for instance_type in {
        PRIMARY_HOSTS["small"],
        PRIMARY_HOSTS["heavy"],
        FALLBACK_HOSTS["small"],
        FALLBACK_HOSTS["heavy"],
    }:
        rate_book[instance_type] = {
            "on_demand_hourly_usd": fetch_on_demand_hourly_rate(pricing, instance_type),
            "spot": fetch_recent_spot_stats(ec2, instance_type, window_hours),
        }
    return rate_book


def compute_lane_costs(measurements: dict[str, dict], rate_book: dict[str, dict]) -> dict:
    small_runtime = max(
        measurements.get("aws", {}).get("estimated_full_month_seconds", 0.0),
        measurements.get("gcp", {}).get("estimated_full_month_seconds", 0.0),
    )
    heavy_runtime = measurements.get("azure", {}).get("estimated_full_month_seconds", 0.0)

    def lane_cost(instance_type: str, runtime_seconds: float) -> dict:
        on_demand = rate_book[instance_type]["on_demand_hourly_usd"]
        recent_spot = rate_book[instance_type]["spot"]["latest_avg_hourly_usd"]
        return {
            "instance_type": instance_type,
            "runtime_seconds": runtime_seconds,
            "runtime_minutes": runtime_seconds / 60.0,
            "on_demand_usd": on_demand * runtime_seconds / 3600.0,
            "recent_spot_avg_usd": (
                recent_spot * runtime_seconds / 3600.0
                if recent_spot is not None else None
            ),
        }

    primary_small = lane_cost(PRIMARY_HOSTS["small"], small_runtime)
    primary_heavy = lane_cost(PRIMARY_HOSTS["heavy"], heavy_runtime)
    fallback_small = lane_cost(FALLBACK_HOSTS["small"], small_runtime)
    fallback_heavy = lane_cost(FALLBACK_HOSTS["heavy"], heavy_runtime)

    return {
        "small_runtime_seconds": small_runtime,
        "heavy_runtime_seconds": heavy_runtime,
        "primary": {
            "small": primary_small,
            "heavy": primary_heavy,
            "total_on_demand_usd": primary_small["on_demand_usd"] + primary_heavy["on_demand_usd"],
            "total_recent_spot_avg_usd": (
                (primary_small["recent_spot_avg_usd"] or 0.0)
                + (primary_heavy["recent_spot_avg_usd"] or 0.0)
            ),
        },
        "fallback": {
            "small": fallback_small,
            "heavy": fallback_heavy,
            "total_on_demand_usd": fallback_small["on_demand_usd"] + fallback_heavy["on_demand_usd"],
            "total_recent_spot_avg_usd": (
                (fallback_small["recent_spot_avg_usd"] or 0.0)
                + (fallback_heavy["recent_spot_avg_usd"] or 0.0)
            ),
        },
    }


def print_summary(summary: dict):
    target = summary["month"]
    print(f"== Monthly Freezer Benchmark: {target} ==")
    for provider in summary["providers"]:
        item = summary["measurements"][provider]
        print(
            f"{provider}: elapsed={item['elapsed_seconds']:.2f}s, "
            f"full_estimate={item['estimated_full_month_seconds']:.2f}s, "
            f"peak_rss={item['peak_rss_gib']:.2f} GiB, "
            f"coverage={item['coverage_ratio'] * 100:.2f}%"
        )

    primary = summary["lane_costs"]["primary"]
    fallback = summary["lane_costs"]["fallback"]
    print(
        "primary_hosts: "
        f"on_demand=${primary['total_on_demand_usd']:.6f}, "
        f"recent_spot_avg=${primary['total_recent_spot_avg_usd']:.6f}"
    )
    print(
        "fallback_hosts: "
        f"on_demand=${fallback['total_on_demand_usd']:.6f}, "
        f"recent_spot_avg=${fallback['total_recent_spot_avg_usd']:.6f}"
    )


def run_parent(args):
    target = target_month_from_args(args.month)
    measurements = {}
    for provider in args.providers:
        print(f"[benchmark] measuring {provider} for {target.label} ...")
        measurements[provider] = run_measurement_subprocess(provider, args)

    rate_book = fetch_rate_book(args.profile, args.spot_window_hours)
    summary = {
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "month": target.label,
        "providers": args.providers,
        "measurements": measurements,
        "rates": rate_book,
        "lane_costs": compute_lane_costs(measurements, rate_book),
    }
    if args.json_out:
        Path(args.json_out).write_text(json.dumps(summary, indent=2))
    print_summary(summary)


def main():
    args = parse_args()
    if args.child_provider:
        run_child(args)
        return
    run_parent(args)


if __name__ == "__main__":
    main()
