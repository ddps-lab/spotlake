"""Single entrypoint for monthly cold freezer coordinator and worker modes."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def _truthy(value: str | None) -> bool:
    return value is not None and value.lower() in {"1", "true", "yes", "on"}


def _ensure_import_paths():
    package_root = Path(__file__).parent
    utility_parent = package_root.parent
    if str(utility_parent) not in sys.path:
        sys.path.insert(0, str(utility_parent))


_ensure_import_paths()

from monthly_cold_freezer.coordinator import run_coordinator  # noqa: E402
from monthly_cold_freezer.freezer_worker import run_freeze  # noqa: E402
from monthly_cold_freezer.signals import parse_target_month, previous_month_target  # noqa: E402


def parse_args():
    parser = argparse.ArgumentParser(description="Monthly cold freezer coordinator/worker")
    parser.add_argument(
        "--mode",
        choices=["coordinator", "worker"],
        default=os.getenv("MODE", "coordinator"),
        help="Execution mode",
    )
    parser.add_argument(
        "--month",
        default=os.getenv("TARGET_MONTH"),
        help="Target month in YYYY-MM format. Defaults to previous UTC month.",
    )
    parser.add_argument(
        "--provider",
        choices=["aws", "azure", "gcp"],
        default=os.getenv("FREEZE_PROVIDER"),
        help="Provider for worker mode",
    )
    parser.add_argument(
        "--env",
        choices=["production", "test"],
        default=os.getenv("FREEZE_ENV", "production"),
        help="Warm/progress environment prefix",
    )
    parser.add_argument("--profile", default=os.getenv("AWS_PROFILE"))
    parser.add_argument("--output-dir", default=os.getenv("FREEZE_OUTPUT_DIR"))
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--ignore-completeness", action="store_true")
    parser.add_argument("--overwrite-existing", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.mode == "coordinator":
        summary = run_coordinator(
            month=args.month,
            env=args.env,
            profile=args.profile,
            titans_bucket=os.getenv("TITANS_BUCKET", "titans-spotlake-data"),
            raw_bucket=os.getenv("RAW_BUCKET", "spotlake"),
        )
        print(f"[batch] Coordinator summary: {summary}")
        return

    if not args.provider:
        raise SystemExit("--provider is required in worker mode")

    target = parse_target_month(args.month) if args.month else previous_month_target()
    ignore_completeness = (
        args.ignore_completeness
        or _truthy(os.getenv("IGNORE_COMPLETENESS"))
        or args.provider == "gcp"
    )
    overwrite_existing = args.overwrite_existing or _truthy(os.getenv("OVERWRITE_EXISTING"))
    skip_upload = args.skip_upload or _truthy(os.getenv("SKIP_UPLOAD"))

    result = run_freeze(
        year=target.year,
        month=target.month,
        provider=args.provider,
        profile=args.profile,
        env=args.env,
        output_dir=args.output_dir,
        skip_upload=skip_upload,
        ignore_completeness=ignore_completeness,
        overwrite_existing=overwrite_existing,
    )
    print(
        "[batch] Worker complete: "
        f"provider={result.provider}, month={target.label}, cp_rows={result.cp_rows}, ap_rows={result.ap_rows}"
    )


if __name__ == "__main__":
    main()
