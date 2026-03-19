#!/usr/bin/env python3
"""Run a single warm compaction directly from provider/hot-key/timestamp args."""
from __future__ import annotations

import argparse
from datetime import datetime
import resource
import sys
from pathlib import Path

import boto3

COLLECTOR_ROOT = Path(__file__).resolve().parents[1]
if str(COLLECTOR_ROOT) not in sys.path:
    sys.path.insert(0, str(COLLECTOR_ROOT))

from titans_common.warm_compactor import ConcurrencyConflictError, run_compaction


def _rss_mb() -> float:
    try:
        with open("/proc/self/status", "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) / 1024.0
    except OSError:
        pass
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--provider", required=True)
    parser.add_argument("--hot-key", required=True)
    parser.add_argument("--timestamp", required=True)
    parser.add_argument("--timeout-seconds", type=float, default=120.0)
    args = parser.parse_args()

    timestamp = datetime.fromisoformat(args.timestamp.replace("Z", "+00:00"))
    print(
        f"[TITANS/{args.provider}] direct compaction start "
        f"hot_key={args.hot_key} timestamp={timestamp.isoformat()} rss_mb={_rss_mb():.1f}",
        flush=True,
    )
    try:
        run_compaction(
            args.hot_key,
            timestamp,
            provider=args.provider,
            timeout_seconds=args.timeout_seconds,
            s3_client=boto3.client("s3"),
        )
    except ConcurrencyConflictError as exc:
        print(
            f"[TITANS/{args.provider}] direct compaction concurrency_conflict "
            f"hot_key={args.hot_key} error={exc}",
            flush=True,
        )
        return 0
    print(
        f"[TITANS/{args.provider}] direct compaction end "
        f"hot_key={args.hot_key} rss_mb={_rss_mb():.1f}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
