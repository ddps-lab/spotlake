#!/usr/bin/env python3
"""Run a queued warm-compaction request in a fresh Python process."""
from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import resource
import sys

import boto3

COLLECTOR_ROOT = Path(__file__).resolve().parents[1]
if str(COLLECTOR_ROOT) not in sys.path:
    sys.path.insert(0, str(COLLECTOR_ROOT))

from titans_common.warm_compactor import ConcurrencyConflictError, run_compaction


def _rss_mb() -> float:
    """Return current process RSS in MiB when available."""
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
    parser.add_argument("--request", required=True, help="Path to queued compaction request JSON")
    args = parser.parse_args()

    request_path = Path(args.request)
    request = json.loads(request_path.read_text(encoding="utf-8"))
    provider = request["provider"]
    hot_key = request["hot_key"]
    timestamp = datetime.fromisoformat(request["timestamp"])
    timeout_seconds = float(request.get("timeout_seconds", 30.0))

    print(
        f"[TITANS/{provider}] separate-process compaction start "
        f"hot_key={hot_key} timestamp={timestamp.isoformat()} rss_mb={_rss_mb():.1f}",
        flush=True,
    )

    try:
        run_compaction(
            hot_key,
            timestamp,
            provider=provider,
            timeout_seconds=timeout_seconds,
            s3_client=boto3.client("s3"),
        )
    except ConcurrencyConflictError as exc:
        print(
            f"[TITANS/{provider}] separate-process compaction concurrency_conflict "
            f"hot_key={hot_key} error={exc}",
            flush=True,
        )
        return 0

    print(
        f"[TITANS/{provider}] separate-process compaction end "
        f"hot_key={hot_key} rss_mb={_rss_mb():.1f}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
