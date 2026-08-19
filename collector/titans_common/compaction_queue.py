#!/usr/bin/env python3
"""Durable, ordered runner for warm-compaction requests."""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from typing import Callable

import boto3
from botocore.exceptions import ClientError


DEFAULT_BUCKET = os.environ.get("TITANS_BUCKET", "titans-spotlake-data")
PENDING_ROOT = "compaction_requests"
RunRequest = Callable[[Path], int]
NotifyFailure = Callable[..., None]


@dataclass(frozen=True)
class ProcessResult:
    processed: int = 0
    failed_key: str | None = None
    return_code: int = 0

    @property
    def exit_code(self) -> int:
        return _display_exit_code(self.return_code)


def _display_exit_code(return_code: int) -> int:
    return 128 + (-return_code) if return_code < 0 else return_code


def _load_request(path: Path) -> dict:
    request = json.loads(path.read_text(encoding="utf-8"))
    for field in ("provider", "hot_key", "timestamp"):
        if not request.get(field):
            raise ValueError(f"compaction request is missing {field}: {path}")
    return request


def _pending_key(request: dict) -> str:
    timestamp = datetime.fromisoformat(request["timestamp"].replace("Z", "+00:00"))
    return (
        f"{PENDING_ROOT}/{request['provider']}/pending/"
        f"{timestamp:%Y/%m/%d/%H-%M-%S}.json"
    )


def enqueue_request(
    request_path: str | Path,
    *,
    s3_client=None,
    bucket: str = DEFAULT_BUCKET,
) -> str:
    """Store a request before execution so a native crash cannot lose it."""
    path = Path(request_path)
    request = _load_request(path)
    key = _pending_key(request)
    client = s3_client or boto3.client("s3")
    try:
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(request, sort_keys=True).encode("utf-8"),
            ContentType="application/json",
            IfNoneMatch="*",
        )
        print(f"[COMPACTION_QUEUE] enqueued s3://{bucket}/{key}", flush=True)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") not in {
            "PreconditionFailed",
            "ConditionalRequestConflict",
        }:
            raise
        print(f"[COMPACTION_QUEUE] already pending s3://{bucket}/{key}", flush=True)
    return key


def list_pending_keys(
    provider: str,
    *,
    s3_client=None,
    bucket: str = DEFAULT_BUCKET,
) -> list[str]:
    client = s3_client or boto3.client("s3")
    prefix = f"{PENDING_ROOT}/{provider}/pending/"
    keys: list[str] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(item["Key"] for item in page.get("Contents", []))
    return sorted(keys)


def _default_run_request(path: Path) -> int:
    runner = Path(__file__).with_name("run_compaction_request.py")
    completed = subprocess.run(
        [sys.executable, "-u", "-X", "faulthandler", str(runner), "--request", str(path)],
        check=False,
    )
    return completed.returncode


def format_failure_message(
    *,
    provider: str,
    hot_key: str,
    return_code: int,
    job_id: str,
) -> str:
    exit_code = _display_exit_code(return_code)
    if return_code == -11 or exit_code == 139:
        termination = "SIGSEGV (Exit 139)"
    else:
        termination = f"Exit {exit_code}"
    return (
        f":rotating_light: **{provider.capitalize()} Warm Compaction Failed**\n"
        "**SpotLake 원본 수집: 완료**\n"
        f"**Warm compaction: 실패** ({termination})\n"
        f"**대상 Hot 파일:** `{hot_key}`\n"
        "**영향:** Hot 파일은 저장됐으며 warm tier 반영을 대기함\n"
        "**보호 동작:** 실패 요청을 S3 pending queue에 보존함\n"
        "**복구:** 다음 수집 주기에서 먼저 재시도\n"
        f"**Batch Job:** `{job_id or 'unknown'}`"
    )


def _default_notify_failure(**kwargs) -> None:
    message = format_failure_message(**kwargs)
    try:
        from utility.slack_msg_sender import send_slack_message

        send_slack_message(message)
    except Exception as exc:
        print(f"[COMPACTION_QUEUE] Slack notification failed: {exc}", flush=True)
        print(message, flush=True)


def process_pending(
    provider: str,
    *,
    s3_client=None,
    bucket: str = DEFAULT_BUCKET,
    run_request: RunRequest | None = None,
    notify_failure: NotifyFailure | None = None,
) -> ProcessResult:
    """Process pending requests oldest-first and stop at the first failure."""
    client = s3_client or boto3.client("s3")
    runner = run_request or _default_run_request
    notifier = notify_failure or _default_notify_failure
    processed = 0

    for key in list_pending_keys(provider, s3_client=client, bucket=bucket):
        response = client.get_object(Bucket=bucket, Key=key)
        request_bytes = response["Body"].read()
        request = json.loads(request_bytes)
        with tempfile.TemporaryDirectory(prefix="warm_compaction_request_") as temp_dir:
            request_path = Path(temp_dir) / "request.json"
            request_path.write_bytes(request_bytes)
            print(
                f"[COMPACTION_QUEUE] processing key={key} hot_key={request['hot_key']}",
                flush=True,
            )
            return_code = runner(request_path)

        if return_code != 0:
            notifier(
                provider=provider,
                hot_key=request["hot_key"],
                return_code=return_code,
                job_id=os.environ.get("AWS_BATCH_JOB_ID", ""),
            )
            print(
                f"[COMPACTION_QUEUE] deferred key={key} "
                f"exit_code={_display_exit_code(return_code)}",
                flush=True,
            )
            return ProcessResult(
                processed=processed,
                failed_key=key,
                return_code=return_code,
            )

        client.delete_object(Bucket=bucket, Key=key)
        processed += 1
        print(f"[COMPACTION_QUEUE] completed key={key}", flush=True)

    return ProcessResult(processed=processed)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--request", required=True, help="Path to a compaction request JSON")
    args = parser.parse_args()
    request_path = Path(args.request)
    request = _load_request(request_path)
    enqueue_request(request_path)
    result = process_pending(request["provider"])
    if result.failed_key:
        # The raw and Hot data are durable. Keep the Batch job successful and
        # retry this queued request before newer compactions next cycle.
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
