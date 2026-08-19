"""Monthly cold freezer coordinator."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

from .signals import (
    ProviderSignal,
    RAW_BUCKET,
    TITANS_BUCKET,
    TargetMonth,
    env_prefix,
    evaluate_all_signals,
    parse_target_month,
    previous_month_target,
)

RUNNING_STATES = {"SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"}
DEFAULT_SMALL_QUEUE = "monthly-cold-freezer-small"
DEFAULT_HEAVY_QUEUE = "monthly-cold-freezer-heavy"
DEFAULT_JOB_DEFINITIONS = {
    "aws": "monthly-cold-freezer-aws-worker",
    "azure": "monthly-cold-freezer-azure-worker",
    "gcp": "monthly-cold-freezer-gcp-worker",
}


@dataclass(frozen=True)
class WorkerTarget:
    provider: str
    job_queue: str
    job_definition: str
    ignore_completeness: bool = True


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def status_key(target: TargetMonth, provider: str, env: str) -> str:
    prefix = env_prefix(env)
    return f"{prefix}ops/monthly_freeze/{target.label}/{provider}.json"


def cold_output_keys(target: TargetMonth, provider: str) -> tuple[str, str]:
    return (
        f"{provider}/{target.cold_cp_suffix}",
        f"{provider}/{target.cold_ap_suffix}",
    )


def _json_dumps(payload: dict) -> bytes:
    return json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")


def _load_json_object(s3, bucket: str, key: str) -> dict | None:
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise
    return json.loads(response["Body"].read())


def load_status(
    s3,
    target: TargetMonth,
    provider: str,
    env: str,
    *,
    titans_bucket: str = TITANS_BUCKET,
) -> dict | None:
    return _load_json_object(s3, titans_bucket, status_key(target, provider, env))


def write_status(
    s3,
    target: TargetMonth,
    provider: str,
    env: str,
    payload: dict,
    *,
    titans_bucket: str = TITANS_BUCKET,
):
    s3.put_object(
        Bucket=titans_bucket,
        Key=status_key(target, provider, env),
        Body=_json_dumps(payload),
        ContentType="application/json",
    )


def _object_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def cold_outputs_complete(
    s3,
    target: TargetMonth,
    provider: str,
    *,
    titans_bucket: str = TITANS_BUCKET,
) -> bool:
    cp_key, ap_key = cold_output_keys(target, provider)
    return _object_exists(s3, titans_bucket, cp_key) and _object_exists(s3, titans_bucket, ap_key)


def describe_job_state(batch, job_id: str | None) -> str | None:
    if not job_id:
        return None
    response = batch.describe_jobs(jobs=[job_id])
    jobs = response.get("jobs", [])
    if not jobs:
        return None
    return jobs[0].get("status")


def decide_provider_action(
    *,
    output_complete: bool,
    stored_state: str | None,
    observed_job_state: str | None,
) -> str:
    effective_state = observed_job_state or stored_state
    if output_complete:
        return "complete"
    if effective_state in RUNNING_STATES:
        return "skip_running"
    if effective_state == "FAILED":
        return "submit_retry"
    if effective_state == "SUCCEEDED":
        return "anomaly"
    if effective_state == "ANOMALY":
        return "skip_anomaly"
    return "submit"


def worker_targets_from_env() -> dict[str, WorkerTarget]:
    small_queue = os.getenv("MONTHLY_FREEZE_SMALL_JOB_QUEUE", DEFAULT_SMALL_QUEUE)
    heavy_queue = os.getenv("MONTHLY_FREEZE_HEAVY_JOB_QUEUE", DEFAULT_HEAVY_QUEUE)
    return {
        "aws": WorkerTarget(
            provider="aws",
            job_queue=small_queue,
            job_definition=os.getenv(
                "MONTHLY_FREEZE_AWS_JOB_DEFINITION",
                DEFAULT_JOB_DEFINITIONS["aws"],
            ),
        ),
        "azure": WorkerTarget(
            provider="azure",
            job_queue=heavy_queue,
            job_definition=os.getenv(
                "MONTHLY_FREEZE_AZURE_JOB_DEFINITION",
                DEFAULT_JOB_DEFINITIONS["azure"],
            ),
        ),
        "gcp": WorkerTarget(
            provider="gcp",
            job_queue=small_queue,
            job_definition=os.getenv(
                "MONTHLY_FREEZE_GCP_JOB_DEFINITION",
                DEFAULT_JOB_DEFINITIONS["gcp"],
            ),
        ),
    }


def submit_worker_job(
    batch,
    target: TargetMonth,
    worker_target: WorkerTarget,
    *,
    env: str,
) -> dict:
    now = datetime.now(timezone.utc)
    job_name = f"monthly-freeze-{worker_target.provider}-{target.label}-{now.strftime('%Y%m%d%H%M%S')}"
    environment = [
        {"name": "MODE", "value": "worker"},
        {"name": "TARGET_MONTH", "value": target.label},
        {"name": "FREEZE_PROVIDER", "value": worker_target.provider},
        {"name": "FREEZE_ENV", "value": env},
        {
            "name": "IGNORE_COMPLETENESS",
            "value": "1" if worker_target.ignore_completeness else "0",
        },
        {"name": "OVERWRITE_EXISTING", "value": "0"},
        {"name": "SKIP_UPLOAD", "value": "0"},
    ]
    response = batch.submit_job(
        jobName=job_name,
        jobQueue=worker_target.job_queue,
        jobDefinition=worker_target.job_definition,
        containerOverrides={"environment": environment},
    )
    return {
        "job_name": job_name,
        "job_id": response["jobId"],
        "job_queue": worker_target.job_queue,
        "job_definition": worker_target.job_definition,
    }


def _send_anomaly_alert(message: str):
    if os.getenv("MONTHLY_FREEZE_DISABLE_SLACK_ALERTS", "0") == "1":
        return
    try:
        from slack_msg_sender import send_slack_message
    except Exception as exc:  # pragma: no cover - optional runtime dependency
        print(f"[coordinator] Slack helper unavailable: {exc}")
        return
    try:
        send_slack_message(message)
    except Exception as exc:  # pragma: no cover - best effort only
        print(f"[coordinator] Failed to send Slack alert: {exc}")


def _signal_summary(signal: ProviderSignal) -> str:
    return (
        f"{signal.provider}: ready={signal.ready}, threshold={signal.threshold}, "
        f"reason={signal.reason}"
    )


def _target_from_label(label: str | None) -> TargetMonth:
    if label:
        return parse_target_month(label)
    return previous_month_target()


def run_coordinator(
    *,
    month: str | None = None,
    env: str = "production",
    profile: str | None = None,
    titans_bucket: str = TITANS_BUCKET,
    raw_bucket: str = RAW_BUCKET,
) -> dict:
    target = _target_from_label(month)
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    s3 = session.client("s3")
    batch = session.client("batch")

    signals = evaluate_all_signals(
        s3,
        target,
        env=env,
        titans_bucket=titans_bucket,
        raw_bucket=raw_bucket,
    )

    print(f"[coordinator] Target month: {target.label}")
    for provider in ("aws", "azure", "gcp"):
        print(f"[coordinator] {_signal_summary(signals[provider])}")

    waiting_on = [provider for provider, signal in signals.items() if not signal.ready]
    summary = {
        "month": target.label,
        "barrier_ready": not waiting_on,
        "waiting_on": waiting_on,
        "submitted": [],
        "completed": [],
        "running": [],
        "anomalies": [],
        "noop": [],
    }
    if waiting_on:
        print(
            "[coordinator] Global barrier not satisfied, but ready providers will still be "
            f"submitted independently. Waiting on: {', '.join(waiting_on)}"
        )

    worker_targets = worker_targets_from_env()
    for provider in ("aws", "azure", "gcp"):
        now = utcnow_iso()
        status = load_status(
            s3,
            target,
            provider,
            env,
            titans_bucket=titans_bucket,
        ) or {}
        output_complete = cold_outputs_complete(
            s3,
            target,
            provider,
            titans_bucket=titans_bucket,
        )
        observed_job_state = describe_job_state(batch, status.get("job_id"))
        action = decide_provider_action(
            output_complete=output_complete,
            stored_state=status.get("state"),
            observed_job_state=observed_job_state,
        )
        if action == "complete":
            payload = {
                **status,
                "provider": provider,
                "month": target.label,
                "state": "COMPLETE",
                "last_checked_at": now,
            }
            write_status(
                s3,
                target,
                provider,
                env,
                payload,
                titans_bucket=titans_bucket,
            )
            summary["completed"].append(provider)
            print(f"[coordinator] {provider}: cold outputs already present")
            continue

        if action == "skip_running":
            payload = {
                **status,
                "provider": provider,
                "month": target.label,
                "state": observed_job_state or status.get("state"),
                "last_checked_at": now,
            }
            write_status(
                s3,
                target,
                provider,
                env,
                payload,
                titans_bucket=titans_bucket,
            )
            summary["running"].append(provider)
            print(f"[coordinator] {provider}: existing job still running ({payload['state']})")
            continue

        if action == "skip_anomaly":
            summary["anomalies"].append(provider)
            print(f"[coordinator] {provider}: previous anomaly retained, not resubmitting")
            continue

        if not signals[provider].ready:
            summary["noop"].append(provider)
            print(f"[coordinator] {provider}: not ready, skipping submission")
            continue

        if action == "anomaly":
            payload = {
                **status,
                "provider": provider,
                "month": target.label,
                "state": "ANOMALY",
                "last_checked_at": now,
                "reason": "Job succeeded but cold outputs are missing",
            }
            write_status(
                s3,
                target,
                provider,
                env,
                payload,
                titans_bucket=titans_bucket,
            )
            summary["anomalies"].append(provider)
            _send_anomaly_alert(
                f"monthly_freeze anomaly: provider={provider}, month={target.label}, "
                f"job_id={status.get('job_id')}, reason=SUCCEEDED_without_outputs"
            )
            print(f"[coordinator] {provider}: anomaly recorded")
            continue

        submission = submit_worker_job(
            batch,
            target,
            worker_targets[provider],
            env=env,
        )
        payload = {
            "provider": provider,
            "month": target.label,
            "state": "SUBMITTED",
            "submitted_at": now,
            "last_checked_at": now,
            **submission,
        }
        write_status(
            s3,
            target,
            provider,
            env,
            payload,
            titans_bucket=titans_bucket,
        )
        summary["submitted"].append(provider)
        print(
            f"[coordinator] {provider}: submitted job_id={submission['job_id']} "
            f"queue={submission['job_queue']}"
        )

    return summary
