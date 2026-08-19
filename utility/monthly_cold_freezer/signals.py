"""Barrier checks for monthly cold freezer coordination."""

from __future__ import annotations

import calendar
import io
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import PurePosixPath

from botocore.exceptions import ClientError

TITANS_BUCKET = "titans-spotlake-data"
RAW_BUCKET = "spotlake"
WARM_PROGRESS_PREFIX = "ops/warm_progress"
RAW_PREFIXES = {
    "aws": "rawdata/aws",
    "azure": "rawdata/azure",
    "gcp": "rawdata/gcp",
}
HOT_PREFIXES = {
    "aws": "parquet_cp_hot/aws",
    "azure": "parquet_cp_hot/azure",
    "gcp": "parquet_cp_hot/gcp",
}


@dataclass(frozen=True)
class TargetMonth:
    year: int
    month: int

    @property
    def label(self) -> str:
        return f"{self.year}-{self.month:02d}"

    @property
    def last_day(self) -> int:
        return calendar.monthrange(self.year, self.month)[1]

    @property
    def next_year(self) -> int:
        return self.year + 1 if self.month == 12 else self.year

    @property
    def next_month(self) -> int:
        return 1 if self.month == 12 else self.month + 1

    @property
    def warm_manifest_threshold(self) -> datetime:
        return datetime(self.year, self.month, self.last_day, 23, 50, tzinfo=timezone.utc)

    @property
    def gcp_raw_threshold(self) -> datetime:
        return datetime(self.year, self.month, self.last_day, 23, 0, tzinfo=timezone.utc)

    @property
    def cold_cp_suffix(self) -> str:
        return f"{self.label}.parquet"

    @property
    def cold_ap_suffix(self) -> str:
        return f"{self.next_year}-{self.next_month:02d}_AP.parquet"


@dataclass(frozen=True)
class ProviderSignal:
    provider: str
    ready: bool
    reason: str
    threshold: str
    last_processed_time: str | None = None
    last_processed_raw_time: str | None = None
    raw_cutoff_present: bool | None = None
    fallback_threshold: str | None = None
    ready_via: str | None = None
    warning: str | None = None

    def to_dict(self) -> dict:
        return asdict(self)


def env_prefix(env: str) -> str:
    return "test/" if env == "test" else ""


def parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso_or_missing(value: datetime | None) -> str:
    return value.isoformat() if value is not None else "missing"


def previous_month_target(now: datetime | None = None) -> TargetMonth:
    current = now.astimezone(timezone.utc) if now else datetime.now(timezone.utc)
    year = current.year
    month = current.month - 1
    if month == 0:
        year -= 1
        month = 12
    return TargetMonth(year=year, month=month)


def parse_target_month(label: str) -> TargetMonth:
    try:
        year_str, month_str = label.split("-", 1)
        year = int(year_str)
        month = int(month_str)
    except ValueError as exc:
        raise ValueError(f"Invalid target month: {label!r}") from exc

    if month < 1 or month > 12:
        raise ValueError(f"Invalid month: {label!r}")
    return TargetMonth(year=year, month=month)


def manifest_key(provider: str, target: TargetMonth, env: str) -> str:
    prefix = env_prefix(env)
    return f"{prefix}parquet_warm/{provider}/m8/{target.year}/{target.month:02d}/manifest.json"


def warm_progress_key(provider: str, target: TargetMonth, env: str) -> str:
    prefix = env_prefix(env)
    return f"{prefix}{WARM_PROGRESS_PREFIX}/{provider}/{target.label}.json"


def hot_prefix(provider: str, target: TargetMonth, env: str) -> str:
    prefix = env_prefix(env)
    return f"{prefix}{HOT_PREFIXES[provider]}/{target.year}/{target.month:02d}/"


def load_json_object(s3, bucket: str, key: str) -> dict | None:
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise
    body = response["Body"].read()
    if isinstance(body, bytes):
        return json.loads(body)
    if isinstance(body, io.BytesIO):
        return json.loads(body.getvalue())
    return json.loads(body)


def read_manifest_last_processed_time(
    s3,
    target: TargetMonth,
    provider: str,
    *,
    env: str = "production",
    titans_bucket: str = TITANS_BUCKET,
) -> datetime | None:
    manifest = load_json_object(s3, titans_bucket, manifest_key(provider, target, env))
    if not manifest:
        return None
    return parse_datetime(manifest.get("last_processed_time"))


def read_progress_last_processed_raw_time(
    s3,
    target: TargetMonth,
    provider: str,
    *,
    env: str = "production",
    titans_bucket: str = TITANS_BUCKET,
) -> datetime | None:
    progress = load_json_object(s3, titans_bucket, warm_progress_key(provider, target, env))
    if not progress:
        return None
    return parse_datetime(progress.get("last_processed_raw_time"))


def iter_s3_keys(s3, bucket: str, prefix: str):
    continuation = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if continuation:
            kwargs["ContinuationToken"] = continuation
        response = s3.list_objects_v2(**kwargs)
        for item in response.get("Contents", []):
            yield item["Key"]
        if not response.get("IsTruncated"):
            break
        continuation = response.get("NextContinuationToken")


def raw_key_to_timestamp(key: str, target: TargetMonth) -> datetime | None:
    filename = PurePosixPath(key).name
    if not filename.endswith(".csv.gz"):
        return None
    parts = filename.removesuffix(".csv.gz").split("-")
    if len(parts) != 3:
        return None
    try:
        hour, minute, second = (int(part) for part in parts)
    except ValueError:
        return None
    return datetime(
        target.year,
        target.month,
        target.last_day,
        hour,
        minute,
        second,
        tzinfo=timezone.utc,
    )


def hot_key_to_timestamp(key: str) -> datetime | None:
    path = PurePosixPath(key)
    try:
        year = int(path.parts[-4])
        month = int(path.parts[-3])
        day = int(path.parts[-2])
    except (ValueError, IndexError):
        return None

    stem = path.stem
    try:
        if stem.startswith("slot_"):
            hhmm = stem.split("_", 2)[1]
            hour = int(hhmm[:2])
            minute = int(hhmm[2:4])
        else:
            hour_str, minute_str = stem.split("-", 1)
            hour = int(hour_str)
            minute = int(minute_str)
    except (IndexError, ValueError):
        return None

    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


def month_has_hot_inputs(
    s3,
    target: TargetMonth,
    provider: str,
    *,
    env: str = "production",
    titans_bucket: str = TITANS_BUCKET,
) -> bool:
    month_prefix = hot_prefix(provider, target, env)
    month_end = datetime(target.next_year, target.next_month, 1, tzinfo=timezone.utc)
    for key in iter_s3_keys(s3, titans_bucket, month_prefix):
        hot_time = hot_key_to_timestamp(key)
        if hot_time is not None and hot_time < month_end:
            return True
    return False


def gcp_has_raw_cutoff_snapshot(
    s3,
    target: TargetMonth,
    *,
    raw_bucket: str = RAW_BUCKET,
) -> bool:
    prefix = f"{RAW_PREFIXES['gcp']}/{target.year}/{target.month:02d}/{target.last_day:02d}/"
    for key in iter_s3_keys(s3, raw_bucket, prefix):
        raw_time = raw_key_to_timestamp(key, target)
        if raw_time and raw_time >= target.gcp_raw_threshold:
            return True
    return False


def evaluate_provider_signal(
    s3,
    target: TargetMonth,
    provider: str,
    *,
    env: str = "production",
    titans_bucket: str = TITANS_BUCKET,
    raw_bucket: str = RAW_BUCKET,
) -> ProviderSignal:
    if provider not in {"aws", "azure", "gcp"}:
        raise ValueError(f"Unsupported provider: {provider}")

    manifest_threshold = target.warm_manifest_threshold
    worker_cutoff = target.gcp_raw_threshold if provider == "gcp" else manifest_threshold

    manifest = load_json_object(s3, titans_bucket, manifest_key(provider, target, env))
    manifest_present = manifest is not None
    last_processed_time = parse_datetime(
        manifest.get("last_processed_time") if manifest else None
    )
    if last_processed_time is not None and last_processed_time >= manifest_threshold:
        return ProviderSignal(
            provider=provider,
            ready=True,
            reason=(
                "manifest last_processed_time "
                f"{last_processed_time.isoformat()} >= {manifest_threshold.isoformat()}"
            ),
            threshold=worker_cutoff.isoformat(),
            last_processed_time=last_processed_time.isoformat(),
            ready_via="manifest",
        )

    if manifest_present and last_processed_time is None:
        return ProviderSignal(
            provider=provider,
            ready=False,
            reason=(
                "manifest exists but last_processed_time is missing; "
                "cannot determine warm snapshot coverage"
            ),
            threshold=worker_cutoff.isoformat(),
        )

    if last_processed_time is not None:
        warning = (
            f"manifest last_processed_time {last_processed_time.isoformat()} < "
            f"{manifest_threshold.isoformat()}; worker will append uncovered hot tail "
            f"through {worker_cutoff.isoformat()}"
        )
        return ProviderSignal(
            provider=provider,
            ready=True,
            reason=warning,
            threshold=worker_cutoff.isoformat(),
            last_processed_time=last_processed_time.isoformat(),
            ready_via="manifest_plus_hot_tail",
            warning=warning,
        )

    if month_has_hot_inputs(
        s3,
        target,
        provider,
        env=env,
        titans_bucket=titans_bucket,
    ):
        warning = (
            "manifest missing; worker will freeze from hot-only month snapshot "
            f"through {worker_cutoff.isoformat()}"
        )
        return ProviderSignal(
            provider=provider,
            ready=True,
            reason=warning,
            threshold=worker_cutoff.isoformat(),
            ready_via="hot_only",
            warning=warning,
        )

    last_processed_raw_time = read_progress_last_processed_raw_time(
        s3,
        target,
        provider,
        env=env,
        titans_bucket=titans_bucket,
    )
    raw_cutoff_present = None
    if provider == "gcp":
        raw_cutoff_present = gcp_has_raw_cutoff_snapshot(
            s3,
            target,
            raw_bucket=raw_bucket,
        )

    reason = (
        "no warm manifest with last_processed_time and no month hot files available; "
        f"last_processed_raw_time={_iso_or_missing(last_processed_raw_time)}"
    )
    if provider == "gcp":
        reason = f"{reason}, raw_cutoff_present={raw_cutoff_present}"
    return ProviderSignal(
        provider=provider,
        ready=False,
        reason=reason,
        threshold=worker_cutoff.isoformat(),
        last_processed_time=last_processed_time.isoformat() if last_processed_time else None,
        last_processed_raw_time=last_processed_raw_time.isoformat() if last_processed_raw_time else None,
        raw_cutoff_present=raw_cutoff_present,
        fallback_threshold=worker_cutoff.isoformat(),
    )


def evaluate_all_signals(
    s3,
    target: TargetMonth,
    *,
    env: str = "production",
    titans_bucket: str = TITANS_BUCKET,
    raw_bucket: str = RAW_BUCKET,
) -> dict[str, ProviderSignal]:
    return {
        provider: evaluate_provider_signal(
            s3,
            target,
            provider,
            env=env,
            titans_bucket=titans_bucket,
            raw_bucket=raw_bucket,
        )
        for provider in ("aws", "azure", "gcp")
    }
