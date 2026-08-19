import os
from datetime import datetime, timedelta, timezone


AWS_REGION = os.environ.get("AWS_REGION", "us-west-2")
SCHEDULE_INTERVAL_MINUTES = 10

REQUEST_FAILURE_REASON_LABELS = {
    "ACTIVE_LEASE_LOST": "SPS 호출권 갱신 실패",
    "CONNECTION_RETRY_EXHAUSTED": "Azure SPS 연결 실패 후 재시도 소진",
    "FANOUT_CANCELLED": "대기 중 요청 취소",
    "INVALID_RESPONSE": "Azure SPS 응답 형식 오류",
    "NO_AVAILABLE_LOCATIONS": "호출 가능한 Azure SPS location 없음",
    "RETRY_EXHAUSTED": "요청 재시도 소진",
    "SERVER_ERROR_RETRY_EXHAUSTED": "Azure SPS 서버 오류가 재시도 후에도 계속됨",
    "UNEXPECTED_ERROR": "예상하지 못한 요청 오류",
}


def _job_context():
    job_id = os.environ.get("AWS_BATCH_JOB_ID", "unknown")
    attempt = os.environ.get("AWS_BATCH_JOB_ATTEMPT", "unknown")
    if job_id == "unknown":
        job_display = f"`{job_id}`"
    else:
        job_url = (
            f"https://console.aws.amazon.com/batch/home?region={AWS_REGION}"
            f"#jobs/detail/{job_id}"
        )
        job_display = f"<{job_url}|`{job_id}`>"
    return job_display, attempt


def _desired_counts_text(desired_counts):
    return ", ".join(str(value) for value in desired_counts)


def format_failure_message(*, timestamp, desired_counts, error):
    job_display, attempt = _job_context()
    return (
        ":rotating_light: *Azure SPS 수집 실패*\n"
        f"*원인:* `{type(error).__name__}: {error}`\n"
        "*영향:* SPS 값은 N/A로 기록하고 Price·IF 수집 결과는 유지\n"
        "*부분 저장:* Price·IF 수집 결과는 부분 snapshot으로 저장 시도\n"
        f"*복구:* 다음 schedule에서 {SCHEDULE_INTERVAL_MINUTES}분 후 자동 재시도\n"
        f"*수집 시각:* `{timestamp}`\n"
        f"*Desired Count:* `{_desired_counts_text(desired_counts)}`\n"
        f"*Batch Job:* {job_display} (attempt {attempt})"
    )


def format_deadline_exceeded_message(
    *,
    timestamp,
    desired_counts,
    query_started_timestamp,
    deadline_timestamp,
    budget_seconds,
    completed_request_count,
    failed_request_count,
    total_request_count,
    partial_sps_saved=True,
):
    job_display, attempt = _job_context()
    scheduled_at = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=timezone.utc
    )
    next_scheduled_at = scheduled_at + timedelta(minutes=SCHEDULE_INTERVAL_MINUTES)
    next_scheduled_text = next_scheduled_at.strftime("%Y-%m-%dT%H:%M:%SZ")
    budget_minutes = budget_seconds // 60
    if partial_sps_saved:
        impact_text = (
            f"*영향:* 성공한 SPS 요청 {completed_request_count:,}건은 저장하고 "
            "Price·IF 수집 결과를 유지\n"
        )
        handling_text = (
            f"*처리:* 완료하지 못한 {failed_request_count:,}건의 요청 범위만 "
            "N/A로 기록하고 기존 작업을 실패 처리\n"
        )
    else:
        impact_text = "*영향:* SPS 값은 N/A로 기록하고 Price·IF 수집 결과를 유지\n"
        handling_text = (
            "*처리:* 부분 SPS 저장에 실패해 Price·IF만 부분 snapshot으로 저장 시도, "
            "기존 작업을 실패 처리\n"
        )

    return (
        ":alarm_clock: *Azure SPS 수집 시간 제한 초과*\n"
        f"*원인:* SPS API 요청 시작부터 {budget_minutes}분 안에 완료되지 않음\n"
        f"*수집 결과:* {completed_request_count:,}/{total_request_count:,}건 완료\n"
        f"{impact_text}"
        f"{handling_text}"
        f"*복구:* 새 수집은 다음 예약 시각 `{next_scheduled_text}`에 실행\n"
        f"*수집 시각:* `{timestamp}`\n"
        f"*SPS 요청 시작:* `{query_started_timestamp}`\n"
        f"*종료 기한:* `{deadline_timestamp}`\n"
        f"*Desired Count:* `{_desired_counts_text(desired_counts)}`\n"
        f"*Batch Job:* {job_display} (attempt {attempt})"
    )


def format_request_failure_message(
    *,
    timestamp,
    desired_counts,
    query_started_timestamp,
    completed_request_count,
    failed_request_count,
    total_request_count,
    failure_reasons,
    partial_sps_saved=True,
):
    job_display, attempt = _job_context()
    scheduled_at = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=timezone.utc
    )
    next_scheduled_at = scheduled_at + timedelta(minutes=SCHEDULE_INTERVAL_MINUTES)
    next_scheduled_text = next_scheduled_at.strftime("%Y-%m-%dT%H:%M:%SZ")
    reason_text = ", ".join(
        REQUEST_FAILURE_REASON_LABELS.get(reason, reason)
        for reason in sorted(set(failure_reasons))
    ) or "원인 미확인"
    if partial_sps_saved:
        handling_text = (
            f"*처리:* 완료하지 못한 {failed_request_count:,}건의 요청 범위만 "
            "N/A로 기록하고 기존 작업을 실패 처리\n"
        )
    else:
        handling_text = (
            "*처리:* 부분 SPS 저장에 실패해 Price·IF만 부분 snapshot으로 "
            "저장 시도, 기존 작업을 실패 처리\n"
        )

    return (
        ":warning: *Azure SPS 일부 요청 실패*\n"
        f"*원인:* {reason_text}\n"
        f"*수집 결과:* {completed_request_count:,}/{total_request_count:,}건 완료\n"
        f"*영향:* 성공한 SPS 요청 {completed_request_count:,}건과 "
        "Price·IF 수집 결과를 유지\n"
        f"{handling_text}"
        f"*복구:* 새 수집은 다음 예약 시각 `{next_scheduled_text}`에 실행\n"
        f"*수집 시각:* `{timestamp}`\n"
        f"*SPS 요청 시작:* `{query_started_timestamp}`\n"
        f"*Desired Count:* `{_desired_counts_text(desired_counts)}`\n"
        f"*Batch Job:* {job_display} (attempt {attempt})"
    )


def format_superseded_message(
    *,
    timestamp,
    desired_counts,
    query_started_timestamp,
    completed_request_count=None,
    failed_request_count=None,
    total_request_count=None,
    partial_sps_saved=False,
):
    job_display, attempt = _job_context()
    scheduled_at = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ").replace(
        tzinfo=timezone.utc
    )
    next_scheduled_at = scheduled_at + timedelta(minutes=SCHEDULE_INTERVAL_MINUTES)
    next_scheduled_text = next_scheduled_at.strftime("%Y-%m-%dT%H:%M:%SZ")
    if partial_sps_saved and total_request_count is not None:
        result_text = (
            f"*수집 결과:* {completed_request_count:,}/{total_request_count:,}건 완료\n"
            f"*영향:* 완료한 SPS 요청 {completed_request_count:,}건과 "
            "Price·IF 수집 결과를 유지\n"
        )
        if failed_request_count:
            result_text += (
                f"*처리:* 완료하지 못한 {failed_request_count:,}건의 요청 범위만 "
                "N/A로 기록하고 남은 SPS 요청 중단\n"
            )
        else:
            result_text += "*처리:* 수집한 SPS 결과를 부분 snapshot으로 저장\n"
    else:
        result_text = (
            "*영향:* 이 작업의 SPS 값은 N/A로 기록하고 Price·IF 수집 결과는 유지\n"
            "*처리:* 남은 SPS 요청 중단, 부분 SPS 데이터 저장 실패\n"
        )

    return (
        ":fast_forward: *Azure SPS 이전 수집 종료*\n"
        f"*원인:* 다음 예약 수집 `{next_scheduled_text}`이 실행 우선권을 가져감\n"
        f"{result_text}"
        "*복구:* 다음 예약 수집이 자체 10분 제한으로 SPS 수집 진행\n"
        f"*수집 시각:* `{timestamp}`\n"
        f"*SPS 요청 시작:* `{query_started_timestamp}`\n"
        f"*Desired Count:* `{_desired_counts_text(desired_counts)}`\n"
        f"*Batch Job:* {job_display} (attempt {attempt})"
    )
