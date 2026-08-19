import importlib.util
from pathlib import Path


def _load_notifications_module():
    module_path = Path(__file__).parents[1] / "sps" / "sps_notifications.py"
    spec = importlib.util.spec_from_file_location("azure_sps_notifications_test", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_generic_failure_message_includes_exception_type(monkeypatch):
    notifications = _load_notifications_module()
    monkeypatch.delenv("AWS_BATCH_JOB_ID", raising=False)
    monkeypatch.delenv("AWS_BATCH_JOB_ATTEMPT", raising=False)

    message = notifications.format_failure_message(
        timestamp="2026-08-03T02:00:00Z",
        desired_counts=[1],
        error=ValueError("invalid metadata"),
    )

    assert "Azure SPS 수집 실패" in message
    assert "ValueError: invalid metadata" in message
    assert "SPS 값은 N/A로 기록" in message
    assert "Batch Job:* `unknown`" in message


def test_deadline_message_explains_old_job_failure_and_next_run(monkeypatch):
    notifications = _load_notifications_module()
    monkeypatch.setenv("AWS_BATCH_JOB_ID", "job-old")

    message = notifications.format_deadline_exceeded_message(
        timestamp="2026-08-03T02:00:00Z",
        desired_counts=[1],
        query_started_timestamp="2026-08-03T02:02:30Z",
        deadline_timestamp="2026-08-03T02:12:30Z",
        budget_seconds=600,
        completed_request_count=2_093,
        total_request_count=2_094,
        failed_request_count=1,
    )

    assert "Azure SPS 수집 시간 제한 초과" in message
    assert "SPS API 요청 시작부터 10분 안에 완료되지 않음" in message
    assert "기존 작업을 실패 처리" in message
    assert "성공한 SPS 요청 2,093건은 저장" in message
    assert "완료하지 못한 1건의 요청 범위만 N/A로 기록" in message
    assert "Price·IF 수집 결과를 유지" in message
    assert "02:10:00Z" in message
    assert "02:02:30Z" in message
    assert "02:12:30Z" in message
    assert "job-old" in message


def test_superseded_message_explains_priority_handoff(monkeypatch):
    notifications = _load_notifications_module()
    monkeypatch.setenv("AWS_BATCH_JOB_ID", "job-old")

    message = notifications.format_superseded_message(
        timestamp="2026-08-03T02:00:00Z",
        desired_counts=[1],
        query_started_timestamp="2026-08-03T02:03:00Z",
        completed_request_count=2_000,
        failed_request_count=94,
        total_request_count=2_094,
        partial_sps_saved=True,
    )

    assert "Azure SPS 이전 수집 종료" in message
    assert "다음 예약 수집 `2026-08-03T02:10:00Z`이 실행 우선권을 가져감" in message
    assert "남은 SPS 요청 중단" in message
    assert "2,000/2,094건 완료" in message
    assert "완료하지 못한 94건의 요청 범위만 N/A로 기록" in message
    assert "자체 10분 제한으로 SPS 수집 진행" in message
    assert "job-old" in message


def test_request_failure_message_reports_accounted_partial_result(monkeypatch):
    notifications = _load_notifications_module()
    monkeypatch.setenv("AWS_BATCH_JOB_ID", "job-partial")

    message = notifications.format_request_failure_message(
        timestamp="2026-08-05T01:10:00Z",
        desired_counts=[5],
        query_started_timestamp="2026-08-05T01:12:00Z",
        completed_request_count=2_085,
        failed_request_count=1,
        total_request_count=2_086,
        failure_reasons=["RETRY_EXHAUSTED"],
        partial_sps_saved=True,
    )

    assert "Azure SPS 일부 요청 실패" in message
    assert "요청 재시도 소진" in message
    assert "2,085/2,086건 완료" in message
    assert "완료하지 못한 1건의 요청 범위만 N/A로 기록" in message
    assert "job-partial" in message


def test_request_failure_message_identifies_azure_connection_failure(monkeypatch):
    notifications = _load_notifications_module()
    monkeypatch.setenv("AWS_BATCH_JOB_ID", "job-connection-error")

    message = notifications.format_request_failure_message(
        timestamp="2026-08-05T05:20:00Z",
        desired_counts=[40],
        query_started_timestamp="2026-08-05T05:23:00Z",
        completed_request_count=2_085,
        failed_request_count=1,
        total_request_count=2_086,
        failure_reasons=["CONNECTION_RETRY_EXHAUSTED"],
        partial_sps_saved=True,
    )

    assert "Azure SPS 연결 실패 후 재시도 소진" in message
    assert "예상하지 못한 요청 오류" not in message


def test_request_failure_message_identifies_azure_server_error(monkeypatch):
    notifications = _load_notifications_module()
    monkeypatch.setenv("AWS_BATCH_JOB_ID", "job-server-error")

    message = notifications.format_request_failure_message(
        timestamp="2026-08-06T08:40:00Z",
        desired_counts=[50],
        query_started_timestamp="2026-08-06T08:41:53Z",
        completed_request_count=2_080,
        failed_request_count=1,
        total_request_count=2_081,
        failure_reasons=["SERVER_ERROR_RETRY_EXHAUSTED"],
        partial_sps_saved=True,
    )

    assert "Azure SPS 서버 오류가 재시도 후에도 계속됨" in message
    assert "예상하지 못한 요청 오류" not in message
