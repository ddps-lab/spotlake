import json
from pathlib import Path
from threading import Event, Thread

from titans_common.compaction_queue import (
    acquire_queue_lease,
    enqueue_request,
    format_failure_message,
    list_pending_keys,
    process_pending,
    process_pending_exclusively,
)


BUCKET = "titans-spotlake-data"


def _write_request(path: Path, timestamp: str, hot_key: str) -> None:
    path.write_text(
        json.dumps(
            {
                "provider": "azure",
                "hot_key": hot_key,
                "timestamp": timestamp,
                "timeout_seconds": 30.0,
            }
        ),
        encoding="utf-8",
    )


def test_failed_request_remains_pending_for_next_cycle(s3_client, tmp_path):
    request_path = tmp_path / "request.json"
    _write_request(
        request_path,
        "2026-08-03T10:10:00+00:00",
        "parquet_cp_hot/azure/2026/08/03/10-10.parquet",
    )

    pending_key = enqueue_request(request_path, s3_client=s3_client)
    result = process_pending(
        "azure",
        s3_client=s3_client,
        run_request=lambda _: -11,
        notify_failure=lambda **_: None,
    )

    assert result.failed_key == pending_key
    assert result.exit_code == 139
    assert list_pending_keys("azure", s3_client=s3_client) == [pending_key]


def test_pending_requests_run_oldest_first_and_delete_only_successes(s3_client, tmp_path):
    newer = tmp_path / "newer.json"
    older = tmp_path / "older.json"
    _write_request(
        newer,
        "2026-08-03T10:20:00+00:00",
        "parquet_cp_hot/azure/2026/08/03/10-20.parquet",
    )
    _write_request(
        older,
        "2026-08-03T10:10:00+00:00",
        "parquet_cp_hot/azure/2026/08/03/10-10.parquet",
    )
    newer_key = enqueue_request(newer, s3_client=s3_client)
    older_key = enqueue_request(older, s3_client=s3_client)
    seen = []

    def run_request(path: Path) -> int:
        seen.append(json.loads(path.read_text(encoding="utf-8"))["hot_key"])
        return 0 if len(seen) == 1 else -11

    result = process_pending(
        "azure",
        s3_client=s3_client,
        run_request=run_request,
        notify_failure=lambda **_: None,
    )

    assert seen == [
        "parquet_cp_hot/azure/2026/08/03/10-10.parquet",
        "parquet_cp_hot/azure/2026/08/03/10-20.parquet",
    ]
    assert result.processed == 1
    assert result.failed_key == newer_key
    assert list_pending_keys("azure", s3_client=s3_client) == [newer_key]
    assert older_key not in list_pending_keys("azure", s3_client=s3_client)


def test_overlapping_consumers_run_a_pending_request_only_once(s3_client, tmp_path):
    request_path = tmp_path / "request.json"
    _write_request(
        request_path,
        "2026-08-03T10:10:00+00:00",
        "parquet_cp_hot/azure/2026/08/03/10-10.parquet",
    )
    enqueue_request(request_path, s3_client=s3_client)

    first_started = Event()
    finish_first = Event()
    seen = []
    first_result = []

    def slow_runner(path: Path) -> int:
        seen.append(json.loads(path.read_text(encoding="utf-8"))["hot_key"])
        first_started.set()
        assert finish_first.wait(timeout=5)
        return 0

    first = Thread(
        target=lambda: first_result.append(
            process_pending_exclusively(
                "azure",
                s3_client=s3_client,
                run_request=slow_runner,
                notify_failure=lambda **_: None,
            )
        )
    )
    first.start()
    assert first_started.wait(timeout=5)

    second_result = process_pending_exclusively(
        "azure",
        s3_client=s3_client,
        run_request=lambda _: seen.append("duplicate") or 0,
        notify_failure=lambda **_: None,
    )

    assert second_result.lease_acquired is False
    assert seen == ["parquet_cp_hot/azure/2026/08/03/10-10.parquet"]

    finish_first.set()
    first.join(timeout=5)
    assert not first.is_alive()
    assert first_result[0].lease_acquired is True
    assert list_pending_keys("azure", s3_client=s3_client) == []

    _write_request(
        request_path,
        "2026-08-03T10:20:00+00:00",
        "parquet_cp_hot/azure/2026/08/03/10-20.parquet",
    )
    enqueue_request(request_path, s3_client=s3_client)
    next_result = process_pending_exclusively(
        "azure",
        s3_client=s3_client,
        run_request=lambda path: seen.append(
            json.loads(path.read_text(encoding="utf-8"))["hot_key"]
        ) or 0,
        notify_failure=lambda **_: None,
    )

    assert next_result.lease_acquired is True
    assert seen[-1] == "parquet_cp_hot/azure/2026/08/03/10-20.parquet"


def test_expired_lease_is_taken_over_without_stale_owner_clobbering(s3_client):
    expired = acquire_queue_lease(
        "azure",
        s3_client=s3_client,
        ttl_seconds=-1,
        refresh_seconds=3600,
    )
    assert expired is not None

    successor = acquire_queue_lease(
        "azure",
        s3_client=s3_client,
        refresh_seconds=3600,
    )
    assert successor is not None
    assert successor.owner_id != expired.owner_id

    expired.release()
    lease_object = s3_client.get_object(
        Bucket=BUCKET,
        Key="compaction_requests/azure/lease.json",
    )
    lease_body = json.loads(lease_object["Body"].read())
    assert lease_body["owner_id"] == successor.owner_id

    successor.release()


def test_failure_message_explains_preserved_data_and_retry():
    message = format_failure_message(
        provider="azure",
        hot_key="parquet_cp_hot/azure/2026/08/03/10-10.parquet",
        return_code=-11,
        job_id="job-123",
    )

    assert "SpotLake 원본 수집: 완료" in message
    assert "Warm compaction: 실패" in message
    assert "SIGSEGV (Exit 139)" in message
    assert "다음 수집 주기에서 먼저 재시도" in message
    assert "job-123" in message
