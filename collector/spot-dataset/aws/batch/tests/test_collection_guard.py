import importlib.util
import io
import json
import threading
import sys
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

from botocore.exceptions import ClientError
import pytest

path = Path(__file__).parents[1] / 'collection_guard.py'
spec = importlib.util.spec_from_file_location('collection_guard', path)
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)


@pytest.fixture(autouse=True)
def no_real_slack(monkeypatch):
    monkeypatch.setattr(guard, 'send_slack', lambda message: None, raising=False)


class S3:
    def __init__(self):
        self.value = None
        self.version = 0
        self.lock = threading.Lock()

    def get_object(self, **kwargs):
        with self.lock:
            if self.value is None:
                raise ClientError({'Error': {'Code': 'NoSuchKey'}}, 'GetObject')
            return {'Body': io.BytesIO(self.value), 'ETag': str(self.version)}

    def put_object(self, Body, IfMatch=None, IfNoneMatch=None, **kwargs):
        with self.lock:
            if (IfNoneMatch == '*' and self.value is not None) or (IfMatch is not None and IfMatch != str(self.version)):
                raise ClientError({'Error': {'Code': 'PreconditionFailed'}}, 'PutObject')
            self.version += 1
            self.value = Body
            return {'ETag': str(self.version)}


def test_lock_covers_read_allocate_collect_commit_and_duplicate_jobs():
    s3 = S3()
    started, finish = threading.Event(), threading.Event()
    metadata = {'next': 1800}
    seen = []

    def first():
        seen.append(metadata['next'])
        started.set()
        assert finish.wait(3)
        metadata['next'] = 1915
        return 0

    def second():
        seen.append(metadata['next'])
        return 0

    with ThreadPoolExecutor(max_workers=2) as pool:
        a = pool.submit(guard.run_guarded, s3, 1000, first, now=lambda: 1000)
        assert started.wait(3)
        assert guard.run_guarded(s3, 1010, second, now=lambda: 1010) == 0
        assert seen == [1800]
        finish.set()
        assert a.result() == 0
    # Duplicate reservation and older reservations cannot reacquire after release.
    assert guard.run_guarded(s3, 1000, second, now=lambda: 1010) == 0
    assert guard.run_guarded(s3, 999, second, now=lambda: 1010) == 0
    assert seen == [1800]
    assert guard.run_guarded(s3, 1010, second, now=lambda: 1010) == 0
    assert seen == [1800, 1915]


def test_expired_owner_cannot_release_replacement():
    s3 = S3()
    now = [1000]
    first = guard.CollectionLease(s3, clock=lambda: now[0], ttl=1200, owner='first')
    second = guard.CollectionLease(s3, clock=lambda: now[0], ttl=1200, owner='second')
    assert first.acquire(1000)
    now[0] = 2201
    assert second.acquire(2201)
    assert first.release(0) is False
    assert json.loads(s3.value)['owner'] == 'second'


def test_stale_reservation_does_not_read_metadata_or_run_pipeline():
    called = []
    assert guard.run_guarded(S3(), 1000, lambda: called.append(True), now=lambda: 1601) == 0
    assert called == []


def test_pipeline_failure_releases_lease_but_preserves_high_watermark():
    s3 = S3()
    assert guard.run_guarded(s3, 1000, lambda: 1, now=lambda: 1000) == 1
    value = json.loads(s3.value)
    assert value['expires_at'] == 0
    assert value['scheduled_at'] == 1000
    assert value['exit_code'] == 1


def test_concurrent_acquisition_has_one_winner():
    s3 = S3()
    start = threading.Barrier(8)
    def acquire(i):
        start.wait()
        return guard.CollectionLease(s3, clock=lambda: 1000, owner=str(i)).acquire(1000)
    with ThreadPoolExecutor(max_workers=8) as pool:
        assert sum(pool.map(acquire, range(8))) == 1


def test_deadline_stops_pipeline_process_group(tmp_path):
    pidfile = tmp_path / 'child.pid'
    code = (
        "import subprocess,sys,time,pathlib; "
        "p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(120)']); "
        "pathlib.Path(sys.argv[1]).write_text(str(p.pid)); time.sleep(120)"
    )
    assert guard.run_process([sys.executable, '-c', code, str(pidfile)], timeout=1) == 124
    pid = int(pidfile.read_text())
    proc = Path(f'/proc/{pid}/stat')
    # A dead adopted process can briefly remain a zombie until init reaps it.
    assert not proc.exists() or proc.read_text().split()[2] == 'Z'


def test_stale_notification_includes_reservation_and_delay():
    messages = []
    assert guard.run_guarded(S3(), 1000, lambda: pytest.fail('must skip'),
                             now=lambda: 1601, notification_sender=messages.append) == 0
    assert len(messages) == 1
    assert '1970-01-01T00:16:40Z' in messages[0]
    assert '601' in messages[0]


def test_busy_notifies_but_duplicate_and_older_do_not():
    s3 = S3()
    active = guard.CollectionLease(s3, clock=lambda: 1000)
    assert active.acquire(1000)
    messages = []
    for stamp in [1010, 1000, 999]:
        assert guard.run_guarded(s3, stamp, lambda: pytest.fail('must skip'),
                                 now=lambda: 1010, notification_sender=messages.append) == 0
    assert len(messages) == 1
    assert '1970-01-01T00:16:50Z' in messages[0]


@pytest.mark.parametrize('busy', [False, True])
def test_notification_failure_preserves_skip_outcome(busy, capsys):
    s3 = S3()
    if busy:
        assert guard.CollectionLease(s3, clock=lambda: 1000).acquire(1000)
    def failed(message):
        raise RuntimeError('secret webhook must not appear in logs')
    result = guard.run_guarded(s3, 1010 if busy else 1000, lambda: pytest.fail('must skip'),
                               now=lambda: 1010 if busy else 1601, notification_sender=failed)
    assert result == 0
    output = capsys.readouterr().out
    assert 'notification_failed' in output
    assert 'secret webhook' not in output


def test_contended_acquisition_is_log_only():
    class Contended(S3):
        def put_object(self, **kwargs):
            raise ClientError({'Error': {'Code': 'ConditionalRequestConflict'}}, 'PutObject')
    messages = []
    assert guard.run_guarded(Contended(), 1000, lambda: pytest.fail('must skip'),
                             now=lambda: 1000, notification_sender=messages.append) == 0
    assert messages == []
