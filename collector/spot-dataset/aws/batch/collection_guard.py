"""S3 CAS lease for the entire AWS allocation/collection/merge transaction."""
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3
import requests
from botocore.config import Config
from botocore.exceptions import ClientError

BUCKET = 'spotlake'
KEY = 'rawdata/aws/localfile/collection_lock.json'
RUNTIME_SECONDS = 840
LEASE_SECONDS = 1200  # Exceeds local deadline and the 900-second Batch timeout.
MAX_RESERVATION_AGE_SECONDS = 600


def log(event, **details):
    print(json.dumps({'event': 'aws_collection_guard', 'state': event, **details}), flush=True)


def send_slack(message):
    """Use the existing notification destination with bounded network requests."""
    webhook = os.environ.get('error_notification_slack_webhook_url')
    if not webhook:
        ssm = boto3.client('ssm', region_name='us-west-2', config=Config(
            connect_timeout=2, read_timeout=3, retries={'total_max_attempts': 1}))
        webhook = ssm.get_parameter(Name='error_notification_slack_webhook_url',
                                    WithDecryption=False)['Parameter']['Value']
    response = requests.post(webhook, json={'text': message}, timeout=(2, 3))
    response.raise_for_status()


def notify_skip(state, scheduled_at, observed_at, sender=None):
    if state not in ['skipped_stale', 'skipped_busy']:
        return
    stamp = datetime.fromtimestamp(scheduled_at, timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    delay = max(0, observed_at - scheduled_at)
    reason = '예약 시각에서 10분을 초과해 시작됨' if state == 'skipped_stale' else '다른 AWS 수집 작업이 실행 중'
    message = (
        ':warning: AWS 수집 예약분 건너뜀\n'
        f'예약 시각: {stamp}\n지연 시간: {delay:.0f}초 ({delay / 60:.1f}분)\n'
        f'사유: {reason}\n해당 예약분의 수집을 건너뛰었습니다.\n'
        f"Batch Job: {os.environ.get('AWS_BATCH_JOB_ID', 'unknown')}"
    )
    try:
        (sender or send_slack)(message)
    except Exception as error:
        # Exception strings can contain webhook URLs; preserve only the exception type.
        log('notification_failed', skipped_state=state, error_type=type(error).__name__)


class CollectionLease:
    def __init__(self, s3, *, clock=time.time, ttl=LEASE_SECONDS, owner=None):
        self.s3, self.clock, self.ttl = s3, clock, ttl
        self.owner = owner or f"{os.environ.get('AWS_BATCH_JOB_ID', 'local')}:{uuid.uuid4()}"
        self.acquired = False
        self.skip_reason = None

    def _read(self):
        try:
            response = self.s3.get_object(Bucket=BUCKET, Key=KEY)
        except ClientError as error:
            if error.response['Error']['Code'] in ['NoSuchKey', '404']:
                return None, None
            raise
        try:
            return json.loads(response['Body'].read()), response['ETag']
        finally:
            response['Body'].close()

    def acquire(self, scheduled_at):
        current, etag = self._read()
        if current:
            if scheduled_at <= current['scheduled_at']:
                self.skip_reason = 'skipped_duplicate_or_older'
                log('skipped_duplicate_or_older', scheduled_at=scheduled_at)
                return False
            if current['expires_at'] > self.clock():
                self.skip_reason = 'skipped_busy'
                log('skipped_busy', owner=current['owner'], scheduled_at=scheduled_at)
                return False
        value = {'owner': self.owner, 'scheduled_at': scheduled_at,
                 'expires_at': self.clock() + self.ttl, 'started_at': self.clock()}
        condition = {'IfMatch': etag} if etag else {'IfNoneMatch': '*'}
        try:
            self.s3.put_object(Bucket=BUCKET, Key=KEY, Body=json.dumps(value).encode(),
                               ContentType='application/json', **condition)
        except ClientError as error:
            if error.response['Error']['Code'] in ['PreconditionFailed', 'ConditionalRequestConflict', '412', '409']:
                self.skip_reason = 'skipped_contended'
                log('skipped_contended', scheduled_at=scheduled_at)
                return False
            raise
        self.acquired = True
        log('acquired', owner=self.owner, scheduled_at=scheduled_at)
        return True

    def release(self, exit_code):
        if not self.acquired:
            return False
        current, etag = self._read()
        if not current or current['owner'] != self.owner:
            log('release_owner_changed', owner=self.owner)
            return False
        current.update(expires_at=0, finished_at=self.clock(), exit_code=exit_code)
        try:
            self.s3.put_object(Bucket=BUCKET, Key=KEY, Body=json.dumps(current).encode(),
                               ContentType='application/json', IfMatch=etag)
        except ClientError as error:
            if error.response['Error']['Code'] in ['PreconditionFailed', 'ConditionalRequestConflict', '412', '409']:
                return False
            raise
        self.acquired = False
        log('released', owner=self.owner, exit_code=exit_code)
        return True


def run_guarded(s3, scheduled_at, run_pipeline, *, now=time.time, notification_sender=None):
    age = now() - scheduled_at
    if age > MAX_RESERVATION_AGE_SECONDS:
        log('skipped_stale', age_seconds=age, scheduled_at=scheduled_at)
        notify_skip('skipped_stale', scheduled_at, now(), notification_sender)
        return 0
    if age < -60:
        raise ValueError('Reservation timestamp is in the future')
    lease = CollectionLease(s3, clock=now)
    if not lease.acquire(scheduled_at):
        notify_skip(lease.skip_reason, scheduled_at, now(), notification_sender)
        return 0
    result = 1
    try:
        result = run_pipeline()
        return result
    finally:
        lease.release(result)


def run_process(command, timeout=RUNTIME_SECONDS):
    """Bound the whole process group; don't leave SPS/background monitors alive."""
    interrupted = []
    previous = {}
    for sig in [signal.SIGTERM, signal.SIGINT]:
        previous[sig] = signal.signal(sig, lambda signum, frame: interrupted.append(signum))
    child = None
    try:
        env = dict(os.environ, AWS_COLLECTION_GUARD_HELD='1')
        child = subprocess.Popen(command, env=env, start_new_session=True)
        deadline = time.monotonic() + timeout
        while child.poll() is None:
            if interrupted or time.monotonic() >= deadline:
                reason = 'signal' if interrupted else 'deadline'
                log('stopping_pipeline', reason=reason)
                os.killpg(child.pid, signal.SIGTERM)
                try:
                    child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid, signal.SIGKILL)
                    child.wait()
                return 128 + interrupted[0] if interrupted else 124
            time.sleep(0.25)
        return child.returncode
    finally:
        if child is not None:
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def main():
    if len(sys.argv) != 2:
        raise ValueError('Expected a reservation timestamp')
    stamp = datetime.fromisoformat(sys.argv[1].replace('Z', '+00:00'))
    stamp = stamp.replace(tzinfo=timezone.utc) if stamp.tzinfo is None else stamp.astimezone(timezone.utc)
    s3 = boto3.client('s3', config=Config(connect_timeout=5, read_timeout=15, retries={'total_max_attempts': 3}))
    script = Path(__file__).parent / 'scripts/run_collection.sh'
    return run_guarded(s3, stamp.timestamp(), lambda: run_process(['/bin/bash', str(script), sys.argv[1]]))


if __name__ == '__main__':
    sys.exit(main())
