import os
import threading
import time
import uuid
from collections import deque

import boto3
from botocore.exceptions import ClientError


class CollectionDeadline:
    def __init__(
        self,
        *,
        deadline_at=None,
        duration_seconds=None,
        clock=time.time,
        before_start=None,
        on_finish=None,
        cancelled=None,
    ):
        if (deadline_at is None) == (duration_seconds is None):
            raise ValueError("set exactly one of deadline_at or duration_seconds")
        self.deadline_at = (
            float(deadline_at) if deadline_at is not None else None
        )
        self.duration_seconds = (
            float(duration_seconds) if duration_seconds is not None else None
        )
        self.started_at = None
        self._clock = clock
        self._before_start = before_start
        self._on_finish = on_finish
        self._cancelled = cancelled
        self._lock = threading.Lock()
        self._finished = False

    def start(self):
        with self._lock:
            if self.deadline_at is None:
                if self._before_start:
                    self._before_start()
                self.started_at = float(self._clock())
                self.deadline_at = self.started_at + self.duration_seconds
            deadline_at = self.deadline_at
        return deadline_at

    def finish(self):
        callback = None
        with self._lock:
            if self.started_at is not None and not self._finished:
                self._finished = True
                callback = self._on_finish
        if callback:
            callback()

    def remaining_seconds(self):
        if self.deadline_at is None:
            return self.duration_seconds
        return max(0.0, self.deadline_at - self._clock())

    def expired(self):
        return self.remaining_seconds() <= 0

    def cancellation_reason(self):
        if not self._cancelled:
            return None
        reason = self._cancelled()
        if isinstance(reason, str):
            return reason
        return "CANCELLED" if reason else None

    def superseded(self):
        return self.cancellation_reason() == "LEASE_SUPERSEDED"


class TimeoutWindowMetrics:
    def __init__(
        self,
        *,
        window_seconds=120,
        clock=time.monotonic,
    ):
        self.window_seconds = window_seconds
        self._clock = clock
        self._events = deque()
        self._lock = threading.Lock()

    def record_timeout(self):
        return self._record(True)

    def record_success(self):
        return self._record(False)

    def snapshot(self):
        with self._lock:
            self._discard_expired(self._clock())
            samples = len(self._events)
            timeouts = sum(is_timeout for _, is_timeout in self._events)
            return {
                "samples": samples,
                "timeouts": timeouts,
                "timeout_ratio": timeouts / samples if samples else 0.0,
            }

    def _record(self, is_timeout):
        with self._lock:
            now = self._clock()
            self._events.append((now, is_timeout))
            self._discard_expired(now)
            return self.snapshot_unlocked()

    def snapshot_unlocked(self):
        samples = len(self._events)
        timeouts = sum(is_timeout for _, is_timeout in self._events)
        return {
            "samples": samples,
            "timeouts": timeouts,
            "timeout_ratio": timeouts / samples if samples else 0.0,
        }

    def _discard_expired(self, now):
        cutoff = now - self.window_seconds
        while self._events and self._events[0][0] < cutoff:
            self._events.popleft()


class DynamoDBLease:
    def __init__(
        self,
        *,
        lease_id,
        owner_id=None,
        table=None,
        table_name="AzureAuth",
        region_name="us-east-1",
        lease_seconds=600,
        heartbeat_interval=60,
        priority=None,
        clock=time.time,
        sleeper=time.sleep,
    ):
        self.lease_id = lease_id
        self.owner_id = owner_id or os.environ.get("AWS_BATCH_JOB_ID") or str(uuid.uuid4())
        self.table = table or boto3.resource("dynamodb", region_name=region_name).Table(table_name)
        self.lease_seconds = lease_seconds
        self.heartbeat_interval = heartbeat_interval
        self.priority = int(priority) if priority is not None else None
        self._clock = clock
        self._sleep = sleeper
        self._acquired = False
        self._stop = threading.Event()
        self._lost = threading.Event()
        self._heartbeat_thread = None

    def acquire(self, *, start_heartbeat=True):
        now = int(self._clock())
        expires_at = self._next_expiry(now)
        if expires_at <= now:
            return False
        item = {
            "id": self.lease_id,
            "lease_owner": self.owner_id,
            "lease_expires_at": expires_at,
        }
        condition = (
            "attribute_not_exists(id) OR lease_expires_at < :now "
            "OR lease_owner = :owner"
        )
        expression_values = {
            ":now": now,
            ":owner": self.owner_id,
        }
        if self.priority is not None:
            item["lease_priority"] = self.priority
            condition = (
                "attribute_not_exists(id) OR lease_owner = :owner "
                "OR attribute_not_exists(lease_priority) "
                "OR lease_priority < :priority "
                "OR (lease_priority = :priority AND lease_expires_at < :now)"
            )
            expression_values[":priority"] = self.priority
        try:
            self.table.put_item(
                Item=item,
                ConditionExpression=condition,
                ExpressionAttributeValues=expression_values,
            )
        except ClientError as error:
            if error.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False
            raise

        self._acquired = True
        self._stop.clear()
        self._lost.clear()
        if start_heartbeat:
            self._heartbeat_thread = threading.Thread(
                target=self._heartbeat_loop,
                name="azure-sps-lease-heartbeat",
                daemon=True,
            )
            self._heartbeat_thread.start()
        return True

    def acquire_with_wait(
        self,
        *,
        timeout_seconds,
        poll_interval=5,
        start_heartbeat=True,
        cancelled=None,
    ):
        wait_deadline = self._clock() + timeout_seconds
        while True:
            if cancelled and cancelled():
                return False
            if self.acquire(start_heartbeat=start_heartbeat):
                return True
            remaining = wait_deadline - self._clock()
            if remaining <= 0:
                return False
            self._sleep(min(poll_interval, remaining))

    def renew(self):
        now = int(self._clock())
        expires_at = self._next_expiry(now)
        if expires_at <= now:
            return False
        try:
            self._update_expiry(expires_at)
        except ClientError as error:
            if error.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
            self._lost.set()
            self._acquired = False
            return False
        return True

    def lost(self):
        return self._lost.is_set()

    def superseded(self):
        return self.lost()

    def owns_lease(self):
        response = self.table.get_item(
            Key={"id": self.lease_id},
            ConsistentRead=True,
        )
        item = response.get("Item")
        owns_lease = bool(
            item
            and item.get("lease_owner") == self.owner_id
            and (
                self.priority is None
                or item.get("lease_priority") == self.priority
            )
        )
        if not owns_lease:
            self._lost.set()
            self._acquired = False
        return owns_lease

    def release(self):
        self._stop.set()
        if self._heartbeat_thread:
            self._heartbeat_thread.join(timeout=self.heartbeat_interval + 1)
        if not self._acquired:
            return
        try:
            self._update_expiry(0)
        except ClientError as error:
            if error.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
        finally:
            self._acquired = False

    def _heartbeat_loop(self):
        while not self._stop.wait(self.heartbeat_interval):
            try:
                if not self.renew():
                    return
            except Exception as error:
                self._lost.set()
                self._acquired = False
                print(f"[SPS_LEASE] heartbeat_failed owner={self.owner_id} error={error}")
                return

    def _next_expiry(self, now):
        return now + self.lease_seconds

    def _update_expiry(self, expires_at):
        self.table.update_item(
            Key={"id": self.lease_id},
            UpdateExpression="SET lease_expires_at = :expires",
            ConditionExpression="lease_owner = :owner",
            ExpressionAttributeValues={
                ":expires": expires_at,
                ":owner": self.owner_id,
            },
        )


class SPSCallCoordinator:
    """Coordinates scheduled priority separately from active Azure SPS calls."""

    def __init__(
        self,
        *,
        priority_lease,
        active_call_lease,
        handoff_timeout_seconds=70,
        poll_interval=1,
    ):
        self.priority_lease = priority_lease
        self.active_call_lease = active_call_lease
        self.handoff_timeout_seconds = handoff_timeout_seconds
        self.poll_interval = poll_interval
        self._priority_claimed = False
        self._calls_active = False

    def claim_priority(self, *, start_heartbeat=True):
        if self._priority_claimed:
            return self.owns_priority()
        self._priority_claimed = self.priority_lease.acquire(
            start_heartbeat=start_heartbeat
        )
        return self._priority_claimed

    def owns_priority(self):
        if not self._priority_claimed:
            return False
        return self.priority_lease.owns_lease()

    def superseded(self):
        return self.priority_lease.lost()

    def cancellation_reason(self):
        if self.priority_lease.lost():
            return "LEASE_SUPERSEDED"
        if self._calls_active and self.active_call_lease.lost():
            return "ACTIVE_LEASE_LOST"
        return None

    def begin_calls(self, *, start_heartbeat=True):
        if not self.claim_priority(start_heartbeat=start_heartbeat):
            return False
        if not self.owns_priority():
            return False

        acquired = self.active_call_lease.acquire_with_wait(
            timeout_seconds=self.handoff_timeout_seconds,
            poll_interval=self.poll_interval,
            start_heartbeat=start_heartbeat,
            cancelled=self.priority_lease.superseded,
        )
        if not acquired:
            return False

        if not self.owns_priority():
            self.active_call_lease.release()
            return False

        self._calls_active = True
        return True

    def finish_calls(self):
        if not self._calls_active:
            return
        self.active_call_lease.release()
        self._calls_active = False

    def close(self):
        self.finish_calls()
        if self._priority_claimed:
            self.priority_lease.release()
            self._priority_claimed = False
