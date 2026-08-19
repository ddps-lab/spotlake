import threading
import sys
from pathlib import Path

from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).parents[1]))

from sps.sps_resilience import (
    CollectionDeadline,
    DynamoDBLease,
    SPSCallCoordinator,
    TimeoutWindowMetrics,
)


class FakeClock:
    def __init__(self, now=1_000):
        self.now = now

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class FakeLeaseTable:
    def __init__(self):
        self.items = {}
        self.lock = threading.Lock()

    @property
    def item(self):
        return self.items.get("collector")

    def put_item(self, Item, **kwargs):
        with self.lock:
            current = self.items.get(Item["id"])
            values = kwargs["ExpressionAttributeValues"]
            same_owner = (
                current
                and current["lease_owner"] == values.get(":owner")
            )
            if ":priority" in values:
                stored_priority = (
                    current.get("lease_priority") if current else None
                )
                allowed = (
                    not current
                    or same_owner
                    or stored_priority is None
                    or stored_priority < values[":priority"]
                    or (
                        stored_priority == values[":priority"]
                        and current["lease_expires_at"] < values[":now"]
                    )
                )
            else:
                allowed = (
                    not current
                    or same_owner
                    or current["lease_expires_at"] < values[":now"]
                )
            if not allowed:
                raise ClientError(
                    {"Error": {"Code": "ConditionalCheckFailedException"}},
                    "PutItem",
                )
            self.items[Item["id"]] = Item.copy()

    def update_item(self, Key, **kwargs):
        with self.lock:
            current = self.items.get(Key["id"])
            owner = kwargs["ExpressionAttributeValues"][":owner"]
            if not current or current["lease_owner"] != owner:
                raise ClientError(
                    {"Error": {"Code": "ConditionalCheckFailedException"}},
                    "UpdateItem",
                )
            current["lease_expires_at"] = kwargs["ExpressionAttributeValues"][":expires"]

    def get_item(self, Key, **kwargs):
        with self.lock:
            item = self.items.get(Key["id"])
            return {"Item": item.copy()} if item else {}


class FailingRenewLeaseTable(FakeLeaseTable):
    def update_item(self, Key, **kwargs):
        raise RuntimeError("DynamoDB heartbeat unavailable")


class ImmediateHeartbeat:
    def wait(self, _seconds):
        return False

    def set(self):
        pass


def test_timeout_volume_is_telemetry_until_collection_deadline():
    clock = FakeClock()
    metrics = TimeoutWindowMetrics(window_seconds=120, clock=clock)

    metrics.record_success()
    for _ in range(4):
        metrics.record_timeout()

    assert metrics.snapshot() == {
        "samples": 5,
        "timeouts": 4,
        "timeout_ratio": 0.8,
    }


def test_timeout_circuit_discards_expired_samples():
    clock = FakeClock()
    metrics = TimeoutWindowMetrics(window_seconds=120, clock=clock)
    metrics.record_timeout()

    clock.now += 121
    metrics.record_success()
    metrics.record_timeout()

    assert metrics.snapshot()["samples"] == 2


def test_absolute_timeout_count_does_not_stop_collection():
    clock = FakeClock()
    metrics = TimeoutWindowMetrics(window_seconds=120, clock=clock)
    for _ in range(10):
        metrics.record_success()
    for _ in range(3):
        metrics.record_timeout()

    assert metrics.snapshot()["timeouts"] == 3


def test_dynamodb_lease_allows_one_owner_and_release():
    clock = FakeClock()
    table = FakeLeaseTable()
    first = DynamoDBLease(table=table, lease_id="collector", owner_id="job-1", clock=clock)
    second = DynamoDBLease(table=table, lease_id="collector", owner_id="job-2", clock=clock)

    assert first.acquire(start_heartbeat=False)
    assert not second.acquire(start_heartbeat=False)

    first.release()

    assert second.acquire(start_heartbeat=False)


def test_dynamodb_lease_can_replace_expired_owner():
    clock = FakeClock()
    table = FakeLeaseTable()
    first = DynamoDBLease(table=table, lease_id="collector", owner_id="job-1", clock=clock)
    second = DynamoDBLease(table=table, lease_id="collector", owner_id="job-2", clock=clock)

    assert first.acquire(start_heartbeat=False)
    clock.now += first.lease_seconds + 1

    assert second.acquire(start_heartbeat=False)


def test_newer_scheduled_job_preempts_existing_lease_without_waiting():
    clock = FakeClock()
    table = FakeLeaseTable()
    old_job = DynamoDBLease(
        table=table,
        lease_id="collector",
        owner_id="job-old",
        priority=1_000,
        clock=clock,
    )
    new_job = DynamoDBLease(
        table=table,
        lease_id="collector",
        owner_id="job-new",
        priority=1_010,
        clock=clock,
    )

    assert old_job.acquire(start_heartbeat=False)
    assert new_job.acquire(start_heartbeat=False)
    assert clock.now == 1_000
    assert table.item["lease_owner"] == "job-new"
    assert table.item["lease_priority"] == 1_010


def test_preempted_job_detects_that_newer_job_owns_the_lease():
    clock = FakeClock()
    table = FakeLeaseTable()
    old_job = DynamoDBLease(
        table=table,
        lease_id="collector",
        owner_id="job-old",
        priority=1_000,
        clock=clock,
    )
    new_job = DynamoDBLease(
        table=table,
        lease_id="collector",
        owner_id="job-new",
        priority=1_010,
        clock=clock,
    )

    assert old_job.acquire(start_heartbeat=False)
    assert new_job.acquire(start_heartbeat=False)

    assert not old_job.owns_lease()
    assert not old_job.renew()
    assert old_job.superseded()


def test_stale_scheduled_job_cannot_run_after_newer_job_releases_lease():
    clock = FakeClock()
    table = FakeLeaseTable()
    new_job = DynamoDBLease(
        table=table,
        lease_id="collector",
        owner_id="job-new",
        priority=1_010,
        clock=clock,
    )
    stale_job = DynamoDBLease(
        table=table,
        lease_id="collector",
        owner_id="job-stale",
        priority=1_000,
        clock=clock,
    )

    assert new_job.acquire(start_heartbeat=False)
    new_job.release()

    assert not stale_job.acquire(start_heartbeat=False)
    assert table.item["lease_priority"] == 1_010


def test_new_job_waits_for_previous_sps_lease_before_starting():
    clock = FakeClock()
    table = FakeLeaseTable()
    first = DynamoDBLease(
        table=table,
        lease_id="collector",
        owner_id="job-1",
        lease_seconds=60,
        clock=clock,
        sleeper=clock.sleep,
    )
    second = DynamoDBLease(
        table=table,
        lease_id="collector",
        owner_id="job-2",
        lease_seconds=60,
        clock=clock,
        sleeper=clock.sleep,
    )

    assert first.acquire(start_heartbeat=False)
    assert second.acquire_with_wait(
        timeout_seconds=70,
        poll_interval=10,
        start_heartbeat=False,
    )
    assert clock.now == 1_070
    assert table.item["lease_owner"] == "job-2"


def test_collection_deadline_supports_fixed_absolute_time():
    clock = FakeClock(now=1_100)
    deadline = CollectionDeadline(deadline_at=1_480, clock=clock)

    assert deadline.remaining_seconds() == 380
    assert not deadline.expired()

    clock.now = 1_480

    assert deadline.remaining_seconds() == 0
    assert deadline.expired()


def test_collection_deadline_starts_ten_minute_window_on_sps_fanout():
    clock = FakeClock(now=1_100)
    deadline = CollectionDeadline(
        duration_seconds=600,
        clock=clock,
    )

    assert deadline.deadline_at is None
    assert deadline.remaining_seconds() == 600

    clock.now = 1_125
    assert deadline.start() == 1_725
    assert deadline.started_at == 1_125

    clock.now = 1_724
    assert deadline.remaining_seconds() == 1
    assert not deadline.expired()

    clock.now = 1_725
    assert deadline.expired()


def test_lease_renewal_extends_ownership_while_work_is_active():
    clock = FakeClock(now=1_000)
    table = FakeLeaseTable()
    lease = DynamoDBLease(
        table=table,
        lease_id="collector",
        owner_id="job-1",
        lease_seconds=60,
        clock=clock,
    )
    assert lease.acquire(start_heartbeat=False)
    assert table.item["lease_expires_at"] == 1_060

    clock.now = 1_055
    assert lease.renew()
    assert table.item["lease_expires_at"] == 1_115


def test_process_death_recovers_active_call_lease_after_ttl():
    clock = FakeClock(now=1_000)
    table = FakeLeaseTable()
    old_job = DynamoDBLease(
        table=table,
        lease_id="collector",
        owner_id="job-old",
        lease_seconds=60,
        clock=clock,
    )
    assert old_job.acquire(start_heartbeat=False)

    clock.now = 1_061
    new_job = DynamoDBLease(
        table=table,
        lease_id="collector",
        owner_id="job-new",
        clock=clock,
    )

    assert new_job.acquire(start_heartbeat=False)
    assert table.item["lease_owner"] == "job-new"


def test_heartbeat_failure_marks_active_lease_as_lost():
    clock = FakeClock(now=1_000)
    lease = DynamoDBLease(
        table=FailingRenewLeaseTable(),
        lease_id="collector",
        owner_id="job-1",
        lease_seconds=60,
        clock=clock,
    )
    assert lease.acquire(start_heartbeat=False)
    lease._stop = ImmediateHeartbeat()

    lease._heartbeat_loop()

    assert lease.lost()
    assert not lease._acquired


def test_sps_call_coordinator_separates_priority_from_active_calls():
    clock = FakeClock()
    table = FakeLeaseTable()
    old = SPSCallCoordinator(
        priority_lease=DynamoDBLease(
            table=table,
            lease_id="priority",
            owner_id="job-old",
            priority=1_000,
            clock=clock,
        ),
        active_call_lease=DynamoDBLease(
            table=table,
            lease_id="active-calls",
            owner_id="job-old",
            clock=clock,
        ),
    )
    new = SPSCallCoordinator(
        priority_lease=DynamoDBLease(
            table=table,
            lease_id="priority",
            owner_id="job-new",
            priority=1_010,
            clock=clock,
        ),
        active_call_lease=DynamoDBLease(
            table=table,
            lease_id="active-calls",
            owner_id="job-new",
            clock=clock,
        ),
    )

    assert old.claim_priority(start_heartbeat=False)
    assert old.begin_calls(start_heartbeat=False)
    assert new.claim_priority(start_heartbeat=False)
    assert not old.owns_priority()
    assert old.superseded()
    assert not new.active_call_lease.acquire(start_heartbeat=False)

    old.finish_calls()

    assert new.begin_calls(start_heartbeat=False)
    assert table.items["active-calls"]["lease_owner"] == "job-new"


def test_collection_deadline_starts_after_call_handoff_and_releases_call_slot():
    clock = FakeClock(now=1_000)
    events = []

    def acquire_call_slot():
        events.append("acquire")
        clock.now += 45

    deadline = CollectionDeadline(
        duration_seconds=600,
        clock=clock,
        before_start=acquire_call_slot,
        on_finish=lambda: events.append("release"),
    )

    assert deadline.start() == 1_645
    assert deadline.started_at == 1_045
    deadline.finish()
    deadline.finish()

    assert events == ["acquire", "release"]


def test_waiting_sps_job_cannot_start_after_a_newer_priority_arrives():
    clock = FakeClock()
    table = FakeLeaseTable()
    active_owner = DynamoDBLease(
        table=table,
        lease_id="active-calls",
        owner_id="active-job",
        clock=clock,
    )
    waiting = SPSCallCoordinator(
        priority_lease=DynamoDBLease(
            table=table,
            lease_id="priority",
            owner_id="waiting-job",
            priority=1_010,
            clock=clock,
        ),
        active_call_lease=DynamoDBLease(
            table=table,
            lease_id="active-calls",
            owner_id="waiting-job",
            clock=clock,
            sleeper=clock.sleep,
        ),
        handoff_timeout_seconds=10,
        poll_interval=1,
    )
    newer_priority = DynamoDBLease(
        table=table,
        lease_id="priority",
        owner_id="newest-job",
        priority=1_020,
        clock=clock,
    )

    assert active_owner.acquire(start_heartbeat=False)
    assert waiting.claim_priority(start_heartbeat=False)
    assert newer_priority.acquire(start_heartbeat=False)
    assert not waiting.owns_priority()

    assert not waiting.begin_calls(start_heartbeat=False)
    assert table.items["active-calls"]["lease_owner"] == "active-job"


def test_active_call_lease_remains_exclusive_until_inflight_calls_finish():
    clock = FakeClock(now=1_000)
    table = FakeLeaseTable()
    old = SPSCallCoordinator(
        priority_lease=DynamoDBLease(
            table=table,
            lease_id="priority",
            owner_id="job-old",
            priority=1_000,
            clock=clock,
        ),
        active_call_lease=DynamoDBLease(
            table=table,
            lease_id="active-calls",
            owner_id="job-old",
            lease_seconds=60,
            clock=clock,
        ),
    )

    assert old.begin_calls(start_heartbeat=False)

    # Model heartbeat renewals while the logical ten-minute window passes and
    # an HTTP request is still draining.
    for now in range(1_050, 1_601, 50):
        clock.now = now
        assert old.active_call_lease.renew()
    clock.now = 1_601

    new_active_lease = DynamoDBLease(
        table=table,
        lease_id="active-calls",
        owner_id="job-new",
        lease_seconds=60,
        clock=clock,
    )

    assert not new_active_lease.acquire(start_heartbeat=False)

    old.finish_calls()
    assert new_active_lease.acquire(start_heartbeat=False)


def test_coordinator_stops_fanout_when_active_call_heartbeat_is_lost():
    clock = FakeClock(now=1_000)
    table = FakeLeaseTable()
    coordinator = SPSCallCoordinator(
        priority_lease=DynamoDBLease(
            table=table,
            lease_id="priority",
            owner_id="job-1",
            priority=1_000,
            clock=clock,
        ),
        active_call_lease=DynamoDBLease(
            table=table,
            lease_id="active-calls",
            owner_id="job-1",
            clock=clock,
        ),
    )
    assert coordinator.begin_calls(start_heartbeat=False)

    coordinator.active_call_lease._lost.set()

    assert coordinator.cancellation_reason() == "ACTIVE_LEASE_LOST"
