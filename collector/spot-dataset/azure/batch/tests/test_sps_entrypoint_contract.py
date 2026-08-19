from pathlib import Path


BATCH_ROOT = Path(__file__).parents[1]


def test_orchestrator_does_not_treat_lease_contention_as_successful_skip():
    script = (BATCH_ROOT / "scripts" / "run_collection.sh").read_text()

    assert "sps_collection_skipped" not in script
    assert 'wait "$PID_SPS" || STATUS_SPS=$?' in script
    assert "One or more collection jobs failed." in script


def test_orchestrator_saves_price_and_if_when_sps_fails():
    script = (BATCH_ROOT / "scripts" / "run_collection.sh").read_text()

    assert '--sps-unavailable' in script
    assert '--sps_key "$SPS_KEY" --sps-partial' in script
    assert 'if [ "$STATUS_SPS" -ne 0 ]' in script
    assert "Partial snapshot saved; SPS collection remains failed." in script


def test_sps_entrypoint_limits_coordination_to_sps_call_phase():
    source = (BATCH_ROOT / "sps" / "collect_sps.py").read_text()
    load_source = (BATCH_ROOT / "sps" / "load_sps.py").read_text()

    assert 'SPS_COLLECTION_BUDGET_SECONDS", "600"' in source
    assert "duration_seconds=SPS_COLLECTION_BUDGET_SECONDS" in source
    assert "SPS_ACTIVE_CALL_LEASE_ID" in source
    assert "SPS_PRIORITY_LEASE_ID" in source
    assert "before_start=begin_sps_calls" in source
    assert "on_finish=finish_sps_calls" in source
    assert "priority=int(timestamp_utc.timestamp())" in source
    assert "cancelled=call_coordinator.cancellation_reason" in source
    assert "call_coordinator.claim_priority()" not in source
    assert "cap_calls_at" not in source
    assert "collection_deadline.start()" in load_source
    assert 'getattr(collection_deadline, "finish", None)' in load_source
    assert "finish_collection()" in load_source
    assert "raise load_sps.SPSCollectionSupersededError" not in source
    assert "timestamp_utc.timestamp() + SPS_COLLECTION_BUDGET_SECONDS" not in source
    assert "Skipping this SPS run" not in source
    assert "SPSLeaseUnavailableError" in source
