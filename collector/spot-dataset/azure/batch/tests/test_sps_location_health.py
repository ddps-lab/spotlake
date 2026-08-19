import importlib.util
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _load_location_manager(monkeypatch, *, subscriptions=None, locations=None):
    subscriptions = subscriptions or ["sub-a", "sub-b"]
    locations = locations or ["healthy", "slow"]

    resources = types.ModuleType("sps_shared_resources")
    resources.subscriptions = list(subscriptions)
    resources.available_locations = list(locations)
    resources.locations_call_history_tmp = {
        subscription: {location: [] for location in locations}
        for subscription in subscriptions
    }
    resources.locations_over_limit_tmp = {
        subscription: {} for subscription in subscriptions
    }
    resources.last_subscription_id_and_location_tmp = {}
    resources.location_health_tmp = {"version": 1, "locations": {}}
    resources.required_sps_call_slots = 0
    resources.effective_excluded_locations = set()
    resources.fallback_locations = set()
    resources.probe_locations_pending = []
    resources.probed_locations_this_job = set()
    resources.succeed_to_get_next_available_location_count = 0
    resources.succeed_to_get_next_available_location_count_all = 0

    sps_module = types.ModuleType("sps_module")
    sps_module.sps_shared_resources = resources

    common = types.ModuleType("utils.common")
    common.S3 = object()

    constants = types.ModuleType("utils.constants")
    constants.AZURE_CONST = types.SimpleNamespace(
        S3_SAVED_VARIABLE_PATH="state",
        S3_AVAILABLE_LOCATIONS_JSON_FILENAME="locations.json",
    )

    utils = types.ModuleType("utils")
    monkeypatch.setitem(sys.modules, "sps_module", sps_module)
    monkeypatch.setitem(sys.modules, "utils", utils)
    monkeypatch.setitem(sys.modules, "utils.common", common)
    monkeypatch.setitem(sys.modules, "utils.constants", constants)

    module_path = (
        Path(__file__).parents[1]
        / "sps_module"
        / "sps_location_manager.py"
    )
    spec = importlib.util.spec_from_file_location(
        "azure_sps_location_manager_test",
        module_path,
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, resources


def _record_outlier(manager, *, location, now):
    for index in range(10):
        manager.record_location_result(
            location,
            outcome="timeout" if index < 4 else "success",
            elapsed_seconds=50 if index < 4 else 2,
            now=now + timedelta(seconds=index),
        )


def test_outlier_is_excluded_when_healthy_capacity_covers_the_job(monkeypatch):
    manager, resources = _load_location_manager(monkeypatch)
    now = datetime(2026, 8, 6, 5, 0, tzinfo=timezone.utc)
    manager.prepare_location_routing(total_request_count=10, now=now)

    _record_outlier(manager, location="slow", now=now)

    assert resources.required_sps_call_slots == 12
    assert resources.effective_excluded_locations == {"slow"}
    assert resources.fallback_locations == set()
    selection = manager.get_next_available_location(now=now + timedelta(minutes=1))
    assert selection[1:] == ("healthy", "normal")


def test_outlier_remains_fallback_when_exclusion_would_remove_needed_capacity(
    monkeypatch,
):
    manager, resources = _load_location_manager(
        monkeypatch,
        subscriptions=["sub-a"],
    )
    now = datetime(2026, 8, 6, 5, 0, tzinfo=timezone.utc)
    manager.prepare_location_routing(total_request_count=15, now=now)

    _record_outlier(manager, location="slow", now=now)

    assert resources.required_sps_call_slots == 18
    assert resources.effective_excluded_locations == set()
    assert resources.fallback_locations == {"slow"}

    resources.locations_call_history_tmp["sub-a"]["healthy"] = [
        (now + timedelta(seconds=index)).replace(tzinfo=None).isoformat()
        for index in range(10)
    ]
    selection = manager.get_next_available_location(now=now + timedelta(minutes=1))
    assert selection == ("sub-a", "slow", "fallback")


def test_over_limit_pairs_are_removed_from_capacity(monkeypatch):
    manager, resources = _load_location_manager(monkeypatch)
    now = datetime(2026, 8, 6, 5, 0, tzinfo=timezone.utc)
    resources.locations_over_limit_tmp["sub-a"]["healthy"] = now.replace(
        tzinfo=None
    ).isoformat()

    available = manager.calculate_available_call_slots(now=now)

    assert available == 30


def test_expired_exclusion_recovers_after_two_fast_probes(monkeypatch):
    manager, resources = _load_location_manager(monkeypatch)
    first_job = datetime(2026, 8, 6, 5, 0, tzinfo=timezone.utc)
    state = {
        "recent": [
            {
                "at": (first_job - timedelta(minutes=31)).isoformat(),
                "bad": True,
            }
            for _ in range(4)
        ]
        + [
            {
                "at": (first_job - timedelta(minutes=31)).isoformat(),
                "bad": False,
            }
            for _ in range(6)
        ],
        "excluded_until": (first_job - timedelta(minutes=1)).isoformat(),
        "probe_successes": 0,
    }
    resources.location_health_tmp["locations"]["slow"] = state

    manager.prepare_location_routing(total_request_count=10, now=first_job)
    first_probe = manager.get_next_available_location(now=first_job)
    assert first_probe[1:] == ("slow", "probe")
    manager.record_location_result(
        "slow",
        outcome="success",
        elapsed_seconds=5,
        route_mode="probe",
        now=first_job,
    )
    assert state["probe_successes"] == 1

    second_job = first_job + timedelta(minutes=10)
    manager.prepare_location_routing(total_request_count=10, now=second_job)
    second_probe = manager.get_next_available_location(now=second_job)
    assert second_probe[1:] == ("slow", "probe")
    manager.record_location_result(
        "slow",
        outcome="success",
        elapsed_seconds=4,
        route_mode="probe",
        now=second_job,
    )

    assert state["probe_successes"] == 0
    assert state["excluded_until"] is None
    assert state["recent"] == []
    assert "slow" not in resources.effective_excluded_locations


def test_failed_probe_extends_exclusion(monkeypatch):
    manager, resources = _load_location_manager(monkeypatch)
    now = datetime(2026, 8, 6, 5, 0, tzinfo=timezone.utc)
    resources.location_health_tmp["locations"]["slow"] = {
        "recent": [{"at": now.isoformat(), "bad": True}] * 10,
        "excluded_until": (now - timedelta(minutes=1)).isoformat(),
        "probe_successes": 1,
    }

    manager.prepare_location_routing(total_request_count=10, now=now)
    probe = manager.get_next_available_location(now=now)
    assert probe[1:] == ("slow", "probe")
    manager.record_location_result(
        "slow",
        outcome="timeout",
        elapsed_seconds=10,
        route_mode="probe",
        now=now,
    )

    state = resources.location_health_tmp["locations"]["slow"]
    assert state["probe_successes"] == 0
    assert datetime.fromisoformat(state["excluded_until"]) == now + timedelta(
        minutes=30
    )
    assert "slow" in resources.effective_excluded_locations


def test_production_shape_routes_2300_requests_away_from_ten_outliers(
    monkeypatch,
):
    subscriptions = [f"sub-{index:03d}" for index in range(181)]
    healthy_locations = [f"healthy-{index:02d}" for index in range(49)]
    outliers = [f"outlier-{index:02d}" for index in range(10)]
    manager, resources = _load_location_manager(
        monkeypatch,
        subscriptions=subscriptions,
        locations=healthy_locations + outliers,
    )
    now = datetime(2026, 8, 6, 5, 0, tzinfo=timezone.utc)
    manager.prepare_location_routing(total_request_count=2_300, now=now)

    for location in outliers:
        _record_outlier(manager, location=location, now=now)

    assert resources.effective_excluded_locations == set(outliers)
    assert manager.calculate_available_call_slots(
        excluded_locations=resources.effective_excluded_locations,
        now=now,
    ) == 88_690

    selections = [
        manager.get_next_available_location(now=now + timedelta(minutes=1))
        for _ in range(2_300)
    ]
    assert all(selection is not None for selection in selections)
    assert all(selection[1] in healthy_locations for selection in selections)
    assert all(selection[2] == "normal" for selection in selections)


def test_missing_or_malformed_health_state_starts_empty(monkeypatch):
    manager, resources = _load_location_manager(monkeypatch)
    now = datetime(2026, 8, 6, 5, 0, tzinfo=timezone.utc)
    resources.location_health_tmp = {"locations": {"slow": "invalid"}}

    manager.prepare_location_routing(total_request_count=10, now=now)

    state = resources.location_health_tmp["locations"]["slow"]
    assert state == {
        "recent": [],
        "excluded_until": None,
        "probe_successes": 0,
    }
    assert resources.effective_excluded_locations == set()


def test_malformed_exclusion_timestamp_does_not_disable_location_health(
    monkeypatch,
):
    manager, resources = _load_location_manager(monkeypatch)
    now = datetime(2026, 8, 6, 5, 0, tzinfo=timezone.utc)
    resources.location_health_tmp["locations"]["slow"] = {
        "recent": [{"at": now.isoformat(), "bad": True}] * 10,
        "excluded_until": "invalid-timestamp",
        "probe_successes": 0,
    }

    manager.prepare_location_routing(total_request_count=10, now=now)

    state = resources.location_health_tmp["locations"]["slow"]
    assert datetime.fromisoformat(state["excluded_until"]) == now + timedelta(
        minutes=30
    )
    assert resources.effective_excluded_locations == {"slow"}
