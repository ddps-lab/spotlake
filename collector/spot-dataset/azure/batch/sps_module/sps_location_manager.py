import re
import requests
import traceback
import sys
import os
import math
from datetime import datetime, timedelta, timezone

# Add parent directory to path to import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sps_module import sps_shared_resources
from utils.common import S3
from utils.constants import AZURE_CONST

SS_Resources = sps_shared_resources

CALL_LIMIT_PER_PAIR = 10
CALL_HISTORY_MINUTES = 62
HEALTH_SAMPLE_SIZE = 10
OUTLIER_BAD_SAMPLE_COUNT = 4
SLOW_CALL_SECONDS = 20
EXCLUSION_MINUTES = 30
PROBE_SUCCESS_SECONDS = 10
PROBE_SUCCESS_REQUIRED = 2
CALL_CAPACITY_RESERVE_RATIO = 1.2


def _utc_now(now=None):
    if now is None:
        return datetime.now(timezone.utc)
    if now.tzinfo is None:
        return now.replace(tzinfo=timezone.utc)
    return now.astimezone(timezone.utc)


def _naive_utc(now=None):
    return _utc_now(now).replace(tzinfo=None)


def _parse_health_time(value):
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _empty_location_state():
    return {
        "recent": [],
        "excluded_until": None,
        "probe_successes": 0,
    }


def _ensure_location_health():
    health = getattr(SS_Resources, "location_health_tmp", None)
    if not isinstance(health, dict):
        health = {"version": 1, "locations": {}}
        SS_Resources.location_health_tmp = health
    health["version"] = 1
    locations = health.setdefault("locations", {})
    if not isinstance(locations, dict):
        locations = {}
        health["locations"] = locations
    for location in getattr(SS_Resources, "available_locations", None) or []:
        state = locations.get(location)
        if not isinstance(state, dict):
            state = _empty_location_state()
            locations[location] = state
        if not isinstance(state.get("recent"), list):
            state["recent"] = []
        state.setdefault("excluded_until", None)
        state.setdefault("probe_successes", 0)
    return locations


def _is_outlier(state):
    recent = state.get("recent", [])[-HEALTH_SAMPLE_SIZE:]
    return (
        len(recent) >= HEALTH_SAMPLE_SIZE
        and sum(bool(sample.get("bad")) for sample in recent)
        >= OUTLIER_BAD_SAMPLE_COUNT
    )


def _bad_sample_count(state):
    return sum(
        bool(sample.get("bad"))
        for sample in state.get("recent", [])[-HEALTH_SAMPLE_SIZE:]
    )


def _over_limit(subscription_id, location):
    return location in (
        (SS_Resources.locations_over_limit_tmp or {}).get(subscription_id, {})
    )


def calculate_available_call_slots(*, excluded_locations=None, now=None):
    clean_expired_over_limit_locations(now=now)
    clean_expired_over_call_history_locations(now=now)
    excluded = set(excluded_locations or ())
    total = 0
    for subscription_id in SS_Resources.subscriptions or []:
        subscription_history = (
            SS_Resources.locations_call_history_tmp or {}
        ).get(subscription_id, {})
        for location in SS_Resources.available_locations or []:
            if location in excluded or _over_limit(subscription_id, location):
                continue
            used = len(subscription_history.get(location, []))
            total += max(0, CALL_LIMIT_PER_PAIR - used)
    return total


def _try_exclude_location(location, *, now, set_new_expiry):
    excluded = set(SS_Resources.effective_excluded_locations)
    excluded.add(location)
    available_slots = calculate_available_call_slots(
        excluded_locations=excluded,
        now=now,
    )
    required_slots = SS_Resources.required_sps_call_slots
    was_excluded = location in SS_Resources.effective_excluded_locations
    was_fallback = location in SS_Resources.fallback_locations
    state = _ensure_location_health()[location]

    if available_slots >= required_slots:
        SS_Resources.effective_excluded_locations.add(location)
        SS_Resources.fallback_locations.discard(location)
        if set_new_expiry:
            state["excluded_until"] = (
                _utc_now(now) + timedelta(minutes=EXCLUSION_MINUTES)
            ).isoformat()
            state["probe_successes"] = 0
        if not was_excluded:
            print(
                "[SPS_LOCATION_HEALTH] action=exclude "
                f"location={location} bad_samples={_bad_sample_count(state)}/"
                f"{HEALTH_SAMPLE_SIZE} available_slots={available_slots} "
                f"required_slots={required_slots} "
                f"excluded_until={state.get('excluded_until')}"
            )
        return True

    SS_Resources.effective_excluded_locations.discard(location)
    SS_Resources.fallback_locations.add(location)
    if not was_fallback:
        print(
            "[SPS_LOCATION_HEALTH] action=fallback "
            f"location={location} bad_samples={_bad_sample_count(state)}/"
            f"{HEALTH_SAMPLE_SIZE} available_slots={available_slots} "
            f"required_slots={required_slots}"
        )
    return False


def prepare_location_routing(*, total_request_count, now=None):
    now = _utc_now(now)
    locations = _ensure_location_health()
    SS_Resources.required_sps_call_slots = math.ceil(
        total_request_count * CALL_CAPACITY_RESERVE_RATIO
    )
    SS_Resources.effective_excluded_locations = set()
    SS_Resources.fallback_locations = set()
    SS_Resources.probe_locations_pending = []
    SS_Resources.probed_locations_this_job = set()

    candidates = []
    for location, state in locations.items():
        if not _is_outlier(state) and not state.get("probe_successes"):
            continue
        excluded_until = _parse_health_time(state.get("excluded_until"))
        candidates.append(
            (
                -_bad_sample_count(state),
                location,
                bool(excluded_until and excluded_until <= now),
                bool(excluded_until and excluded_until > now),
            )
        )

    for _, location, probe_due, exclusion_active in sorted(candidates):
        can_exclude = _try_exclude_location(
            location,
            now=now,
            set_new_expiry=not (probe_due or exclusion_active),
        )
        if can_exclude and probe_due:
            SS_Resources.probe_locations_pending.append(location)

    available_slots = calculate_available_call_slots(
        excluded_locations=SS_Resources.effective_excluded_locations,
        now=now,
    )
    print(
        "[SPS_LOCATION_CAPACITY] "
        f"available_slots={available_slots} "
        f"required_slots={SS_Resources.required_sps_call_slots} "
        f"excluded_locations={sorted(SS_Resources.effective_excluded_locations)} "
        f"fallback_locations={sorted(SS_Resources.fallback_locations)}"
    )
    return available_slots


def record_location_result(
    location,
    *,
    outcome,
    elapsed_seconds,
    route_mode="normal",
    now=None,
):
    now = _utc_now(now)
    state = _ensure_location_health().setdefault(
        location,
        _empty_location_state(),
    )
    bad = outcome == "timeout" or elapsed_seconds >= SLOW_CALL_SECONDS
    state["recent"].append(
        {
            "at": now.isoformat(),
            "bad": bad,
            "outcome": outcome,
            "elapsed_seconds": round(float(elapsed_seconds), 3),
        }
    )
    state["recent"] = state["recent"][-HEALTH_SAMPLE_SIZE:]

    if route_mode == "probe":
        fast_success = (
            outcome == "success" and elapsed_seconds < PROBE_SUCCESS_SECONDS
        )
        if fast_success:
            state["probe_successes"] += 1
            if state["probe_successes"] >= PROBE_SUCCESS_REQUIRED:
                state["recent"] = []
                state["excluded_until"] = None
                state["probe_successes"] = 0
                SS_Resources.effective_excluded_locations.discard(location)
                SS_Resources.fallback_locations.discard(location)
                print(
                    "[SPS_LOCATION_HEALTH] action=recover "
                    f"location={location} probe_elapsed_seconds={elapsed_seconds:.3f}"
                )
            return

        state["probe_successes"] = 0
        state["excluded_until"] = (
            now + timedelta(minutes=EXCLUSION_MINUTES)
        ).isoformat()
        SS_Resources.effective_excluded_locations.add(location)
        SS_Resources.fallback_locations.discard(location)
        print(
            "[SPS_LOCATION_HEALTH] action=extend_exclusion "
            f"location={location} probe_outcome={outcome} "
            f"probe_elapsed_seconds={elapsed_seconds:.3f} "
            f"excluded_until={state['excluded_until']}"
        )
        return

    if _is_outlier(state):
        if (
            location in SS_Resources.effective_excluded_locations
            or location in SS_Resources.fallback_locations
        ):
            return
        excluded_until = _parse_health_time(state.get("excluded_until"))
        active = bool(excluded_until and excluded_until > now)
        _try_exclude_location(
            location,
            now=now,
            set_new_expiry=not active,
        )


def check_and_add_available_locations(az):
    try:
        SS_Resources.available_locations = collect_available_locations()
        az_str = f"availability-zones-{str(az).lower()}"
        available_locations_path = f"{AZURE_CONST.S3_SAVED_VARIABLE_PATH}/{az_str}/{AZURE_CONST.S3_AVAILABLE_LOCATIONS_JSON_FILENAME}"

        if not SS_Resources.available_locations:
            print("No available locations collected. Reading the S3 file")
            SS_Resources.available_locations = S3.read_file(available_locations_path, 'json')
        else:
            S3.upload_file(SS_Resources.available_locations, available_locations_path, 'json')


        if SS_Resources.locations_call_history_tmp is None:
            SS_Resources.locations_call_history_tmp = {}

        updated = False
        for subscription_id in SS_Resources.subscriptions:
            if subscription_id not in SS_Resources.locations_call_history_tmp:
                SS_Resources.locations_call_history_tmp[subscription_id] = {}

            if subscription_id not in SS_Resources.locations_over_limit_tmp:
                SS_Resources.locations_over_limit_tmp[subscription_id] = {}

            for location in SS_Resources.available_locations:
                if location not in SS_Resources.locations_call_history_tmp[subscription_id]:
                    SS_Resources.locations_call_history_tmp[subscription_id][location] = []
                    updated = True

        if updated:
            _ensure_location_health()
            print("Updated available locations to locations_call_history_tmp or locations_call_history_tmp successfully.")
            return True
        else:
            _ensure_location_health()
            print("No new available locations found. locations_call_history_tmp or locations_call_history_tmp unchanged.")
            return True

    except Exception as e:
        print(f"Error in check_and_add_available_locations: {e}")
        return False


def validation_can_call(subscription_id, location):
    if SS_Resources.locations_over_limit_tmp.get(subscription_id):
        if ((location not in SS_Resources.locations_over_limit_tmp.get(subscription_id))
                and (len(SS_Resources.locations_call_history_tmp[subscription_id][location]) < CALL_LIMIT_PER_PAIR)):
            return True
    else:
        if len(SS_Resources.locations_call_history_tmp[subscription_id][location]) < CALL_LIMIT_PER_PAIR:
            return True
    return False


def _next_pair_start(subs, locs):
    last_pair = getattr(
        SS_Resources,
        "last_subscription_id_and_location_tmp",
        None,
    ) or {}
    last_sub_id = last_pair.get("last_subscription_id")
    last_loc = last_pair.get("last_location")

    if last_sub_id in subs and last_loc in locs:
        s_idx = subs.index(last_sub_id)
        l_idx = (locs.index(last_loc) + 1) % len(locs)
        if l_idx == 0:
            s_idx = (s_idx + 1) % len(subs)
        return s_idx, l_idx
    return 0, 0


def _claim_pair(subscription_id, location, route_mode, now):
    SS_Resources.succeed_to_get_next_available_location_count += 1
    SS_Resources.succeed_to_get_next_available_location_count_all += 1
    SS_Resources.locations_call_history_tmp[subscription_id][location].append(
        _naive_utc(now).isoformat()
    )
    SS_Resources.last_subscription_id_and_location_tmp = {
        "last_subscription_id": subscription_id,
        "last_location": location,
    }
    return subscription_id, location, route_mode


def _select_pair(subs, locs, *, allowed, route_mode, now):
    s_idx, l_idx = _next_pair_start(subs, locs)
    attempts = 0
    while attempts < len(subs) * len(locs):
        subscription_id = subs[s_idx]
        location = locs[l_idx]
        if allowed(location) and validation_can_call(subscription_id, location):
            return _claim_pair(subscription_id, location, route_mode, now)
        l_idx = (l_idx + 1) % len(locs)
        if l_idx == 0:
            s_idx = (s_idx + 1) % len(subs)
        attempts += 1
    return None


def get_next_available_location(*, now=None):
    try:
        if SS_Resources.locations_call_history_tmp is None or SS_Resources.locations_over_limit_tmp is None:
            return None

        clean_expired_over_limit_locations(now=now)
        clean_expired_over_call_history_locations(now=now)

        subs = SS_Resources.subscriptions
        locs = SS_Resources.available_locations
        if not subs or not locs:
            return None

        while SS_Resources.probe_locations_pending:
            probe_location = SS_Resources.probe_locations_pending.pop(0)
            if probe_location in SS_Resources.probed_locations_this_job:
                continue
            selection = _select_pair(
                subs,
                locs,
                allowed=lambda location: location == probe_location,
                route_mode="probe",
                now=now,
            )
            SS_Resources.probed_locations_this_job.add(probe_location)
            if selection:
                return selection

        excluded = SS_Resources.effective_excluded_locations
        fallback = SS_Resources.fallback_locations
        selection = _select_pair(
            subs,
            locs,
            allowed=lambda location: location not in excluded
            and location not in fallback,
            route_mode="normal",
            now=now,
        )
        if selection:
            return selection

        selection = _select_pair(
            subs,
            locs,
            allowed=lambda location: location in fallback,
            route_mode="fallback",
            now=now,
        )
        if selection:
            return selection

        selection = _select_pair(
            subs,
            locs,
            allowed=lambda location: location in excluded,
            route_mode="fallback",
            now=now,
        )
        if selection:
            print(
                "[SPS_LOCATION_HEALTH] action=emergency_fallback "
                f"location={selection[1]}"
            )
            return selection

    except Exception as e:
        print("\n[ERROR] Exception occurred in get_next_available_location:")
        print(traceback.format_exc())
        print(f"\n[ERROR] Failed to get_next_available_location: {e}")
        return None
    return None


def collect_available_locations():
    print("Start to collect_available_locations")
    subscription_id = SS_Resources.subscriptions[0]
    location = "ERROR_LOCATION"

    try:
        url = f"https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.Compute/locations/{location}/placementScores/spot/generate?api-version=2025-06-05"
        headers = {
            "Authorization": f"Bearer {sps_shared_resources.sps_token}",
            "Content-Type": "application/json"
        }
        request_body = {
                "availabilityZones": False,
                "desiredCount": 1,
                "desiredLocations": ["korea"],
                "desiredSizes": [{"sku": "Standard_D2_v3"}],
            }

        response = requests.post(url, headers=headers, json=request_body, timeout=15)
        response.raise_for_status()

    except requests.exceptions.HTTPError as http_err:
        available_locations_tmp = re.search(r"supported locations are '([^']+)'", http_err.response.text)
        if available_locations_tmp:
            available_locations = available_locations_tmp.group(1).split(', ')
            return available_locations

    except Exception as e:
        print(f"Failed to collect_available_locations, Error: {e}")
        return None
    return None

def clean_expired_over_limit_locations(*, now=None):
    if SS_Resources.locations_over_limit_tmp:
        for subscription_id in SS_Resources.subscriptions:
            one_hour_ago = _naive_utc(now) - timedelta(minutes=CALL_HISTORY_MINUTES)
            for location_key, location_value in list(
                    SS_Resources.locations_over_limit_tmp[subscription_id].items()):
                dt = datetime.fromisoformat(location_value)
                if dt <= one_hour_ago:
                    del SS_Resources.locations_over_limit_tmp[subscription_id][location_key]


def clean_expired_over_call_history_locations(*, now=None):
    if SS_Resources.locations_call_history_tmp:
        one_hour_ago = _naive_utc(now) - timedelta(minutes=CALL_HISTORY_MINUTES)
        for subscription_id in SS_Resources.subscriptions:
            subscription_data = SS_Resources.locations_call_history_tmp.get(subscription_id, {})

            new_subscription_data = {
                location: [
                    t for t in timestamps if datetime.fromisoformat(t) > one_hour_ago
                ]
                for location, timestamps in subscription_data.items()
            }
            SS_Resources.locations_call_history_tmp[subscription_id] = new_subscription_data


def update_call_history(subscription_id, location):
    try:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        current_timestamp = now.isoformat()
        SS_Resources.locations_call_history_tmp[subscription_id][location].append(current_timestamp)
        return True

    except Exception as e:
        print(f"Failed to update_call_history: {e}")
        return False

def update_over_limit_locations(subscription_id, location):
    try:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        current_timestamp = now.isoformat()
        SS_Resources.locations_over_limit_tmp[subscription_id][location] = current_timestamp

        print("Successfully update_over_limit_locations. Subscription ID:" + subscription_id.split('-')[
            0] + ", Location:", location + ", Time:", now.strftime('%Y-%m-%d %H:%M:%S'))
        return True

    except Exception as e:
        print(f"Failed to update_all_over_limit_locations: {e}")
        return False
