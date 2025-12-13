import re
import requests
import traceback
import sys
import os
from datetime import datetime, timedelta, timezone

# Add parent directory to path to import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sps_module import sps_shared_resources
from utils.common import S3
from utils.constants import AZURE_CONST

SS_Resources = sps_shared_resources

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
            print("Updated available locations to locations_call_history_tmp or locations_call_history_tmp successfully.")
            return True
        else:
            print("No new available locations found. locations_call_history_tmp or locations_call_history_tmp unchanged.")
            return True

    except Exception as e:
        print(f"Error in check_and_add_available_locations: {e}")
        return False


def validation_can_call(subscription_id, location):
    if SS_Resources.locations_over_limit_tmp.get(subscription_id):
        if ((location not in SS_Resources.locations_over_limit_tmp.get(subscription_id))
                and (len(SS_Resources.locations_call_history_tmp[subscription_id][location]) < 10)):
            return True
    else:
        if len(SS_Resources.locations_call_history_tmp[subscription_id][location]) < 10:
            return True
    return False


def get_next_available_location():
    try:
        if SS_Resources.locations_call_history_tmp is None or SS_Resources.locations_over_limit_tmp is None:
            return None

        clean_expired_over_limit_locations()
        clean_expired_over_call_history_locations()

        subs = SS_Resources.subscriptions
        locs = SS_Resources.available_locations
        if not subs or not locs:
            return None

        n, m = len(subs), len(locs)

        last_pair = getattr(SS_Resources, "last_subscription_id_and_location_tmp", None) or {}
        last_sub_id = last_pair.get("last_subscription_id")
        last_loc = last_pair.get("last_location")

        if last_sub_id in subs and last_loc in locs:
            s_idx = subs.index(last_sub_id)
            l_idx = locs.index(last_loc)
            l_idx = (l_idx + 1) % m
            if l_idx == 0:
                s_idx = (s_idx + 1) % n
        else:
            s_idx, l_idx = 0, 0

        attempts = 0
        while attempts < n * m:
            sub_id = subs[s_idx]
            loc = locs[l_idx]

            if validation_can_call(sub_id, loc):
                SS_Resources.succeed_to_get_next_available_location_count += 1
                SS_Resources.succeed_to_get_next_available_location_count_all += 1

                SS_Resources.locations_call_history_tmp[sub_id][loc].append(
                    datetime.now(timezone.utc).replace(tzinfo=None).isoformat())

                SS_Resources.last_subscription_id_and_location_tmp = {
                    "last_subscription_id": sub_id,
                    "last_location": loc,
                }
                return sub_id, loc

            l_idx = (l_idx + 1) % m
            if l_idx == 0:
                s_idx = (s_idx + 1) % n

            attempts += 1

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

def clean_expired_over_limit_locations():
    if SS_Resources.locations_over_limit_tmp:
        for subscription_id in SS_Resources.subscriptions:
            one_hour_ago = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=62)
            for location_key, location_value in list(
                    SS_Resources.locations_over_limit_tmp[subscription_id].items()):
                dt = datetime.fromisoformat(location_value)
                if dt <= one_hour_ago:
                    del SS_Resources.locations_over_limit_tmp[subscription_id][location_key]


def clean_expired_over_call_history_locations():
    if SS_Resources.locations_call_history_tmp:
        one_hour_ago = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=62)
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
