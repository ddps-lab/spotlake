import re
import requests
import traceback
import random
from sps_module import sps_shared_resources
from datetime import datetime, timedelta, timezone
from utils.pub_service import S3, AZURE_CONST

SS_Resources = sps_shared_resources

def check_and_add_available_locations(az):
    """
    이 메서드는 최신 사용 가능한 location을 수집하고, locations_call_history_tmp 변수에 저장된 기존 구독 기록과 비교하고 갱신합니다.
    """
    try:
        SS_Resources.available_locations = collect_available_locations()
        az_str = f"availability-zones-{str(az).lower()}"
        available_locations_path = f"{AZURE_CONST.S3_SAVED_VARIABLE_PATH}/{az_str}/{AZURE_CONST.S3_AVAILABLE_LOCATIONS_JSON_FILENAME}"

        if not SS_Resources.available_locations:
            print("No available locations collected. Reading the S3 file")
            SS_Resources.available_locations = S3.read_file(available_locations_path, 'json')
        else:
            S3.upload_file(SS_Resources.available_locations, available_locations_path, 'json')


        # 기존 location_call_history 데이터가 없으면 빈 딕셔너리로 초기화
        if SS_Resources.locations_call_history_tmp is None:
            SS_Resources.locations_call_history_tmp = {}

        # 새로운 location을 기존 기록과 비교하여 추가
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

    except Exception as e:
        print(f"Error in check_and_add_available_locations: {e}")
        return False


def validation_can_call(subscription_id, location):
    """
    이 메서드는 지정된 location으로 호출 가능한지 확인합니다.
    초과 요청 여부와 호출 이력의 크기를 기준으로 판단합니다.
    """
    if SS_Resources.locations_over_limit_tmp.get(subscription_id):
        if ((location not in SS_Resources.locations_over_limit_tmp.get(subscription_id))
                and (len(SS_Resources.locations_call_history_tmp[subscription_id][location]) < 10)):
            return True
    else:
        if len(SS_Resources.locations_call_history_tmp[subscription_id][location]) < 10:
            return True
    return False


def get_next_available_location():
    """
     이 메서드는 사용 가능한 다음 위치를 리턴합니다.
     호출 이력과 초과 요청 데이터를 기반으로 적절한 위치를 선택합니다.
     호출 시 이용하는 location은 구독 내에 지난 호출의 location을 이용 안 해야 하는 로직이 들어갑니다.

     이유:
     단 기간에 같은 location으로 호출 시, timeout율이 놉습니다.
     """
    try:
        if SS_Resources.locations_call_history_tmp is None or SS_Resources.locations_over_limit_tmp is None:
            return None

        # Clean expired data
        clean_expired_over_limit_locations()
        clean_expired_over_call_history_locations()

        start_subscription_index = random.randint(0, len(SS_Resources.subscriptions) - 1)

        for i in range(len(SS_Resources.subscriptions)):
            subscription_index = (start_subscription_index + i) % len(SS_Resources.subscriptions)
            subscription_id = SS_Resources.subscriptions[subscription_index]

            start_location_index = random.randint(0, len(SS_Resources.available_locations) - 1)
            for j in range(len(SS_Resources.available_locations)):
                location_index = (start_location_index + j) % len(SS_Resources.available_locations)
                location = SS_Resources.available_locations[location_index]

                if validation_can_call(subscription_id, location):
                    SS_Resources.succeed_to_get_next_available_location_count += 1
                    SS_Resources.succeed_to_get_next_available_location_count_all += 1
                    SS_Resources.locations_call_history_tmp[subscription_id][location].append(datetime.now(timezone.utc).replace(tzinfo=None).isoformat())
                    return subscription_id, location

        return None

    except Exception as e:
        print("\n[ERROR] Exception occurred in get_next_available_location:")
        print(traceback.format_exc())
        print(f"\n[ERROR] Failed to get_next_available_location: {e}")
        return None


def collect_available_locations():
    """
    이 메서드는 잘못된 location 파라미터로 Azure API를 호출해 API에서 리턴한 지원되는 locations 을 리턴합니다.
    """
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

def clean_expired_over_limit_locations():
    '''
    이 메서드는 구독 해당한 limited locations 대해, 1시간이 지났으면 유효하지 않은 항목을 제거합니다.
    '''
    if SS_Resources.locations_over_limit_tmp:
        for subscription_id in SS_Resources.subscriptions:
            one_hour_ago = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=63)
            for location_key, location_value in list(
                    SS_Resources.locations_over_limit_tmp[subscription_id].items()):
                dt = datetime.fromisoformat(location_value)
                if dt <= one_hour_ago:
                    del SS_Resources.locations_over_limit_tmp[subscription_id][location_key]


def clean_expired_over_call_history_locations():
    '''
    이 메서드는 호출 이력을 유효하지 않은 위치를 정리하고 호출 이력을 업데이트합니다.
    '''
    if SS_Resources.locations_call_history_tmp:
        one_hour_ago = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=63)
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
    """
    이 메서드는 특정 계정, 구독, 위치의 호출 이력을 업데이트합니다.
    업데이트된 데이터를 JSON 파일에 저장합니다.
    """
    try:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        current_timestamp = now.isoformat()
        SS_Resources.locations_call_history_tmp[subscription_id][location].append(current_timestamp)
        return True

    except Exception as e:
        print(f"Failed to update_call_history: {e}")
        return False

def update_over_limit_locations(subscription_id, location):
    """
    이 메서드는 초과 요청된 location을 업데이트합니다.
    초과 요청 위치 정보를 JSON 파일에 저장합니다.
    """
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