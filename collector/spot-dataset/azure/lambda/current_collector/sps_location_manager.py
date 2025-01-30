import re
import requests
import sps_shared_resources
import os
import json
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv('./files_sps/.env')
LOCATIONS_CALL_HISTORY_FILENAME = os.getenv('LOCATIONS_CALL_HISTORY_FILENAME')
LOCATIONS_OVER_LIMIT_FILENAME = os.getenv('LOCATIONS_OVER_LIMIT_FILENAME')
SUBSCRIPTIONS = os.getenv('SUBSCRIPTIONS').split(",")

SS_Resources = sps_shared_resources

def check_and_add_available_locations():
    '''
    이 함수는 최신 사용 가능한 location을 수집하고, JSON 파일에 저장된 기존 구독 기록과 비교합니다.
    새로운 위치가 발견되면 해당 구독 기록에 추가됩니다. 변경 사항이 있는 경우, 업데이트된 데이터를 JSON 파일에 저장합니다.
    '''
    added_available_locations_flag = False
    available_locations = collect_available_locations()
    if available_locations is None:
        print("Failed to check_and_add_available_locations. No available locations collected.")
        return False

    all_subscriptions_history = sps_shared_resources.read_json_file(LOCATIONS_CALL_HISTORY_FILENAME) or {}

    for subscription_id in SUBSCRIPTIONS:
        if subscription_id not in all_subscriptions_history:
            all_subscriptions_history[subscription_id] = {}

        subscription_history = all_subscriptions_history[subscription_id]

        for location in available_locations:
            if location not in subscription_history:
                subscription_history[location] = []
                added_available_locations_flag = True

    if added_available_locations_flag:
        if not sps_shared_resources.write_json_file(LOCATIONS_CALL_HISTORY_FILENAME, all_subscriptions_history):
            print("Failed to save available_locations to all_subscriptions_history file.")
            return False

        print("Successfully saved available_locations to all_subscriptions_history file.")
        return True

    print("available_locations has not been added in this cycle.")
    return False


def validation_can_call(location, history, over_limit_locations):
    """
    이 메서드는 지정된 location으로 호출 가능한지 확인합니다.
    초과 요청 여부와 호출 이력의 크기를 기준으로 판단합니다.
    """
    if over_limit_locations is not None:
        if ((location not in over_limit_locations)
                and (len(history[location]) < 10)):
            return True
    else:
        if len(history[location]) < 10:
            return True
    return False


def get_next_available_location():
    """
    이 메서드는 사용 가능한 다음 위치를 리턴합니다.
    호출 이력과 초과 요청 데이터를 기반으로 적절한 위치를 선택합니다.
    호출 시 이용하는 location은 구독내에 지난 호출의 location을 이용 안 해야 하는 로직이 들어갑니다.

    이유:
    단 기간에 같은 location으로 호출 시, timeout율이 놉습니다.
    """
    try:

        if SS_Resources.locations_call_history_tmp is None or SS_Resources.locations_over_limit_tmp is None:
            return None
        else:
            clean_expired_over_limit_locations()
            clean_expired_over_call_history_locations()

            for subscription_id, subscription_data in SS_Resources.locations_call_history_tmp.items():
                current_history = subscription_data
                current_over_limit_locations = SS_Resources.locations_over_limit_tmp.get(subscription_id) or None

                if subscription_id not in sps_shared_resources.last_location_index:
                    sps_shared_resources.last_location_index[subscription_id] = 0

                start_index = sps_shared_resources.last_location_index[subscription_id]
                num_locations = len(current_history)

                for i in range(num_locations):
                    index = (start_index + i) % num_locations
                    location = list(current_history.keys())[index]

                    if validation_can_call(location, current_history, current_over_limit_locations):
                        sps_shared_resources.last_location_index[subscription_id] = (index + 1) % num_locations
                        return subscription_id, location, current_history, current_over_limit_locations
            return None

    except Exception as e:
        print(f"Failed to get_next_available_location: {e}")
        return None

def collect_available_locations():
    """
    이 메서드는 잘못된 location 파라미터로 Azure API를 호출해 API에서 리턴한 지원되는 locations 을 리턴합니다.
    """
    print("Start to collect_available_locations")
    subscription_id = SUBSCRIPTIONS[0]
    location = "ERROR_LOCATION"

    try:
        url = f"https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.Compute/locations/{location}/diagnostics/spotPlacementRecommender/generate?api-version=2024-06-01-preview"
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
            print(f"Collect_available_locations successfully, locations: {available_locations}")
            return available_locations

    except Exception as e:
        print(f"Failed to collect_available_locations, Error: {e}")
        return None

def load_call_history_locations_file():
    # S3 이용으로 변경 예정
    """
    이 메서드는 call_history 데이터(JSON 파일)를 로드합니다.
    데이터가 비어 있거나 유효하지 않을 경우 None을 반환합니다.
    """
    try:
        with open(LOCATIONS_CALL_HISTORY_FILENAME, "r", encoding="utf-8") as json_file:
            content = json_file.read().strip()
            if not content:
                return None

            parsed_content = json.loads(content)

            if not parsed_content:
                return None

            return parsed_content

    except json.JSONDecodeError as e:
        print(f"load_call_history_locations_file func. JSON decoding error: {str(e)}")
    except Exception as e:
        print(f"load_call_history_locations_file func. An unexpected error occurred: {e}")
    return None


def load_over_limit_locations_file():
    # S3 이용으로 변경 예정
    """
    이 메서드는 over_limit_locations 데이터(JSON 파일)를 로드합니다.
    데이터가 비어 있거나 유효하지 않을 경우 None을 반환합니다.
    """
    try:
        with open(LOCATIONS_OVER_LIMIT_FILENAME, "r", encoding="utf-8") as json_file:
            content = json_file.read().strip()
            if not content:
                return None

            parsed_content = json.loads(content)
            if not parsed_content:
                for subscription_id in SUBSCRIPTIONS:
                    if subscription_id not in parsed_content:
                        parsed_content[subscription_id] = {}
            return parsed_content

    except json.JSONDecodeError as e:
        print(f"load_over_limit_locations_file func. JSON decoding error: {str(e)}")
    except Exception as e:
        print(f"load_over_limit_locations_file func. An unexpected error occurred: {e}")
    return None

def clean_expired_over_limit_locations():
    '''
    이 메서드는 구독 해당한 limited locations 대해, 1시간이 지났으면 유효하지 않은 항목을 제거합니다.
    '''
    for subscription_id in SUBSCRIPTIONS:
        one_hour_ago = datetime.now() - timedelta(hours=1)
        for location_key, location_value in list(
                SS_Resources.locations_over_limit_tmp[subscription_id].items()):
            dt = datetime.fromisoformat(location_value)
            if dt <= one_hour_ago:
                del SS_Resources.locations_over_limit_tmp[subscription_id][location_key]


def clean_expired_over_call_history_locations():
    '''
    이 메서드는 호출 이력을 유효하지 않은 위치를 정리하고 호출 이력을 업데이트합니다.
    '''
    one_hour_ago = datetime.now() - timedelta(hours=1)
    for subscription_id in SUBSCRIPTIONS:
        subscription_data = SS_Resources.locations_call_history_tmp.get(subscription_id, {})

        new_subscription_data = {
            location: [
                t for t in timestamps if datetime.fromisoformat(t) > one_hour_ago
            ]
            for location, timestamps in subscription_data.items()
        }
        SS_Resources.locations_call_history_tmp[subscription_id] = new_subscription_data


def update_call_history(subscription_id, location, current_history):
    """
    이 메서드는 특정 계정, 구독, 위치의 호출 이력을 업데이트합니다.
    업데이트된 데이터를 JSON 파일에 저장합니다.
    """
    try:
        now = datetime.now()
        current_timestamp = now.isoformat()
        current_history[location].append(current_timestamp)
        SS_Resources.locations_call_history_tmp[subscription_id] = current_history
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
        now = datetime.now()
        current_timestamp = now.isoformat()
        SS_Resources.locations_over_limit_tmp[subscription_id][location] = current_timestamp

        print("Succeed to update_over_limit_locations. Subscription ID:" + subscription_id.split('-')[
            0] + ", Location:", location + ", Time:", now.strftime('%Y-%m-%d %H:%M:%S'))
        return True

    except Exception as e:
        print(f"Failed to update_all_over_limit_locations: {e}")
        return False

def save_call_history_file():
    # S3 이용으로 변경 예정
    try:
        if SS_Resources.locations_call_history_tmp:
            with open(LOCATIONS_CALL_HISTORY_FILENAME, 'w', encoding='utf-8') as file:
                json.dump(SS_Resources.locations_call_history_tmp, file, indent=4)
            print(f"Succeed to save locations_call_history_tmp to {LOCATIONS_CALL_HISTORY_FILENAME}.")
            return True

    except Exception as e:
        print(f"Failed to save locations_call_history_tmp: {e}")

    return False

def save_over_limit_locations_file():
    # S3 이용으로 변경 예정
    try:
        if SS_Resources.locations_over_limit_tmp:
            with open(LOCATIONS_OVER_LIMIT_FILENAME, 'w', encoding='utf-8') as file:
                json.dump(SS_Resources.locations_over_limit_tmp, file, indent=4)
            print(f"Succeed to save locations_over_limit_tmp to {LOCATIONS_OVER_LIMIT_FILENAME}.")
            return True

    except Exception as e:
        print(f"Failed to save locations_over_limit_tmp: {e}")

    return False