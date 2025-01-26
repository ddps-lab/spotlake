import re
import requests
import sps_shared_resources
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from utill.azure_auth import sps_get_token

load_dotenv('./files_sps/.env')
LOCATIONS_CALL_HISTORY_FILENAME = os.getenv('LOCATIONS_CALL_HISTORY_FILENAME')
LOCATIONS_OVER_LIMIT_FILENAME = os.getenv('LOCATIONS_OVER_LIMIT_FILENAME')
SPS_TOKEN_FILENAME_JSON = os.getenv('SPS_TOKEN_FILENAME_JSON')

AZ_CLI_PATH = os.getenv('AZ_CLI_PATH')

def load_over_limit_locations():
    '''
    이 메서드는 구독 해당한 초과 locations를 로드하고, 1시간이 지났으며 유효하지 않은 항목을 제거합니다.
    over_limit_locations의 초기화는 ACCOUNTS_CONFIG 에서 구독을 읽어 이용합니다.
    '''
    updated_over_limit_locations_flag = False
    all_over_limit_locations = sps_shared_resources.read_json_file(LOCATIONS_OVER_LIMIT_FILENAME)

    if all_over_limit_locations is None:
        print("Failed to load over limit locations. The file might be missing or corrupted.")
        return None

    for account_id, account_data in sps_shared_resources.ACCOUNTS_CONFIG.items():
        if account_id not in all_over_limit_locations:
            all_over_limit_locations[account_id] = {}
        if "subscription_ids" in account_data:
            for subscription_id in account_data['subscription_ids']:
                if subscription_id not in all_over_limit_locations[account_id]:
                    all_over_limit_locations[account_id][subscription_id] = {}

                one_hour_ago = datetime.now() - timedelta(hours=1)
                for location_key, location_value in list(
                        all_over_limit_locations[account_id][subscription_id].items()):
                    dt = datetime.fromisoformat(location_value)
                    if dt <= one_hour_ago:
                        updated_over_limit_locations_flag = True
                        del all_over_limit_locations[account_id][subscription_id][location_key]
                        print(f"Deleted expired location: '{location_key}', subscription_id: '{subscription_id}'")

    if updated_over_limit_locations_flag:
        if not sps_shared_resources.write_json_file(LOCATIONS_OVER_LIMIT_FILENAME, all_over_limit_locations):
            print("Failed to save over_limit_location.")
            return None
    return all_over_limit_locations


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

    for account_id, account_data in sps_shared_resources.ACCOUNTS_CONFIG.items():
        if account_id not in all_subscriptions_history:
            all_subscriptions_history[account_id] = {}
        if "subscription_ids" in account_data:
            for subscription_id in account_data['subscription_ids']:
                if subscription_id not in all_subscriptions_history[account_id]:
                    all_subscriptions_history[account_id][subscription_id] = {}

                subscription_history = all_subscriptions_history[account_id][subscription_id]

                for location in available_locations:
                    if location not in subscription_history:
                        print(f"Location: {location} is fresh for subscription {subscription_id}, will add it.")
                        subscription_history[location] = []
                        added_available_locations_flag = True

    if added_available_locations_flag:
        if not sps_shared_resources.write_json_file(LOCATIONS_CALL_HISTORY_FILENAME, all_subscriptions_history):
            print("Failed to save available_locations to all_subscriptions_history file.")
            return False
        print("Successfully saved available_locations to all_subscriptions_history file.")
        return True

    print("available_locations has not been added.")
    return False


def load_call_history_locations():
    '''
    이 메서드는 호출 이력을 로드하고 필요에 따라 초기화합니다.
    또한, 유효하지 않은 위치를 정리하고 호출 이력 파일을 업데이트합니다.
    '''
    updated_history_flag = False
    all_subscriptions_history = sps_shared_resources.read_json_file(LOCATIONS_CALL_HISTORY_FILENAME) or None

    if all_subscriptions_history is None:
        print("Failed to load call history. The file might be missing or corrupted.")
        return None

    one_hour_ago = datetime.now() - timedelta(hours=1)
    for account_id, account_data in sps_shared_resources.ACCOUNTS_CONFIG.items():
        if "subscription_ids" in account_data:
            for subscription_id in account_data['subscription_ids']:
                subscription_data = all_subscriptions_history.get(account_id, {}).get(subscription_id, {})

                new_subscription_data = {
                    location: [
                        t for t in timestamps if datetime.fromisoformat(t) > one_hour_ago
                    ]
                    for location, timestamps in subscription_data.items()
                }

                if new_subscription_data != all_subscriptions_history.get(account_id, {}).get(subscription_id, {}):
                    updated_history_flag = True

                all_subscriptions_history.setdefault(account_id, {})[subscription_id] = new_subscription_data

    if updated_history_flag:
        if not sps_shared_resources.write_json_file(LOCATIONS_CALL_HISTORY_FILENAME, all_subscriptions_history):
            print("Failed to save the call history file with deleted expired values.")
            return None

    return all_subscriptions_history


def update_call_history(account_id, subscription_id, location, current_history, all_subscriptions_history):
    """
    이 메서드는 특정 계정, 구독, 위치의 호출 이력을 업데이트합니다.
    업데이트된 데이터를 JSON 파일에 저장합니다.
    """
    try:
        now = datetime.now()
        current_timestamp = now.isoformat()
        current_history[location].append(current_timestamp)
        all_subscriptions_history[account_id][subscription_id] = current_history

        sps_shared_resources.write_json_file(LOCATIONS_CALL_HISTORY_FILENAME, all_subscriptions_history)
        return True

    except Exception as e:
        print(f"Failed to update_call_history: {e}")
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
        all_subscriptions_history = load_call_history_locations()
        all_over_limit_locations = load_over_limit_locations()

        if all_subscriptions_history is None or all_over_limit_locations is None:
            return None

        for account_id, account_history_data in all_subscriptions_history.items():
            if sps_shared_resources.last_login_account != account_id:
                if not sps_shared_resources.login_to_account(account_id):
                    return False
                sps_shared_resources.last_login_account = account_id

            for subscription_id, subscription_data in account_history_data.items():
                current_history = subscription_data
                current_over_limit_locations = all_over_limit_locations.get(account_id, {}).get(subscription_id, {})

                if not current_over_limit_locations:
                    current_over_limit_locations = None

                if subscription_id not in sps_shared_resources.last_location_index:
                    sps_shared_resources.last_location_index[subscription_id] = 0

                start_index = sps_shared_resources.last_location_index[subscription_id]
                num_locations = len(current_history)

                for i in range(num_locations):
                    index = (start_index + i) % num_locations
                    location = list(current_history.keys())[index]

                    if validation_can_call(location, current_history, current_over_limit_locations):
                        sps_shared_resources.last_location_index[subscription_id] = (index + 1) % num_locations
                        return account_id, subscription_id, location, current_history, all_subscriptions_history, current_over_limit_locations, all_over_limit_locations

        print("No next_available_location.")
        return None

    except Exception as e:
        print(f"Failed to get_next_available_location: {e}")
        return None


def update_over_limit_locations(account_id, subscription_id, location, all_over_limit_locations):
    """
    이 메서드는 초과 요청된 location을 업데이트합니다.
    초과 요청 위치 정보를 JSON 파일에 저장합니다.
    """
    try:
        now = datetime.now()
        current_timestamp = now.isoformat()
        all_over_limit_locations[account_id][subscription_id][location] = current_timestamp

        print("Save over_limit_locations. Account: " + account_id + ", Subscription ID:" + subscription_id.split('-')[
            0] + ", Location:", location + ", Time:", now.strftime('%Y-%m-%d %H:%M:%S'))

        sps_shared_resources.write_json_file(LOCATIONS_OVER_LIMIT_FILENAME, all_over_limit_locations)
        return True

    except Exception as e:
        print(f"Failed to update_all_over_limit_locations: {e}")
        return False

def collect_available_locations():
    """
    이 메서드는 잘못된 location 파라미터로 Azure API를 호출해 API에서 리턴한 지원되는 locations 을 리턴합니다.
    """
    print("Start to collect_available_locations")

    subscription_id = sps_shared_resources.ACCOUNTS_CONFIG['account_1']['subscription_ids'][0]
    location = "ERROR_LOCATION"
    sps_token = sps_shared_resources.read_json_file(SPS_TOKEN_FILENAME_JSON)['sps_token']
    try:
        url = f"https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.Compute/locations/{location}/diagnostics/spotPlacementRecommender/generate?api-version=2024-06-01-preview"
        headers = {
            "Authorization": f"Bearer {sps_token}",
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
        available_locations_temp = re.search(r"supported locations are '([^']+)'", http_err.response.text)
        if available_locations_temp:
            available_locations = available_locations_temp.group(1).split(', ')
            print(f"Get Available locations successfully, locations: {available_locations}")
            return available_locations

    except Exception as e:
        print(f"Failed to collect_available_locations, Error: {e}")
        return None