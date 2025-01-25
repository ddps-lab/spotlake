import re
import json
import subprocess
import sps_shared_resources
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

updated_available_locations = True

load_dotenv('./files_sps/.env')
LOCATIONS_CALL_HISTORY_FILENAME = os.getenv('LOCATIONS_CALL_HISTORY_FILENAME')
LOCATIONS_OVER_LIMIT_FILENAME = os.getenv('LOCATIONS_OVER_LIMIT_FILENAME')
AZ_CLI_PATH = os.getenv('AZ_CLI_PATH')

def load_over_limit_locations():
    '''
    이 메서드는 구독 해당한 초과 locations를 로드하고, 1시간이 지났으며 유효하지 않은 항목을 제거합니다.
    over_limit_locations의 초기화는 ACCOUNTS_CONFIG 에서 구독을 읽어 이용합니다.
    '''
    try:
        try:
            with open(LOCATIONS_OVER_LIMIT_FILENAME, 'r', encoding="utf-8") as file:
                all_over_limit_locations = json.load(file)

        except FileNotFoundError:
            print("load_over_limit_locations func. Too_many_req_locations file not found; initializing.")
            all_over_limit_locations = {}

        except json.JSONDecodeError as e:
            print(f"load_over_limit_locations func. Failed to parse JSON: {e}")
            all_over_limit_locations = {}

        except Exception as e:
            print(f"load_over_limit_locations func. An unexpected error occurred: {e}")
            all_over_limit_locations = {}

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
                        try:
                            dt = datetime.fromisoformat(location_value)
                            if dt <= one_hour_ago:
                                del all_over_limit_locations[account_id][subscription_id][location_key]
                                print(
                                    f"Deleted expired location: '{location_key}', subscription_id: '{subscription_id}'")

                        except ValueError:
                            print(f"Invalid isoformat string: '{location_value}', subscription_id: '{subscription_id}'")

        with open(LOCATIONS_OVER_LIMIT_FILENAME, 'w') as file:
            json.dump(all_over_limit_locations, file, indent=4)
        return all_over_limit_locations

    except Exception as e:
        print(f"Failed to load_over_limit_locations: {e}")
        return None


def load_call_history_locations():
    '''
    이 메서드는 호출 이력을 로드하고 필요에 따라 초기화합니다.
    또한, 유효하지 않은 위치를 정리하고 호출 이력 파일을 업데이트합니다.
    '''
    global updated_available_locations
    try:
        try:
            with open(LOCATIONS_CALL_HISTORY_FILENAME, 'r', encoding="utf-8") as file:
                all_subscriptions_history = json.load(file)

        except FileNotFoundError:
            print("load_call_history_locations func. file not found; initializing new history.")
            all_subscriptions_history = {}

        except json.JSONDecodeError as e:
            print(f"load_call_history_locations func. Failed to parse JSON: {e}. Reinitializing history.")
            all_subscriptions_history = {}

        except Exception as e:
            print(f"load_call_history_locations func. An unexpected error occurred: {e}")
            all_subscriptions_history = {}

        if updated_available_locations:

            available_locations = get_available_locations()
            if available_locations is None:
                return None
            updated_available_locations = False

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

        one_hour_ago = datetime.now() - timedelta(hours=1)
        for account_id, account_data in sps_shared_resources.ACCOUNTS_CONFIG.items():
            if "subscription_ids" in account_data:
                for subscription_id in account_data['subscription_ids']:
                    subscription_data = all_subscriptions_history.get(account_id, {}).get(subscription_id, {})

                    all_subscriptions_history[account_id][subscription_id] = {
                        location: [
                            t for t in timestamps if datetime.fromisoformat(t) > one_hour_ago
                        ]
                        for location, timestamps in subscription_data.items()
                    }

        with open(LOCATIONS_CALL_HISTORY_FILENAME, 'w') as file:
            json.dump(all_subscriptions_history, file, indent=4)

        return all_subscriptions_history

    except Exception as e:
        print(f"Failed to load_call_history: {e}")
        return None


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

        with open(LOCATIONS_CALL_HISTORY_FILENAME, 'w') as file:
            json.dump(all_subscriptions_history, file, indent=4)

        return True

    except Exception as e:
        print(f"Failed to update_call_history: {e}")
        return False


def validation_can_call(location, history, over_limit_locations):
    """
    이 메서드는 지정된 location으로 호출 가능한지 확인합니다.
    초과 요청 여부와 호출 이력의 크기를 기준으로 판단합니다.
    """
    try:
        if over_limit_locations is not None:
            if ((location not in over_limit_locations)
                    and (len(history[location]) < 10)):
                return True
        else:
            if len(history[location]) < 10:
                return True
        return False


    except Exception as e:
        print(f"Failed to valid can_call: {e}")
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

        with open(LOCATIONS_OVER_LIMIT_FILENAME, 'w') as file:
            json.dump(all_over_limit_locations, file, indent=4)
        return True

    except Exception as e:
        print(f"Failed to update_all_over_limit_locations: {e}")
        return False


def get_available_locations():
    """
    이 메서드는 사용 가능한 위치를 잘못된 파라미터로 Azure API를 호출해 리턴한 지원되는 위치 목록을 리턴합니다.
    """
    print("Start to get_available_locations")
    id_1 = sps_shared_resources.ACCOUNTS_CONFIG['account_1']['subscription_ids'][0]
    location = "ERROR_LOCATION"
    request_body = {
        "availabilityZones": False,
        "desiredCount": 1,
        "desiredLocations": ["korea"],
        "desiredSizes": [{"sku": "Standard_D2_v3"}],
    }

    command = [
        AZ_CLI_PATH, "rest",
        "--method", "post",
        "--uri",
        f"https://management.azure.com/subscriptions/{id_1}/providers/Microsoft.Compute/locations/{location}/diagnostics/spotPlacementRecommender/generate?api-version=2024-06-01-preview",
        "--headers", "Content-Type=application/json",
        "--body", json.dumps(request_body)
    ]

    try:
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True,
                                timeout=15)

    except subprocess.CalledProcessError as e:
        error_message = e.stderr
        available_locations_temp = re.search(r"supported locations are '([^']+)'", error_message)
        if available_locations_temp:
            available_locations = available_locations_temp.group(1).split(', ')
            print(f"Get Available locations successfully, locations: {available_locations}")
            return available_locations

    except Exception as e:
        print(f"Failed to get_available_locations, Error: " + e)
        return None
