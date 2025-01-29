import json
import re
import random
import requests
import sps_location_manager
import sps_shared_resources
import sps_get_regions_instance_types
import concurrent.futures
import sps_prepare_parameters
import pandas as pd
import time
import os
from dotenv import load_dotenv
from json import JSONDecodeError
from functools import wraps
from datetime import datetime
from utill.azure_auth import get_sps_token


def log_execution_time(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if getattr(wrapper, "_is_running", False):
            return func(*args, **kwargs)

        wrapper._is_running = True
        try:
            start_time = datetime.now()
            print(f"Start time for {func.__name__}: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")


            result = func(*args, **kwargs)

            end_time = datetime.now()
            elapsed_time = end_time - start_time
            minutes, seconds = divmod(elapsed_time.seconds, 60)

            print(f"End time for {func.__name__}: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{func.__name__} executed in {minutes}min {seconds}sec")

            return result
        finally:
            wrapper._is_running = False
    return wrapper


load_dotenv('./files_sps/.env')

INVALID_REGIONS_PATH_JSON = os.getenv('INVALID_REGIONS_PATH_JSON')
INVALID_INSTANCE_TYPES_PATH_JSON = os.getenv('INVALID_INSTANCE_TYPES_PATH_JSON')
REGIONS_AND_INSTANCE_TYPES_DF_FROM_PRICEAPI_FILENAME_PKL = os.getenv('REGIONS_AND_INSTANCE_TYPES_DF_FROM_PRICEAPI_FILENAME_PKL')
DF_TO_USE_TODAY_FILENAME_PKL = os.getenv('DF_TO_USE_TODAY_FILENAME_PKL')

@log_execution_time
def collect_spot_placement_score_first_time(desired_count, collect_time):
    if initialize_files():
        initialize_sps_shared_resources()

        print(f"Start to collect_spot_placement_score_first_time")

        start_time = time.time()
        regions_and_instance_types_df = sps_get_regions_instance_types.request_regions_and_instance_types_df_by_priceapi()
        regions_and_instance_types_df.to_pickle(REGIONS_AND_INSTANCE_TYPES_DF_FROM_PRICEAPI_FILENAME_PKL)
        print(f"The file '{REGIONS_AND_INSTANCE_TYPES_DF_FROM_PRICEAPI_FILENAME_PKL}' has been successfully saved.")

        df_greedy_clustering_initial = sps_prepare_parameters.greedy_clustering_to_create_optimized_request_list(regions_and_instance_types_df)
        end_time = time.time()
        elapsed = end_time - start_time
        minutes, seconds = divmod(int(elapsed), 60)
        print(f"request_regions_and_instance_types_df_by_priceapi + greedy_clustering_to_create_optimized_request_list time: {minutes}min {seconds}sec")

        sps_location_manager.check_and_add_available_locations()

        start_time = time.time()
        execute_spot_placement_score_task_by_parameter_pool_df(df_greedy_clustering_initial, True, desired_count, collect_time)
        print(f'Time_out_retry_count: {sps_shared_resources.time_out_retry_count}')
        print(f'Bad_request_retry_count: {sps_shared_resources.bad_request_retry_count}')
        print(f'Too_many_requests_count: {sps_shared_resources.too_many_requests_count}')
        print(f'Found_invalid_region_retry_count: {sps_shared_resources.found_invalid_region_retry_count}')
        print(f'Found_invalid_instance_type_retry_count: {sps_shared_resources.found_invalid_instance_type_retry_count}')

        end_time = time.time()
        elapsed = end_time - start_time
        minutes, seconds = divmod(int(elapsed), 60)
        print(f"execute_spot_placement_score_task_by_parameter_pool_df time: {minutes}min {seconds}sec")

        start_time = time.time()
        regions_and_instance_types_df = pd.read_pickle(REGIONS_AND_INSTANCE_TYPES_DF_FROM_PRICEAPI_FILENAME_PKL)
        regions_and_instance_types_filtered_df = sps_prepare_parameters.filter_invalid_parameter(
            regions_and_instance_types_df)
        df_greedy_clustering_filtered = sps_prepare_parameters.greedy_clustering_to_create_optimized_request_list(
            regions_and_instance_types_filtered_df)
        df_greedy_clustering_filtered.to_pickle(DF_TO_USE_TODAY_FILENAME_PKL)
        print(f"The file '{DF_TO_USE_TODAY_FILENAME_PKL}' has been successfully saved.")

        end_time = time.time()
        elapsed = end_time - start_time
        minutes, seconds = divmod(int(elapsed), 60)
        print(f"Prepare the request pool. time: {minutes}min {seconds}sec")


@log_execution_time
def collect_spot_placement_score(desired_count, collect_time):
    initialize_sps_shared_resources()

    print(f"Start to collect_spot_placement_score")
    df_greedy_clustering_filtered = pd.read_pickle(DF_TO_USE_TODAY_FILENAME_PKL)

    execute_spot_placement_score_task_by_parameter_pool_df(df_greedy_clustering_filtered, True, desired_count, collect_time)
    print(f'Time_out_retry_count: {sps_shared_resources.time_out_retry_count}')
    print(f'Bad_request_retry_count: {sps_shared_resources.bad_request_retry_count}')
    print(f'Too_many_requests_count: {sps_shared_resources.too_many_requests_count}')
    print(f'Found_invalid_region_retry_count: {sps_shared_resources.found_invalid_region_retry_count}')
    print(f'Found_invalid_instance_type_retry_count: {sps_shared_resources.found_invalid_instance_type_retry_count}')


def execute_spot_placement_score_task_by_parameter_pool_df(api_calls_df, availability_zones, desired_count, collect_time):
    merged_result = {
        "Collect_Time": collect_time,
        "Desired_Count": desired_count,
        "Availability_Zones": availability_zones,
        "Placement_Scores": []
    }

    all_subscriptions_history = sps_location_manager.load_call_history_locations()
    locations = list(all_subscriptions_history[list(all_subscriptions_history.keys())[0]].keys())

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(locations) * 3) as executor:
        futures = []

        for index, row in api_calls_df.iterrows():
            future = executor.submit(
                execute_spot_placement_score_api,
                row['Regions'], row['InstanceTypes'], availability_zones, desired_count, max_retries=50
            )
            futures.append((future, desired_count))


        for future, desired_count in futures:
            try:
                result = future.result()
                if result and result != "NO_AVAILABLE_LOCATIONS":
                    for score in result["placementScores"]:
                        if "sku" in score:
                            score["Instance_Type"] = score.pop("sku")
                        if "score" in score:
                            score["Score"] = score.pop("score")
                        if "isQuotaAvailable" in score:
                            del score["isQuotaAvailable"]
                        if "region" in score:
                            score["Region_Code"] = score.pop("region")
                        # availabilityZone 은 일부 결과에 필드가 아예 없는 상황이 있어, 우선 마지막에 둡니다.
                        if "availabilityZone" in score:
                            score["Availability_Zone"] = score.pop("availabilityZone")

                    merged_result["Placement_Scores"].extend(result["placementScores"])

                elif result == "NO_AVAILABLE_LOCATIONS":
                    for f, _ in futures:
                        if not f.done():
                            f.cancel()

            except JSONDecodeError as e:
                print(f"execute_spot_placement_score_task_by_parameter_pool_df func. JSON decoding error: {str(e)}")

            except Exception as e:
                print(f"{result}")
                print(f"execute_spot_placement_score_task_by_parameter_pool_df func. An unexpected error occurred: {e}")

    # json 파일이 아닌 dataframe 형태 변경 예정
    with open(f"./files_sps/{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.json", "w") as json_file:
        json.dump(merged_result, json_file, indent=3)

    print(f"병합된 결과가 './files_sps/{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.json' 파일에 저장되었습니다.")
    return True


def execute_spot_placement_score_api(region_chunk, instance_type_chunk, availability_zones, desired_count, max_retries=10):
    region_chunk = filter_invalid_items(region_chunk, sps_get_regions_instance_types.load_invalid_regions(),
                                        "invalid_regions")
    instance_type_chunk = filter_invalid_items(instance_type_chunk,
                                               sps_get_regions_instance_types.load_invalid_instance_types(),
                                               "invalid_instance_types")
    if not region_chunk or not instance_type_chunk:
        print(f"execute_spot_placement_score_api: This execute will not execute because, after filtering, the chunk becomes empty. region_chunk: {region_chunk}, instance_type_chunk: {instance_type_chunk}")
        return None

    request_body = {
        "availabilityZones": availability_zones,
        "desiredCount": desired_count,
    }

    retries = 0
    while retries <= max_retries:
        request_body['desiredLocations'] = region_chunk
        request_body['desiredSizes'] = [{"sku": instance_type} for instance_type in instance_type_chunk]


        with sps_shared_resources.get_next_available_location_lock:
            res = sps_location_manager.get_next_available_location()
            account_id, subscription_id, location, history, all_subscriptions_history, over_limit_locations, all_over_limit_locations = res
            sps_location_manager.update_call_history(account_id, subscription_id, location, history,
                                                     all_subscriptions_history)

        if res is None:
            print("No available locations with remaining calls.")
            return "NO_AVAILABLE_LOCATIONS"

        url = f"https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.Compute/locations/{location}/diagnostics/spotPlacementRecommender/generate?api-version=2024-06-01-preview"
        headers = {
            "Authorization": f"Bearer {get_sps_token()}",
            "Content-Type": "application/json",
        }

        try:
            response = requests.post(url, headers=headers, json=request_body, timeout=15)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.Timeout:
            retries = handle_retry("Timeout", retries, max_retries)
            continue

        except requests.exceptions.HTTPError as http_err:
            error_message = http_err.response.text
            print(f"HTTP error occurred: {error_message}")

            match_res = extract_invalid_values(error_message)
            if match_res:
                if match_res["region"]:
                    region_chunk = del_invalid_chunk(region_chunk, match_res["region"], "region")
                    if not region_chunk:
                        print(f"This retry will not execute because, after filtering, the region_chunk becomes empty.")
                        break
                    retries = handle_retry("InvalidRegion", retries, max_retries)

                if match_res["instanceType"]:
                    instance_type_chunk = del_invalid_chunk(instance_type_chunk, match_res["instanceType"],"instanceType")
                    if not instance_type_chunk:
                        print(f"This retry will not execute because, after filtering, the instance_type_chunk becomes empty.")
                        break
                    retries = handle_retry("InvalidInstanceType", retries, max_retries)
                continue

            if "BadGatewayConnection" in error_message:
                retries = handle_retry("BadGatewayConnection", retries, max_retries)
                continue

            if "You have reached the maximum number of requests allowed." in error_message:
                with sps_shared_resources.get_next_available_location_lock:
                    sps_location_manager.update_over_limit_locations(account_id, subscription_id, location, all_over_limit_locations)
                retries = handle_retry("Too Many Requests", retries, max_retries)
                continue

        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            break

    print(f"Max retries-> ({max_retries}) reached for regions: {region_chunk}, instance types: {instance_type_chunk}.")
    return None



def extract_invalid_values(error_message):
    region_match = re.search(
        r"The value '([a-zA-Z0-9-_]+)' provided for the input parameter 'desiredLocations' is not valid",
        error_message
    )

    instance_type_match = re.search(
        r"The value '([a-zA-Z0-9-_]+)' provided for the input parameter 'SpotPlacementRecommenderInput.desiredSizes' is not valid",
        error_message
    )

    if not region_match and not instance_type_match:
        return None

    match_res = {
        "region": region_match.group(1) if region_match else None,
        "instanceType": instance_type_match.group(1) if instance_type_match else None
    }

    return match_res

def initialize_files():
    try:
        empty_data = {}
        with open(INVALID_REGIONS_PATH_JSON, 'w') as file:
            json.dump(empty_data, file, indent=4)
            print(f"{INVALID_REGIONS_PATH_JSON} has been initialized.")

        with open(INVALID_INSTANCE_TYPES_PATH_JSON, 'w') as file:
            json.dump(empty_data, file, indent=4)
            print(f"{INVALID_INSTANCE_TYPES_PATH_JSON} has been initialized.")
        return True

    except Exception as e:
        print(f"An error occurred during initialization: {e}")
        return False

def del_invalid_chunk(chunk, invalid_value, value_type):
    with sps_shared_resources.lock:
        if value_type == "region":
            invalid_regions = sps_get_regions_instance_types.load_invalid_regions()
            if invalid_regions is not None and "invalid_regions" in invalid_regions:
                if invalid_value not in invalid_regions["invalid_regions"]:
                    sps_get_regions_instance_types.update_invalid_regions(invalid_value, invalid_regions[
                        "invalid_regions"])
            else:
                sps_get_regions_instance_types.update_invalid_regions(invalid_value, invalid_regions)

        elif value_type == "instanceType":
            invalid_instance_types = sps_get_regions_instance_types.load_invalid_instance_types()
            if invalid_instance_types is not None and "invalid_instance_types" in invalid_instance_types:
                if invalid_value not in invalid_instance_types["invalid_instance_types"]:
                    sps_get_regions_instance_types.update_invalid_instance_types(invalid_value,
                                                                                 invalid_instance_types[
                                                                                     "invalid_instance_types"])
            else:
                sps_get_regions_instance_types.update_invalid_instance_types(invalid_value, invalid_instance_types)
    if invalid_value in chunk:
        chunk.remove(invalid_value)
    else:
        print(f"x not in list, invalid_value: {invalid_value}, chunk: {chunk}")

    return chunk if chunk else None




def handle_retry(error_type, retries, max_retries):
    if error_type == "Timeout":
        sps_shared_resources.time_out_retry_count += 1
    elif error_type == "BadGatewayConnection":
        sps_shared_resources.bad_request_retry_count += 1
    elif error_type == "Too Many Requests":
        sps_shared_resources.too_many_requests_count += 1
    elif error_type == "InvalidRegion":
        sps_shared_resources.found_invalid_region_retry_count += 1
    elif error_type == "InvalidInstanceType":
        sps_shared_resources.found_invalid_instance_type_retry_count += 1

    if retries < max_retries:
        sleep_time = round(random.uniform(0.5, 1.5), 1)
        # print(f"Retrying ({retries + 1}/{max_retries})... {error_type}. Sleep: {sleep_time}s")
        time.sleep(sleep_time)
        retries += 1

    return retries

def filter_invalid_items(items, invalid_data, key):
    if isinstance(invalid_data, dict) and key in invalid_data:
        return [item for item in items if item not in invalid_data[key]]
    return items

def initialize_sps_shared_resources():
    sps_shared_resources.bad_request_retry_count = 0
    sps_shared_resources.time_out_retry_count = 0
    sps_shared_resources.too_many_requests_count = 0
    sps_shared_resources.found_invalid_region_retry_count = 0
    sps_shared_resources.found_invalid_instance_type_retry_count = 0
    sps_shared_resources.last_login_account = None