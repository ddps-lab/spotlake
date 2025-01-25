import subprocess
import json
import re
import random
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

AZ_CLI_PATH = os.getenv('AZ_CLI_PATH')
INVALID_REGIONS_PATH_JSON = os.getenv('INVALID_REGIONS_PATH_JSON')
INVALID_INSTANCE_TYPES_PATH_JSON = os.getenv('INVALID_INSTANCE_TYPES_PATH_JSON')
REGIONS_AND_INSTANCE_TYPES_DF_FROM_PRICEAPI_FILENAME_PKL = os.getenv('REGIONS_AND_INSTANCE_TYPES_DF_FROM_PRICEAPI_FILENAME_PKL')
DF_TO_USE_TODAY_FILENAME_PKL = os.getenv('DF_TO_USE_TODAY_FILENAME_PKL')

@log_execution_time
def collect_spot_placement_score_first_time(desired_count, request_time):
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


        start_time = time.time()
        execute_spot_placement_score_task_by_parameter_pool_df(df_greedy_clustering_initial, True, desired_count, request_time)
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
def collect_spot_placement_score(desired_count, request_time):
    initialize_sps_shared_resources()

    print(f"Start to collect_spot_placement_score")
    df_greedy_clustering_filtered = pd.read_pickle(DF_TO_USE_TODAY_FILENAME_PKL)

    execute_spot_placement_score_task_by_parameter_pool_df(df_greedy_clustering_filtered, True, desired_count, request_time)
    print(f'Time_out_retry_count: {sps_shared_resources.time_out_retry_count}')
    print(f'Bad_request_retry_count: {sps_shared_resources.bad_request_retry_count}')
    print(f'Too_many_requests_count: {sps_shared_resources.too_many_requests_count}')
    print(f'Found_invalid_region_retry_count: {sps_shared_resources.found_invalid_region_retry_count}')
    print(f'Found_invalid_instance_type_retry_count: {sps_shared_resources.found_invalid_instance_type_retry_count}')


def execute_spot_placement_score_task_by_parameter_pool_df(api_calls_df, availability_zones, desired_count, request_time):
    merged_result = {
        "AvailabilityZones": availability_zones,
        "Regions": set(),
        "Instance_Types": set(),
        "PlacementScores": []
    }

    all_subscriptions_history = sps_location_manager.load_call_history_locations()
    locations = list(all_subscriptions_history[list(all_subscriptions_history.keys())[0]].keys())

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(locations) * 3) as executor:
        futures = []

        for index, row in api_calls_df.iterrows():
            future = executor.submit(
                execute_spot_placement_score_api,
                row['Regions'], row['InstanceTypes'], availability_zones, desired_count, max_retries_for_timeout=8
            )
            futures.append((future, desired_count))


        for future, desired_count in futures:
            try:
                result = future.result()
                if result and result != "No_available_locations":
                    merged_result["Regions"].update(result["desiredLocations"])
                    for size in result["desiredSizes"]:
                        merged_result["Instance_Types"].add(size["sku"])

                    for score in result["placementScores"]:
                        if "isQuotaAvailable" in score:
                            del score["isQuotaAvailable"]
                        score["DesiredCount"] = desired_count
                        if "sku" in score:
                            score["InstanceType"] = score.pop("sku")
                        if "region" in score:
                            score["RegionCode"] = score.pop("region")
                        if "region" in score:
                            score["Score"] = score.pop("score")

                    merged_result["PlacementScores"].extend(result["placementScores"])

                elif result == "No_available_locations":
                    for f, _ in futures:
                        if not f.done():
                            f.cancel()

            except JSONDecodeError as e:
                print(f"execute_spot_placement_score_task_by_parameter_pool_df func. JSON decoding error: {str(e)}")

            except Exception as e:
                print(f"execute_spot_placement_score_task_by_parameter_pool_df func. An unexpected error occurred: {e}")

        merged_result["Regions"] = list(merged_result["RegionCode"])
        merged_result["Instance_Types"] = list(merged_result["InstanceType"])
        merged_result["Collect_Time"] = request_time


    with open(f"./files_sps/{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.json", "w") as json_file:
        json.dump(merged_result, json_file, indent=3)

    print(f"병합된 결과가 './files_sps/{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}.json' 파일에 저장되었습니다.")
    return True


def execute_spot_placement_score_api(region_chunk, instance_type_chunk, availability_zones,
                                                              desired_count, max_retries_for_timeout=4):
    invalid_regions = sps_get_regions_instance_types.load_invalid_regions()
    if isinstance(invalid_regions, dict) and "invalid_regions" in invalid_regions:
        region_chunk = [
            region for region in region_chunk
            if region not in invalid_regions["invalid_regions"]
        ]

    invalid_instance_types = sps_get_regions_instance_types.load_invalid_instance_types()
    if isinstance(invalid_instance_types, dict) and "invalid_instance_types" in invalid_instance_types:
        instance_type_chunk = [
            instance_type for instance_type in instance_type_chunk
            if instance_type not in invalid_instance_types["invalid_instance_types"]
        ]

    request_body = {
        "availabilityZones": availability_zones,
        "desiredCount": desired_count,
        "desiredLocations": [region for region in region_chunk],
        "desiredSizes": [{"sku": instance_type} for instance_type in instance_type_chunk],
    }

    retries = 0
    location = None
    response = None
    while retries <= max_retries_for_timeout:
        with sps_shared_resources.lock:
            res = sps_location_manager.get_next_available_location()
            if res is not None:
                account_id, subscription_id, location, history, all_subscriptions_history, over_limit_locations, all_over_limit_locations = res
                sps_location_manager.update_call_history(account_id, subscription_id, location, history,
                                                         all_subscriptions_history)
            else:
                print("No available locations with remaining calls.")
                return "No_available_locations"

        if location is None:
            print("No available locations with remaining calls.")
            return "No_available_locations"

        else:
            command = [
                AZ_CLI_PATH, "rest",
                "--method", "post",
                "--uri",
                f"https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.Compute/locations/{location}/diagnostics/spotPlacementRecommender/generate?api-version=2024-06-01-preview",
                "--headers", "Content-Type=application/json",
                "--body", json.dumps(request_body)
            ]

            try:
                result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True,
                                        timeout=15)
                response = json.loads(result.stdout)

            except subprocess.TimeoutExpired:
                retries += 1
                if retries >= max_retries_for_timeout:
                    print(f"지역 {region_chunk}에 대한 최대 재시도 횟수에 도달하여 요청을 건너뜁니다.")
                    break
                sleep_time = round(random.uniform(0.2, 1.5), 1)
                time.sleep(sleep_time)
                sps_shared_resources.time_out_retry_count += 1
                continue

            except subprocess.CalledProcessError as e:
                error_message = e.stderr
                match_res = extract_invalid_values(error_message)
                region_match = match_res["region"]
                instance_type_match = match_res["instanceType"]
                with sps_shared_resources.lock:
                    if region_match is not None:
                        invalid_regions = sps_get_regions_instance_types.load_invalid_regions()
                        if invalid_regions is not None and "invalid_regions" in invalid_regions:
                            if region_match not in invalid_regions["invalid_regions"]:
                                sps_get_regions_instance_types.update_invalid_regions(region_match, invalid_regions[
                                    "invalid_regions"])
                        else:
                            sps_get_regions_instance_types.update_invalid_regions(region_match, invalid_regions)
                        if len(region_chunk) == 1 and region_match in region_chunk:
                            print(
                                f"This retry will not execute because, after filtering, the region_chunk becomes empty. regions: {region_chunk}, instance types: {instance_type_chunk}.")
                            break
                        elif len(region_chunk) > 1 and region_match in region_chunk:
                            region_chunk.remove(region_match)
                            sps_shared_resources.found_invalid_region_retry_count += 1
                            continue


                with sps_shared_resources.lock:
                    if instance_type_match is not None:
                        invalid_instance_types = sps_get_regions_instance_types.load_invalid_instance_types()
                        if invalid_instance_types is not None and "invalid_instance_types" in invalid_instance_types:
                            if instance_type_match not in invalid_instance_types["invalid_instance_types"]:
                                sps_get_regions_instance_types.update_invalid_instance_types(instance_type_match,
                                                                                             invalid_instance_types[
                                                                                                 "invalid_instance_types"])
                        else:
                            sps_get_regions_instance_types.update_invalid_instance_types(instance_type_match,
                                                                                         invalid_instance_types)

                        if len(instance_type_chunk) == 1 and instance_type_match in instance_type_chunk:
                            print(
                                f"This retry will not execute because, after filtering, the instance_type_chunk becomes empty. regions: {region_chunk}, instance types: {instance_type_chunk}.")
                            break
                        elif len(instance_type_chunk) > 1 and instance_type_match in instance_type_chunk:
                            instance_type_chunk.remove(instance_type_match)
                            sps_shared_resources.found_invalid_instance_type_retry_count += 1
                            continue

                if "Bad Gateway" in error_message:
                    retries += 1
                    if retries >= max_retries_for_timeout:
                        print(f"BadGatewayConnection 대한 최대 재시도 횟수에 도달하여 요청을 건너뜁니다.")
                        break
                    sleep_time = round(random.uniform(0.2, 1.5), 1)
                    print(f"Retrying {retries}/{max_retries_for_timeout}... BadGatewayConnection for regions: {region_chunk}, instance types: {instance_type_chunk}.\nNow: [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}], Sleep: [{sleep_time}s]")
                    time.sleep(sleep_time)
                    sps_shared_resources.bad_request_retry_count += 1
                    continue

                if "Too Many Requests" in error_message:
                    with sps_shared_resources.lock:
                        sps_location_manager.update_over_limit_locations(account_id, subscription_id, location,
                                                                         all_over_limit_locations)
                    sleep_time = round(random.uniform(0.2, 1.5), 1)
                    sps_shared_resources.time_out_retry_count += 1
                    print(
                        f"Retrying {retries}/{max_retries_for_timeout}... Too Many Requests for regions: {region_chunk}, instance types: {instance_type_chunk}.\nNow: [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}], Sleep: [{sleep_time}s]")
                    time.sleep(sleep_time)
                    sps_shared_resources.too_many_requests_count += 1
                    continue


            except Exception as e:
                print(
                    f"Failed collect_spot_placement_recommendation_with_multithreading for regions {region_chunk}, for instance_types {region_chunk},  location: {location}")
                print(f"e.stderr: {e.stderr}")
                break

            return response



def extract_invalid_values(error_message):
    region_match = re.findall(
        r"The value '([a-zA-Z0-9-_]+)' provided for the input parameter 'desiredLocations' is not valid", error_message)

    instance_type_match = re.findall(
        r"The value '([a-zA-Z0-9-_]+)' provided for the input parameter 'SpotPlacementRecommenderInput.desiredSizes' is not valid",
        error_message)

    match_res = {}

    if region_match:
        match_res["region"] = region_match[0]
    else:
        match_res["region"] = None

    if instance_type_match:
        match_res["instanceType"] = instance_type_match[0]
    else:
        match_res["instanceType"] = None

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

def initialize_sps_shared_resources():
    sps_shared_resources.bad_request_retry_count = 0
    sps_shared_resources.time_out_retry_count = 0
    sps_shared_resources.too_many_requests_count = 0
    sps_shared_resources.found_invalid_region_retry_count = 0
    sps_shared_resources.found_invalid_instance_type_retry_count = 0
    sps_shared_resources.last_login_account = None