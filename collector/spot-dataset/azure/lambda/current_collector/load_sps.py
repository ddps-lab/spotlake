import re
import random
import requests
import sps_location_manager
import sps_shared_resources
import sps_regions_instance_types_manager
import concurrent.futures
import time
import sps_prepare_parameters
from json import JSONDecodeError
from functools import wraps
from datetime import datetime
from utill.azure_auth import get_sps_token_and_subscriptions
from const_config import AzureCollector, Storage
from utill.aws_service import S3Handler

STORAGE_CONST = Storage()
AZURE_CONST = AzureCollector()
S3 = S3Handler()

SS_Resources = sps_shared_resources
SRI_M = sps_regions_instance_types_manager
SL_M = sps_location_manager
SS_Resources.sps_token, SS_Resources.subscriptions = get_sps_token_and_subscriptions()

def log_execution_time(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if getattr(wrapper, "_is_running", False):
            return func(*args, **kwargs)

        wrapper._is_running = True
        try:
            start_time = datetime.now()
            result = func(*args, **kwargs)
            end_time = datetime.now()

            elapsed_time = end_time - start_time
            minutes, seconds = divmod(elapsed_time.seconds, 60)

            print(f"{func.__name__} executed in {minutes}min {seconds}sec")

            return result
        finally:
            wrapper._is_running = False
    return wrapper

@log_execution_time
def collect_spot_placement_score_first_time(desired_count, collect_time):
    if initialize_files_in_s3():
        assert get_variable_from_s3()
        initialize_sps_shared_resources()

        print(f"Start to collect_spot_placement_score_first_time")

        start_time = time.time()
        regions_and_instance_types_df = SRI_M.request_regions_and_instance_types_df_by_priceapi()
        df_greedy_clustering_initial = sps_prepare_parameters.greedy_clustering_to_create_optimized_request_list(regions_and_instance_types_df)
        end_time = time.time()
        elapsed = end_time - start_time
        minutes, seconds = divmod(int(elapsed), 60)
        print(f"request_regions_and_instance_types_df_by_priceapi + greedy_clustering_to_create_optimized_request_list time: {minutes}min {seconds}sec")

        sps_location_manager.check_and_add_available_locations()

        start_time = time.time()
        execute_spot_placement_score_task_by_parameter_pool_df(df_greedy_clustering_initial, True, desired_count, collect_time)
        print(f'Time_out_retry_count: {SS_Resources.time_out_retry_count}')
        print(f'Bad_request_retry_count: {SS_Resources.bad_request_retry_count}')
        print(f'Too_many_requests_count: {SS_Resources.too_many_requests_count}')
        print(f'Found_invalid_region_retry_count: {SS_Resources.found_invalid_region_retry_count}')
        print(f'Found_invalid_instance_type_retry_count: {SS_Resources.found_invalid_instance_type_retry_count}')

        end_time = time.time()
        elapsed = end_time - start_time
        minutes, seconds = divmod(int(elapsed), 60)
        print(f"execute_spot_placement_score_task_by_parameter_pool_df time: {minutes}min {seconds}sec")

        start_time = time.time()
        regions_and_instance_types_filtered_df = sps_prepare_parameters.filter_invalid_parameter(
            regions_and_instance_types_df)
        df_greedy_clustering_filtered = sps_prepare_parameters.greedy_clustering_to_create_optimized_request_list(
            regions_and_instance_types_filtered_df)

        S3.upload_file(df_greedy_clustering_filtered, AZURE_CONST.DF_TO_USE_TODAY_PKL_FILENAME, "pkl")

        end_time = time.time()
        elapsed = end_time - start_time
        minutes, seconds = divmod(int(elapsed), 60)
        print(f"Prepare the request pool. time: {minutes}min {seconds}sec")


@log_execution_time
def collect_spot_placement_score(desired_count, collect_time):
    assert get_variable_from_s3()
    initialize_sps_shared_resources()

    print(f"Start to collect_spot_placement_score")
    df_greedy_clustering_filtered = S3.read_file(AZURE_CONST.DF_TO_USE_TODAY_PKL_FILENAME, 'pkl')

    execute_spot_placement_score_task_by_parameter_pool_df(df_greedy_clustering_filtered, True, desired_count, collect_time)
    print(f'Time_out_retry_count: {SS_Resources.time_out_retry_count}')
    print(f'Bad_request_retry_count: {SS_Resources.bad_request_retry_count}')
    print(f'Too_many_requests_count: {SS_Resources.too_many_requests_count}')
    print(f'Found_invalid_region_retry_count: {SS_Resources.found_invalid_region_retry_count}')
    print(f'Found_invalid_instance_type_retry_count: {SS_Resources.found_invalid_instance_type_retry_count}')


def execute_spot_placement_score_task_by_parameter_pool_df(api_calls_df, availability_zones, desired_count, collect_time):
    merged_result = {
        "Collect_Time": collect_time,
        "Desired_Count": desired_count,
        "Availability_Zones": availability_zones,
        "Placement_Scores": []
    }
    locations = list(SS_Resources.locations_call_history_tmp[list(SS_Resources.locations_call_history_tmp.keys())[0]].keys())

    with concurrent.futures.ThreadPoolExecutor(max_workers=int(len(locations) * 1)) as executor:
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
                raise

            except Exception as e:
                print(f"execute_spot_placement_score_task_by_parameter_pool_df func. An unexpected error occurred: {e}")
                raise


    save_tmp_files_to_s3()
    file_name = f"result/{collect_time}.json"
    S3.upload_file(merged_result, file_name, "json")
    return True


def execute_spot_placement_score_api(region_chunk, instance_type_chunk, availability_zones, desired_count, max_retries=10):
    retries = 0
    while retries <= max_retries:
        region_chunk = filter_invalid_items(region_chunk, "invalid_regions")
        instance_type_chunk = filter_invalid_items(instance_type_chunk, "invalid_instance_types")

        if region_chunk is None or instance_type_chunk is None:
            print(f"execute_spot_placement_score_api: Execution skipped as filtered chunks are empty. "
                  f"region_chunk: {region_chunk}, instance_type_chunk: {instance_type_chunk}")
            return None

        request_body = {
            "availabilityZones": availability_zones,
            "desiredCount": desired_count,
            'desiredLocations' : region_chunk,
            'desiredSizes' : [{"sku": instance_type} for instance_type in instance_type_chunk]
        }

        with SS_Resources.location_lock:
            res = sps_location_manager.get_next_available_location()
            if res is None:
                print("No available locations with remaining calls.")
                return "NO_AVAILABLE_LOCATIONS"
            subscription_id, location, history, over_limit_locations = res
            sps_location_manager.update_call_history(subscription_id, location, history)

        url = f"https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.Compute/locations/{location}/diagnostics/spotPlacementRecommender/generate?api-version=2024-06-01-preview"
        headers = {
            "Authorization": f"Bearer {sps_shared_resources.sps_token}",
            "Content-Type": "application/json",
        }
        try:
            response = requests.post(url, headers=headers, json=request_body, timeout=15)
            response.raise_for_status()
            return response.json()

        except requests.exceptions.Timeout:
            "Timeout"
            retries = handle_retry("Timeout", retries, max_retries)

        except requests.exceptions.HTTPError as http_err:
            error_message = http_err.response.text
            match_res = extract_invalid_values(error_message)
            if match_res:
                if match_res["invalid_region"]:
                    region_chunk = del_invalid_chunk(region_chunk, match_res["invalid_region"], "invalid_region")
                    if region_chunk is None:
                        print(f"This retry will not execute because, after filtering, the region_chunk becomes empty.")
                        break
                    retries = handle_retry("InvalidRegion", retries, max_retries)

                if match_res["invalid_instanceType"]:
                    instance_type_chunk = del_invalid_chunk(instance_type_chunk, match_res["invalid_instanceType"],"invalid_instanceType")
                    if instance_type_chunk is None:
                        print(f"This retry will not execute because, after filtering, the instance_type_chunk becomes empty.")
                        break
                    retries = handle_retry("InvalidInstanceType", retries, max_retries)

            if "BadGatewayConnection" in error_message:
                print(f"HTTP error occurred: {error_message}")
                retries = handle_retry("BadGatewayConnection", retries, max_retries)

            elif "InvalidParameter" in error_message:
                print(f"HTTP error occurred: {error_message}, region_chunk: {region_chunk}, instance_type_chunk: {instance_type_chunk}")
                print(f"url: {url}")
                retries = handle_retry("InvalidParameter", retries, max_retries)

            elif "You have reached the maximum number of requests allowed." in error_message:
                print(f"HTTP error occurred: {error_message}")
                with SS_Resources.location_lock:
                    sps_location_manager.update_over_limit_locations(subscription_id, location)
                retries = handle_retry("Too Many Requests", retries, max_retries)

        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            break

        if retries:
            continue

    if retries is None:
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
        "invalid_region": region_match.group(1) if region_match else None,
        "invalid_instanceType": instance_type_match.group(1) if instance_type_match else None
    }
    return match_res


def initialize_files_in_s3():
    try:
        files_to_initialize = {
            AZURE_CONST.INVALID_REGIONS_JSON_FILENAME: [],
            AZURE_CONST.INVALID_INSTANCE_TYPES_JSON_FILENAME: []
        }

        for file_name, data in files_to_initialize.items():
            S3.upload_file(data, file_name, "json", initialization=True)
        return True

    except Exception as e:
        print(f"An error occurred during S3 initialization: {e}")
        return False


def del_invalid_chunk(chunk, invalid_value, value_type):
    with SS_Resources.lock:
        if value_type == "invalid_region":
            if invalid_value not in SS_Resources.invalid_regions_tmp:
                SS_Resources.invalid_regions_tmp.append(invalid_value)

        elif value_type == "invalid_instanceType":
            if invalid_value not in SS_Resources.invalid_instance_types_tmp:
                SS_Resources.invalid_instance_types_tmp.append(invalid_value)

    if invalid_value in chunk:
        chunk.remove(invalid_value)
    else:
        print(f"x not in list, invalid_value: {invalid_value}, chunk: {chunk}")

    return chunk if chunk else None


def handle_retry(error_type, retries, max_retries):
    if error_type == "Timeout":
        SS_Resources.time_out_retry_count += 1
    elif error_type == "BadGatewayConnection":
        SS_Resources.bad_request_retry_count += 1
        print(f"BadGatewayConnection +1 {SS_Resources.bad_request_retry_count}")
    elif error_type == "Too Many Requests":
        SS_Resources.too_many_requests_count += 1
    elif error_type == "InvalidRegion":
        SS_Resources.found_invalid_region_retry_count += 1
    elif error_type == "InvalidInstanceType":
        SS_Resources.found_invalid_instance_type_retry_count += 1

    if retries < max_retries:
        sleep_time = round(random.uniform(0.5, 1.5), 1)
        time.sleep(sleep_time)
        retries += 1
        return retries
    else:
        return None


def filter_invalid_items(items, invalid_type):
    if invalid_type == "invalid_regions":
        invalid_data = SS_Resources.invalid_regions_tmp
    elif invalid_type == "invalid_instance_types":
        invalid_data = SS_Resources.invalid_instance_types_tmp
    else:
        return None

    filtered_items = [item for item in items if item not in invalid_data]
    return filtered_items if filtered_items else None


def initialize_sps_shared_resources():
    SS_Resources.bad_request_retry_count = 0
    SS_Resources.time_out_retry_count = 0
    SS_Resources.too_many_requests_count = 0
    SS_Resources.found_invalid_region_retry_count = 0
    SS_Resources.found_invalid_instance_type_retry_count = 0

def save_tmp_files_to_s3():
    files_to_upload = {
        AZURE_CONST.INVALID_REGIONS_JSON_FILENAME: SS_Resources.invalid_regions_tmp,
        AZURE_CONST.INVALID_INSTANCE_TYPES_JSON_FILENAME: SS_Resources.invalid_instance_types_tmp,
        AZURE_CONST.LOCATIONS_CALL_HISTORY_JSON_FILENAME: SS_Resources.locations_call_history_tmp,
        AZURE_CONST.LOCATIONS_OVER_LIMIT_JSON_FILENAME: SS_Resources.locations_over_limit_tmp,
        AZURE_CONST.LAST_SUBSCRIPTION_ID_AND_LOCATION_JSON_FILENAME: {
            "last_subscription_id": SS_Resources.last_subscription_id_and_location_tmp['last_subscription_id'],
            "last_location": SS_Resources.last_subscription_id_and_location_tmp['last_location']
        },
    }

    for file_name, file_data in files_to_upload.items():
        if file_data:
            S3.upload_file(file_data, file_name, "json")

def get_variable_from_s3():
    try:
        invalid_regions_data = S3.read_file(AZURE_CONST.INVALID_REGIONS_JSON_FILENAME, 'json')
        instance_types_data = S3.read_file(AZURE_CONST.INVALID_INSTANCE_TYPES_JSON_FILENAME, 'json')
        call_history_data = S3.read_file(AZURE_CONST.LOCATIONS_CALL_HISTORY_JSON_FILENAME, 'json')
        over_limit_data = S3.read_file(AZURE_CONST.LOCATIONS_OVER_LIMIT_JSON_FILENAME, 'json')
        last_location_index_data = S3.read_file(AZURE_CONST.LAST_SUBSCRIPTION_ID_AND_LOCATION_JSON_FILENAME, 'json')

        SS_Resources.invalid_regions_tmp = invalid_regions_data
        SS_Resources.invalid_instance_types_tmp = instance_types_data
        SS_Resources.locations_call_history_tmp = call_history_data
        SS_Resources.locations_over_limit_tmp = over_limit_data
        SS_Resources.last_subscription_id_and_location_tmp = {
            "last_subscription_id": last_location_index_data.get('last_subscription_id'),
            "last_location": last_location_index_data.get('last_location')
        }

        if all(data is not None for data in [
            SS_Resources.invalid_regions_tmp,
            SS_Resources.invalid_instance_types_tmp,
            SS_Resources.locations_call_history_tmp,
            SS_Resources.locations_over_limit_tmp,
            SS_Resources.last_subscription_id_and_location_tmp
        ]):
            print("[S3]: Succeed to prepare variable from s3.")
            return True

        else:
            return False

    except KeyError as e:
        print(f"Missing expected key in S3 JSON data: {e}")
        return False
    except Exception as e:
        print(f"Error loading files from S3: {e}")
        return False