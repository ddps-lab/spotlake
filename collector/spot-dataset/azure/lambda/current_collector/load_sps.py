import re
import random
import requests
import concurrent.futures
import time
import load_price
import pandas as pd
from sps_module import sps_location_manager
from sps_module import sps_shared_resources
from sps_module import sps_prepare_parameters
from json import JSONDecodeError
from functools import wraps
from datetime import datetime
from utill.azure_auth import get_sps_token_and_subscriptions
from const_config import AzureCollector, Storage
from utill.pub_service import S3


STORAGE_CONST = Storage()
AZURE_CONST = AzureCollector()

SS_Resources = sps_shared_resources
SL_Manager = sps_location_manager

SS_Resources.sps_token, SS_Resources.subscriptions = get_sps_token_and_subscriptions()


# 본 시간 수집 function은 추후 제거 예정입니다.
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
def collect_spot_placement_score_first_time(desired_count):
    # 시간 수집 로직들은 추후 제거 예정입니다.
    '''
    이 메서드는 0:00분에 호출합니다.
    1. RESET 필요한 일부 S3 파일을 RESET
    2. priceapi로 regions_and_instance_types 원본 수집
    3. greedy_clustering 방법으로 호출 파라미터 pool만들기
    4. invalid 값으로 valid한 location 을 갱신.
    5. SPS 호출 및 invalid_region, invalid_instanceType 수집.
    6. regions_and_instance_types 원본을 invalid_region, invalid_instanceType 으로 필터링
    7. 다시 greedy_clustering 방법으로 하루애 이용 예정한 호출 파라미터 pool만들고 S3에 업로드
    '''
    if initialize_files_in_s3():
        assert get_variable_from_s3()
        initialize_sps_shared_resources()

        start_time = time.time()
        res_price_api = collect_regions_and_instance_types_df_by_priceapi()
        regions_and_instance_types_df, SS_Resources.region_map_and_instance_map_tmp['region_map'], SS_Resources.region_map_and_instance_map_tmp['instance_map'] = res_price_api
        S3.upload_file(SS_Resources.region_map_and_instance_map_tmp,
                       AZURE_CONST.REGION_MAP_AND_INSTANCE_MAP_JSON_FILENAME, "json")

        end_time = time.time()
        elapsed = end_time - start_time
        minutes, seconds = divmod(int(elapsed), 60)
        print(f"collect_regions_and_instance_types_df_by_priceapi. time: {minutes}min {seconds}sec")

        start_time = time.time()
        df_greedy_clustering_initial = sps_prepare_parameters.greedy_clustering_to_create_optimized_request_list(regions_and_instance_types_df)
        end_time = time.time()
        elapsed = end_time - start_time
        minutes, seconds = divmod(int(elapsed), 60)
        print(f"greedy_clustering_to_create_optimized_request_list. time: {minutes}min {seconds}sec")

        SL_Manager.check_and_add_available_locations()

        start_time = time.time()
        sps_res_df = execute_spot_placement_score_task_by_parameter_pool_df(df_greedy_clustering_initial, True, desired_count)
        print(f'Time_out_retry_count: {SS_Resources.time_out_retry_count}')
        print(f'Bad_request_retry_count: {SS_Resources.bad_request_retry_count}')
        print(f'Too_many_requests_count: {SS_Resources.too_many_requests_count}')
        print(f'Too_many_requests_count_2: {SS_Resources.too_many_requests_count_2}')
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

        return sps_res_df


@log_execution_time
def collect_spot_placement_score(desired_count):
    '''
    이 메서드는 0:00분외에 매 10분 마다 호출합니다.
    1. 하루애 이용 예정한 호출 파라미터 pool을 S3에서 read.
    2. SPS 호출 및 invalid_region, invalid_instanceType 수집 및 필터링.
    '''

    assert get_variable_from_s3()
    initialize_sps_shared_resources()

    df_greedy_clustering_filtered = S3.read_file(AZURE_CONST.DF_TO_USE_TODAY_PKL_FILENAME, 'pkl')

    sps_res_df = execute_spot_placement_score_task_by_parameter_pool_df(df_greedy_clustering_filtered, True, desired_count)
    print(f'Time_out_retry_count: {SS_Resources.time_out_retry_count}')
    print(f'Bad_request_retry_count: {SS_Resources.bad_request_retry_count}')
    print(f'Too_many_requests_count: {SS_Resources.too_many_requests_count}')
    print(f'Found_invalid_region_retry_count: {SS_Resources.found_invalid_region_retry_count}')
    print(f'Found_invalid_instance_type_retry_count: {SS_Resources.found_invalid_instance_type_retry_count}')

    return sps_res_df


def execute_spot_placement_score_task_by_parameter_pool_df(api_calls_df, availability_zones, desired_count):
    '''
    SPS 수집 공용 메서드, 멀티 호출 실행, 결과를 S3에 업로드
    '''
    # 결과를 저장할 리스트
    results = []

    locations = list(SS_Resources.locations_call_history_tmp[list(SS_Resources.locations_call_history_tmp.keys())[0]].keys())

    with concurrent.futures.ThreadPoolExecutor(max_workers=int(len(locations) * 1)) as executor:
        futures = []

        for row in api_calls_df.itertuples(index=False):
            future = executor.submit(
                execute_spot_placement_score_api,
                row.Regions, row.InstanceTypes, availability_zones, desired_count, max_retries=50
            )
            futures.append(future)

        try:
            for future in futures:
                try:
                    result = future.result()
                    if result and result != "NO_AVAILABLE_LOCATIONS":
                        for score in result["placementScores"]:
                            score_data = {
                                "DesiredCount": desired_count,
                                "AvailabilityZone": score.get("availabilityZone", None),
                                "RegionCodeSPS": score.get("region", None),
                                "Region": SS_Resources.region_map_and_instance_map_tmp['region_map'].get(
                                    score.get("region", ""), ""),
                                "InstanceTypeSPS": score.get("sku", None),
                                "InstanceTier": SS_Resources.region_map_and_instance_map_tmp['instance_map'].get(
                                    score.get("sku", ""), {}).get("InstanceTier", None),
                                "InstanceType": SS_Resources.region_map_and_instance_map_tmp['instance_map'].get(
                                    score.get("sku", ""), {}).get("InstanceTypeOld", None),
                                "Score": score.get("score", None)
                            }
                            results.append(score_data)

                    elif result == "NO_AVAILABLE_LOCATIONS":
                        # NO_AVAILABLE_LOCATIONS인 경우 나머지 작업 취소
                        for f in futures:
                            if not f.done():
                                f.cancel()

                except JSONDecodeError as e:
                    print(
                        f"execute_spot_placement_score_task_by_parameter_pool_df func. JSON decoding error: {str(e)}")
                    raise

                except Exception as e:
                    print(
                        f"execute_spot_placement_score_task_by_parameter_pool_df func. An unexpected error occurred: {e}")
                    raise
        finally:
            save_tmp_files_to_s3()


    sps_res_df = pd.DataFrame(results)

    sps_res_df.drop_duplicates(
        subset=["RegionCodeSPS", "InstanceTypeSPS", "AvailabilityZone"],
        keep="last",
        inplace=True
    )

    print("execute_spot_placement_score_task_by_parameter_pool_df. Successfully.")
    return sps_res_df


def execute_spot_placement_score_api(region_chunk, instance_type_chunk, availability_zones, desired_count, max_retries=10):
    '''
    실제 SPS API호출 메서드.
    '''
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
            res = SL_Manager.get_next_available_location()
            if res is None:
                print("No available locations with remaining calls.")
                return "NO_AVAILABLE_LOCATIONS"
            subscription_id, location, history, over_limit_locations = res
            SL_Manager.update_call_history(subscription_id, location, history)

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
                    SL_Manager.update_over_limit_locations(subscription_id, location)
                retries = handle_retry("Too Many Requests", retries, max_retries)

            elif "Max retries exceeded with url" in error_message:
                print(f"HTTP error occurred: {error_message}")
                with SS_Resources.location_lock:
                    SL_Manager.update_over_limit_locations(subscription_id, location)
                retries = handle_retry("Too Many Requests(2)", retries, max_retries)

        except Exception as e:
            print(f"execute_spot_placement_score_api. An unexpected error occurred: {e}")
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
            S3.upload_file(data, file_name, "json")

        print("Successfully initialized files in S3.")
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
    elif error_type == "Too Many Requests":
        SS_Resources.too_many_requests_count += 1
    elif error_type == "Too Many Requests(2)":
        SS_Resources.too_many_requests_count_2 += 1
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
        region_map_and_instance_map = S3.read_file(AZURE_CONST.REGION_MAP_AND_INSTANCE_MAP_JSON_FILENAME, 'json')

        SS_Resources.invalid_regions_tmp = invalid_regions_data
        SS_Resources.invalid_instance_types_tmp = instance_types_data
        SS_Resources.locations_call_history_tmp = call_history_data
        SS_Resources.locations_over_limit_tmp = over_limit_data
        SS_Resources.last_subscription_id_and_location_tmp = {
            "last_subscription_id": last_location_index_data.get('last_subscription_id'),
            "last_location": last_location_index_data.get('last_location')
        }
        SS_Resources.region_map_and_instance_map_tmp = {
            "region_map": region_map_and_instance_map.get('region_map'),
            "instance_map": region_map_and_instance_map.get('instance_map')
        }

        if all(data is not None for data in [
            SS_Resources.invalid_regions_tmp,
            SS_Resources.invalid_instance_types_tmp,
            SS_Resources.locations_call_history_tmp,
            SS_Resources.locations_over_limit_tmp,
            SS_Resources.last_subscription_id_and_location_tmp,
            SS_Resources.region_map_and_instance_map_tmp
        ]):
            print("[S3]: Successfully prepared variable from s3.")
            return True

        else:
            return False

    except KeyError as e:
        print(f"Missing expected key in S3 JSON data: {e}")
        return False
    except Exception as e:
        print(f"Error loading files from S3: {e}")
        return False

def collect_regions_and_instance_types_df_by_priceapi():
    """
    이 메서드는 Price API를 호출하여 region 및 instance_type 데이터를 가져옵니다.
    결과는 DataFrame 형식으로 정리되며 'RegionCode'와 'InstanceType' 열을 포함합니다.
    """
    try:
        price_source_df = load_price.collect_price_with_multithreading()
        price_source_df = price_source_df[price_source_df['InstanceTier'].notna()]
        price_source_df['InstanceTypeNew'] = price_source_df.apply(
            lambda row: f"{row['InstanceTier']}_{row['InstanceType']}" if pd.notna(row['InstanceTier']) else row[
                'InstanceType'], axis=1
        )
        regions_and_instance_types_df = price_source_df[['armRegionName', 'Region', 'InstanceTypeNew', 'InstanceTier', 'InstanceType']]
        regions_and_instance_types_df = regions_and_instance_types_df.rename(columns={
            'InstanceType': 'InstanceTypeOld',
            'armRegionName': 'RegionCode',
            'InstanceTypeNew': 'InstanceType'
        })

        regions_and_instance_types_df['RegionCode'] = regions_and_instance_types_df['RegionCode'].map(lambda x: x.strip() if isinstance(x, str) else x)
        regions_and_instance_types_df['InstanceType'] = regions_and_instance_types_df['InstanceType'].map(lambda x: x.strip() if isinstance(x, str) else x)


        region_map = regions_and_instance_types_df[['RegionCode', 'Region']].drop_duplicates().set_index('RegionCode')['Region'].to_dict()

        instance_map = regions_and_instance_types_df[['InstanceType', 'InstanceTier', 'InstanceTypeOld']].drop_duplicates().set_index('InstanceType').to_dict(orient='index')

        return regions_and_instance_types_df, region_map, instance_map

    except Exception as e:
        print(f"Failed to collect_regions_and_instance_types_df_by_priceapi, Error: {e}")
        return None