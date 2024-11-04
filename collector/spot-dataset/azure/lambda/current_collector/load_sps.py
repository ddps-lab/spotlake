import os
import subprocess
import json
import re
import concurrent.futures
import time
import configparser
from datetime import datetime
import get_regions_skus_sps

config = configparser.ConfigParser()
config.read('./files_sps/config_sps.ini', encoding='utf-8')

# True 시 API에서 데이터를 가져오거 저장합니다.
update_skus_region = True

# update_invalid_locations이 True일대 regions를 1개로 cut하고, vm_skus을 고정 1개로 설정하여 invalid_locations를 얻어오는 스위치입니다.
# 한 번  실행후 정상적으로 8 리전 5 vm_skus으로 정보를 가져옵니다.
update_invalid_locations = True

availability_zones = False
regions_cut = 8
vm_skus_cut = 5
desired_counts = [1, 2, 5, 30, 100, 1000]
max_workers = 6

current_time = datetime.now().strftime("%y%m%d_%H%M")
subscription_id = config.get('Config', 'SUBSCRIPTION_ID')
az_cli_path = config.get('Config', 'AZ_CLI_PATH')

if availability_zones:
    region_vm_sku_source = config.get('Availability_zones_True', 'REGION_VM_SKU_SOURCE_FILENAME')
    region_vm_sku_processed = config.get('Availability_zones_True', 'REGION_VM_SKU_PROCESSED_FILENAME')
    invalid_regions = config.get('Availability_zones_True', 'INVALID_REGIONS_FILENAME')
    results_for_recommendation = config.get('Availability_zones_True', 'RESULTS_FOR_RECOMMENDATION_PATH')

else:
    region_vm_sku_source = config.get('Availability_zones_False', 'REGION_VM_SKU_SOURCE_FILENAME')
    region_vm_sku_processed = config.get('Availability_zones_False', 'REGION_VM_SKU_PROCESSED_FILENAME')
    invalid_regions = config.get('Availability_zones_False', 'INVALID_REGIONS_FILENAME')
    results_for_recommendation = config.get('Availability_zones_False', 'RESULTS_FOR_RECOMMENDATION_PATH')
results_for_recommendation = f"{results_for_recommendation}recommendation_results_{current_time}.json"


def execute_az_cli_spot_placement_recommendation(region_chunk, sku_chunk, availability_zones, desired_count, invalid_locations, max_retries_for_timeout=4):
    '''
    az rest 실행
    :param region_chunk: 8개 제한이 있어, cut를한 region 정보
    :param sku_chunk: 5개 제한이 있어, cut를한 sku 정보
    :param invalid_locations: 저장한 필요한 invalid_locations 변수
    :return: response / None
    '''
    request_body = {
        "availabilityZones": availability_zones,
        "desiredCount": desired_count,
        "desiredLocations": [region for region in region_chunk],
        "desiredSizes": [{"sku": sku} for sku in sku_chunk],
    }

    command = [
        az_cli_path, "rest",
        "--method", "post",
        "--uri",
        f"https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.Compute/locations/koreasouth/diagnostics/spotPlacementRecommender/generate?api-version=2024-06-01-preview",
        "--headers", "Content-Type=application/json",
        "--body", json.dumps(request_body)
    ]

    retries = 0
    while retries <= max_retries_for_timeout:
        try:
            result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True, timeout=15)
            response = result.stdout
            print(f"Spot Placement Recommender Response for region {region_chunk}, VM SKUs {sku_chunk}")
            return json.loads(response)

        except subprocess.TimeoutExpired:
            # Timeout시에만 재시도 합니다.
            retries += 1
            print(f"Retrying {retries}/{max_retries_for_timeout}..., Timeout occurred for region {region_chunk}, VM SKUs {sku_chunk}. ")

        except subprocess.CalledProcessError as e:
            error_message = e.stderr
            location_match = re.findall(
                r"The value '([a-zA-Z0-9-]+)' provided for the input parameter 'desiredLocations' is not valid", error_message)


            invalid_locations.extend(location_match)
            print(f"Failed for region {region_chunk}")
            print(f"e.stderr: {e.stderr}")
            return None  # None리턴，재시도 중지

        except subprocess.SubprocessError as e:
            print(f"An unexpected error occurred: {e}")
            return None  # None리턴，재시도 중지

    # 최대 재시도 회수 만료시, 메세지 추출후 None 리턴
    print(f"Max retries reached for region {region_chunk}. Skipping this request.")
    return None


def save_spot_placement_recommendation(update_invalid_locations, regions, regions_cut, vm_skus, vm_skus_cut, availability_zones, desired_counts, max_workers=5):
    '''
    Spot Placement Recommender 추천을 가져옵니다.
    :param update_invalid_locations: True일대 regions를 1개로 cut하고, vm_skus을 고정 1개로 설정하여 invalid_locations를 얻어오는 스위치입니다.
    :param regions: CLI통해 얻은 모든 regions 정보
    :param vm_skus: REGION_VM_SKU_PROCESSED에 있는 처리된 전체 skus정보
    :param availability_zones: availability_zones 필요 있는지 True/False
    :param desired_count: desired_count지정
    :param max_workers: 동시 처리 수치
    :return: True / False
    '''
    if update_invalid_locations:
        # update_invalid_locations가 True일 때, region과 SKU,desired_counts를 1개씩만 처리합니다.
        regions_chunks = [regions[i:i + 1] for i in range(0, len(regions), 1)]
        sku_chunks = [['Standard_A1_v2']]
        desired_counts = [1]
    else:
        try:
            # INVALID_REGIONS_FILENAME 파일을 열어 유효하지 않은 지역 데이터를 불러옵니다.
            with open(invalid_regions, "r") as json_file:
                invalid_regions_data = json.load(json_file)
                existing_invalid_locations = set(invalid_regions_data.get("invalid_locations", []))
        except FileNotFoundError:
            # 파일이 없을 경우, 빈 set으로 초기화합니다.
            print("유효하지 않은 지역 파일이 없습니다.")
            existing_invalid_locations = set()

        # 유효한 지역만 필터링합니다.
        valid_regions = [region for region in regions if region not in existing_invalid_locations]

        # 유효한 지역을 regions_cut 크기로 나눕니다.
        regions_chunks = [valid_regions[i:i + regions_cut] for i in range(0, len(valid_regions), regions_cut)]

        # SKU를 vm_skus_cut 크기로 나눕니다.
        sku_chunks = [vm_skus[i:i + vm_skus_cut] for i in range(0, len(vm_skus), vm_skus_cut)]

        print(f"필터된 유효한 지역: {valid_regions}")

    invalid_locations = []  # 모든 오류 발생 지역 초기화

    merged_result = {
        "availabilityZones": availability_zones,
        "desiredLocations": set(),  # 중복 방지를 위해 set 사용
        "desiredSizes": set(),  # 중복 방지를 위해 set 사용
        "placementScores": []
    }

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for region_chunk in regions_chunks:
            for sku_chunk in sku_chunks:
                for desired_count in desired_counts:
                    future = executor.submit(
                        execute_az_cli_spot_placement_recommendation,
                        region_chunk, sku_chunk, availability_zones, desired_count, invalid_locations, max_retries_for_timeout=4
                    )
                    futures.append((future, desired_count))  # 将 desired_count 与 future 一起保存

        # 모든 작업 결과를 수집하고 처리합니다.
        for future, desired_count in futures:
            try:
                result = future.result()
                if result:
                    # 결과를 병합합니다.
                    merged_result["desiredLocations"].update(result["desiredLocations"])
                    for size in result["desiredSizes"]:
                        merged_result["desiredSizes"].add(size["sku"])

                    # 각 딕셔너리에서 "isQuotaAvailable" 키 삭제 (존재하는 경우)
                    for score in result["placementScores"]:
                        if "isQuotaAvailable" in score:
                            del score["isQuotaAvailable"]
                        score["desired_count"] = desired_count  # desired_count 추가

                    merged_result["placementScores"].extend(result["placementScores"])
                else:
                    pass

            except Exception as e:
                print(f"작업 실행 중 오류 발생: {e}")
                return False

    if not update_invalid_locations:
        # 결과의 set을 list로 변환합니다.
        merged_result["desiredLocations"] = list(merged_result["desiredLocations"])
        # "desiredSizes"에 SKU 정보만 추가합니다.
        for size in result["desiredSizes"]:
            if isinstance(size, tuple):
                # 튜플을 딕셔너리로 변환하여 SKU 추출
                size_dict = dict(size)
                merged_result["desiredSizes"].add(size_dict["sku"])
            else:
                # 이미 딕셔너리인 경우 직접 SKU 추출
                merged_result["desiredSizes"].add(size["sku"])

        # 최종적으로 set을 list로 변환합니다.
        merged_result["desiredSizes"] = list(merged_result["desiredSizes"])

        # 병합된 결과를 출력합니다.
        print(json.dumps(merged_result, indent=3))

        # 결과를 JSON 파일로 저장합니다.
        with open(results_for_recommendation, "w") as json_file:
            json.dump(merged_result, json_file, indent=3)

        print(f"병합된 결과가 {results_for_recommendation} 파일에 저장되었습니다.")
        return True

    else:
        print(f"유효하지 않은 지역 저장 모드 실행 완료.")

    if invalid_locations and update_invalid_locations:
        # 중복된 유효하지 않은 지역을 제거합니다.
        new_invalid_locations = list(set(invalid_locations))

        # 새로 기록할 데이터 준비
        new_data = {
            "update_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # 업데이트 날짜
            "invalid_locations": new_invalid_locations,  # 새로운 지역 목록
            "invalid_locations_count": len(new_invalid_locations)  # 지역 개수
        }

        # 기존 파일 내용을 불러옵니다.
        try:
            with open(invalid_regions, "r") as json_file:
                existing_data = json.load(json_file)
        except (FileNotFoundError, json.JSONDecodeError):
            # 파일이 없거나 JSON 형식이 올바르지 않을 경우 기존 데이터를 None으로 설정합니다.
            existing_data = None

        # 새 데이터와 기존 데이터의 차이를 비교합니다.
        if existing_data != new_data:
            # 데이터가 다를 경우, 새로운 데이터를 덮어씁니다.
            with open(invalid_regions, "w") as json_file:
                json.dump(new_data, json_file, indent=3)
            print(f"유효하지 않은 지역이 업데이트되었습니다: {invalid_regions}, 개수: {len(new_invalid_locations)}")
        else:
            print("기존 데이터와 동일하여 업데이트를 건너뜁니다.")
    return True





if __name__ == "__main__":
    # 실행 시작 시간
    start_time = time.time()
    print(f"프로그램 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")



    print(f"skus_region 갱신모드: " + str(update_invalid_locations))
    print(f"invalid_locations 갱신모드: " + str(update_invalid_locations))

    print("----------------------------------")


    if update_skus_region:
        get_regions_skus_sps.save_all_vm_skus(max_workers)     # 모든 리전의 VM SKU 정보를 가져오고 REGION_VM_SKU_SOURCE 파일에 저장
        get_regions_skus_sps.save_and_extract_regions_and_skus()     # REGION_VM_SKU_SOURCE의 리전과 SKU 데이터를 중복제거와 정리합니다.

    regions, vm_skus = get_regions_skus_sps.get_regions_and_skus()

    if update_invalid_locations:
        save_spot_placement_recommendation(update_invalid_locations, regions, regions_cut, vm_skus, vm_skus_cut,
                                           availability_zones, desired_counts, max_workers)
        save_spot_placement_recommendation(False, regions, regions_cut, vm_skus, vm_skus_cut, availability_zones,
                                           desired_counts, max_workers)
    else:
        save_spot_placement_recommendation(False, regions, regions_cut, vm_skus, vm_skus_cut, availability_zones,
                                           desired_counts, max_workers)

    # 실행 종료 시간 및 총 소요 시간 계산
    end_time = time.time()
    elapsed_time = end_time - start_time
    minutes, seconds = divmod(elapsed_time, 60)

    # 종료 시간과 소요 시간 출력
    print(f"프로그램 종료: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"총 소요 시간: {int(minutes)}분 {seconds:.2f}초")

