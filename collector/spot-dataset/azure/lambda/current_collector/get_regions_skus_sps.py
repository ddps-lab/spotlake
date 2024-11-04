import concurrent
import json
import os
import subprocess
import importlib
from datetime import datetime

load_sps = importlib.import_module("load_sps")

# 값 가져오기
subscription_id = load_sps.subscription_id
az_cli_path = load_sps.az_cli_path
region_vm_sku_source = load_sps.region_vm_sku_source
region_vm_sku_processed = load_sps.region_vm_sku_processed

def get_regions_and_skus():
    '''
    파일에 있는 regions / skus 정보를 가겨옵니다
    :return: region, vm_skus / None
    '''

    # 처리된 파일이 있는지 다시 확인
    if not os.path.exists(region_vm_sku_processed):
        print(f"Error: {region_vm_sku_processed} still not found after extraction. Exiting.")
        return None

    try:
        # 처리된 파일에서 리전과 SKU 읽기
        with open(region_vm_sku_processed, "r") as f:
            spot_data = json.load(f)
            region = spot_data['regions']
            vm_skus = spot_data['skus']

        return region, vm_skus
    except Exception as e:
        print(f"Failed to get_regions_and_skus, Error: " + e)

def save_and_extract_regions_and_skus():
    '''
    REGION_VM_SKU_SOURCE 파일에서 리전과 SKU를 추출하고, 고유한 리전과 SKU를 중복제거하고 양식 정리하여 신규 파일에 저장합니다.
    :return: True / False
    '''
    if not os.path.exists(region_vm_sku_source):
        print(f"File {region_vm_sku_source} does not exist.")
        return False

    try:
        with open(region_vm_sku_source, "r") as f:
            data = json.load(f)

        regions = list(data.keys())
        vm_skus = set()  # set을 사용해 자동으로 중복 제거

        # 각 리전의 SKU 목록을 순회하면서 병합하고 중복 제거
        for region, sku_list in data.items():
            vm_skus.update(sku_list)
        skus = list(vm_skus)
        # 결과 구성
        output_data = {
            "update_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # 업데이트 날짜
            "regions_count": len(regions),  # 지역 개수
            "skus_count": len(skus),  # skus 개수
            "regions": regions,
            "skus": skus
        }

        # 결과를 파일에 저장, 기존 파일을 덮어씀
        with open(region_vm_sku_processed, "w") as f:
            json.dump(output_data, f, indent=3)

        print(f"Extracted regions and SKUs saved to {region_vm_sku_processed}")
        return True

    except Exception as e:
        print(f"Failed to save_and_extract_regions_and_skus, Error: " + e)
        return False

def get_azure_regions():
    '''
    리전 정보를 가져옵니다
    :return: result_regions / None
    '''
    command = [az_cli_path, "account", "list-locations", "--query", "[].{RegionName:name}", "--output", "json"]
    print(az_cli_path)
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
    regions = json.loads(result.stdout)

    if len(regions) > 0:
        result_regions = [region["RegionName"] for region in regions]
        return result_regions
    else:
        return None


def get_vm_skus_for_region(region):
    '''
    지정된 리전에서 VM SKU 정보를 가져옵니다.
    :param region: 단일 리전
    :return: valid_skus
    '''
    uri = f"https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.Compute/skus?api-version=2024-06-01-preview&$filter=location eq '{region}'"
    command = [
        az_cli_path, "rest",
        "--method", "get",
        "--uri", uri,
        "--query", "value[].{sku: name, location: locations[0], capabilities: capabilities}",
        "--output", "json"
    ]
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
    all_skus = json.loads(result.stdout)
    valid_skus = filter_valid_vm_skus(all_skus)
    return valid_skus


def filter_valid_vm_skus(vm_skus):
    '''
    유효한 VM SKU만 필터링하고, VM이 아닌 SKU 항목을 제거함
    :param vm_skus
    :return: valid_vm_skus / None
    '''
    valid_vm_skus = []
    excluded_vm_skus = {"Standard_LRS", "Standard_ZRS"}
    try:
        for sku in vm_skus:
            # "Standard_" 또는 "Basic_"으로 시작하는 SKU만 유지
            if (sku['sku'].startswith('Standard_') or sku['sku'].startswith('Basic_')) and sku['sku'] not in excluded_vm_skus:
                valid_vm_skus.append(sku)
        return valid_vm_skus

    except Exception as e:
        print(f"Failed to filter_valid_vm_skus, Error: " + e)
        return None


def save_all_vm_skus(max_workers=5):
    '''
    모든 리전을 순회하며 각 리전의 VM SKU 정보를 가져옵니다.
    다중 스레드를 사용해 데이터를 병렬로 가져오고 파일에 저장합니다.
    :param max_workers: 별령 시행 수치
    :return: True / False
    '''
    # 데이터를 업데이트하고 파일에 저장
    regions = get_azure_regions()
    all_skus = {}

    # 각 리전을 처리하는 함수를 정의
    def fetch_skus_for_region(region):
        print(f"Fetching VM SKUs for region: {region}")
        vm_skus = get_vm_skus_for_region(region)
        return region, [sku_info['sku'] for sku_info in vm_skus]

    try:
        # 스레드 풀을 사용해 SKU 정보를 병렬로 가져옴
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(fetch_skus_for_region, region) for region in regions]

            for future in concurrent.futures.as_completed(futures):
                region, vm_skus = future.result()
                all_skus[region] = vm_skus

        # 로컬 파일에 저장, 리전과 VM SKU 정보만 포함
        with open(region_vm_sku_source, "w") as f:
            json.dump(all_skus, f, indent=3)

        print(f"VM SKUs saved to {region_vm_sku_source}")
        return True

    except Exception as e:
        print(f"Failed to save_all_vm_skus, Error: " + e)
        return False