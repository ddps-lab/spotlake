import json
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv
import load_price

load_dotenv('./files_sps/.env')
INVALID_REGIONS_PATH_JSON = os.getenv('INVALID_REGIONS_PATH_JSON')
INVALID_INSTANCE_TYPES_PATH_JSON = os.getenv('INVALID_INSTANCE_TYPES_PATH_JSON')


def request_regions_and_instance_types_df_by_priceapi():
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
        regions_and_instance_types_df = price_source_df[['armRegionName', 'InstanceTypeNew']]
        regions_and_instance_types_df = regions_and_instance_types_df.rename(columns={
            'armRegionName': 'RegionCode',
            'InstanceTypeNew': 'InstanceType'
        })

        regions_and_instance_types_df['RegionCode'] = regions_and_instance_types_df['RegionCode'].str.strip()
        regions_and_instance_types_df['InstanceType'] = regions_and_instance_types_df['InstanceType'].str.strip()

        return regions_and_instance_types_df[['RegionCode', 'InstanceType']]

    except Exception as e:
        print(f"Failed to get_regions_and_instance_types_df_by_priceapi, Error: " + e)
        return None


def update_invalid_regions(invalid_region, invalid_regions):
    """
    이 메서드는 무효한 region 데이터를 업데이트하고 저장합니다.
    새로 발견된 무효 region을 추가하며, 업데이트된 데이터를 JSON 파일에 저장합니다.
    """
    try:
        # debugging 필요할지 몰라 남깁니다.
        # print("Start to save invalid_region. Region:", invalid_region + ", Current time:", now_time)
        now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        if invalid_regions is None:
            invalid_regions = []

        if isinstance(invalid_regions, list):
            if invalid_region not in invalid_regions:
                invalid_regions.append(invalid_region)


        new_data = {
            "update_date": now_time,
            "invalid_regions": invalid_regions,
            "regions_count": len(invalid_regions)
        }


        with open(INVALID_REGIONS_PATH_JSON, 'w') as file:
            json.dump(new_data, file, indent=4)
        return True

    except Exception as e:
        print(f"Failed to update_invalid_regions: {e}")
        return False

def update_invalid_instance_types(invalid_instance_type, invalid_instance_types):
    """
    이 메서드는 무효한 instance_type 데이터를 업데이트하고 저장합니다.
    새로 발견된 무효 instance_type 을 추가하며, 업데이트된 데이터를 JSON 파일에 저장합니다.
    """
    try:
        # debugging 필요할지 몰라 남깁니다.
        # print("Start to save invalid instance type:", invalid_instance_type + ", Current timestamp:", now_time)
        now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        if invalid_instance_types is None:
            invalid_instance_types = []

        if isinstance(invalid_instance_types, list):
            if invalid_instance_type not in invalid_instance_types:
                invalid_instance_types.append(invalid_instance_type)


        new_data = {
            "update_date": now_time,
            "invalid_instance_types": invalid_instance_types,
            "types_count": len(invalid_instance_types)
        }


        with open(INVALID_INSTANCE_TYPES_PATH_JSON, 'w') as file:
            json.dump(new_data, file, indent=4)
        return True

    except Exception as e:
        print(f"Failed to update_invalid_instance_types: {e}")
        return False

def load_invalid_regions():
    """
    이 메서드는 무효한 region 데이터(JSON 파일)를 로드합니다.
    데이터가 비어 있거나 유효하지 않을 경우 None을 반환합니다.
    """
    try:
        with open(INVALID_REGIONS_PATH_JSON, "r", encoding="utf-8") as json_file:
            content = json_file.read().strip()
            if not content:
                return None

            parsed_content = json.loads(content)
            return None if isinstance(parsed_content, dict) and not parsed_content else parsed_content

    except json.JSONDecodeError as e:
        print(f"load_invalid_regions func. JSON decoding error: {str(e)}")
    except Exception as e:
        print(f"load_invalid_regions func. An unexpected error occurred: {e}")

    return None

def load_invalid_instance_types():
    """
    이 메서드는 무효화된 instance_type 데이터(JSON 파일)를 로드합니다.
    데이터가 비어 있거나 유효하지 않을 경우 None을 반환합니다.
    """
    try:
        with open(INVALID_INSTANCE_TYPES_PATH_JSON, "r", encoding="utf-8") as json_file:
            content = json_file.read().strip()
            if not content:
                return None

            parsed_content = json.loads(content)
            return None if isinstance(parsed_content, dict) and not parsed_content else parsed_content

    except json.JSONDecodeError as e:
        print(f"load_invalid_instance_types func. JSON decoding error: {str(e)}")

    except Exception as e:
        print(f"load_invalid_instance_types func. An unexpected error occurred: {e}")

    return None