import json
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv
import load_price
import sps_shared_resources



load_dotenv('./files_sps/.env')
INVALID_REGIONS_PATH_JSON = os.getenv('INVALID_REGIONS_PATH_JSON')
INVALID_INSTANCE_TYPES_PATH_JSON = os.getenv('INVALID_INSTANCE_TYPES_PATH_JSON')

SS_Resources = sps_shared_resources

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


def load_invalid_regions():
    # S3 이용으로 변경 예정
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
            return parsed_content['invalid_regions']

    except json.JSONDecodeError as e:
        print(f"load_invalid_regions func. JSON decoding error: {str(e)}")
    except Exception as e:
        print(f"load_invalid_regions func. An unexpected error occurred: {e}")

    return None


def load_invalid_instance_types():
    # S3 이용으로 변경 예정
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
            return parsed_content['invalid_instance_types']


    except json.JSONDecodeError as e:
        print(f"load_invalid_instance_types func. JSON decoding error: {str(e)}")

    except Exception as e:
        print(f"load_invalid_instance_types func. An unexpected error occurred: {e}")

    return None


def save_invalid_regions():
    # S3 이용으로 변경 예정
    try:
        now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data = {
            "save_time": now_time,
            "invalid_regions": SS_Resources.invalid_regions_tmp,
            "regions_count": len(SS_Resources.invalid_regions_tmp)
        }

        with open(INVALID_REGIONS_PATH_JSON, 'w') as file:
            json.dump(data, file, indent=4)
        print(f"Succeed to save_invalid_regions.")
        return True

    except Exception as e:
        print(f"Failed to save_invalid_regions: {e}")
        return False

def save_invalid_instance_types():
    # S3 이용으로 변경 예정
    try:
        now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        data = {
            "save_time": now_time,
            "invalid_instance_types": SS_Resources.invalid_instance_types_tmp,
            "types_count": len(SS_Resources.invalid_instance_types_tmp)
        }

        with open(INVALID_INSTANCE_TYPES_PATH_JSON, 'w') as file:
            json.dump(data, file, indent=4)
        print(f"Succeed to save_invalid_instance_types.")
        return True

    except Exception as e:
        print(f"Failed to save_invalid_regions: {e}")
        return False