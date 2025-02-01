import json
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv
import load_price
import sps_shared_resources

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