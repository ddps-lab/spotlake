import pandas as pd
import numpy as np

def merge_price_eviction_df(price_df, eviction_df):
    join_df = pd.merge(price_df, eviction_df,
                    left_on=['InstanceType', 'InstanceTier', 'armRegionName'],
                    right_on=['InstanceType', 'InstanceTier', 'Region'],
                    how='outer')
    
    join_df = join_df[['InstanceTier', 'InstanceType', 'Region_x', 'armRegionName', 'OndemandPrice_x', 'SpotPrice_x', 'Savings_x', 'IF']]
    join_df = join_df[~join_df['SpotPrice_x'].isna()]
    join_df.rename(columns={'Region_x' : 'Region', 'OndemandPrice_x' : 'OndemandPrice', 'SpotPrice_x' : 'SpotPrice', 'Savings_x' : 'Savings'}, inplace=True)

    return join_df


def merge_price_eviction_sps_df(price_eviction_df, sps_df, availability_zones=True):
    join_df = pd.merge(price_eviction_df, sps_df, on=['InstanceTier', 'InstanceType', 'Region'], how='outer')
    join_df.rename(columns={'time_x': 'PriceEviction_Update_Time', 'time_y': 'SPS_Update_Time'}, inplace=True)
    join_df.drop(columns=['id', 'InstanceTypeSPS', 'RegionCodeSPS'], inplace=True)

    columns = ["InstanceTier", "InstanceType", "Region", "OndemandPrice", "SpotPrice", "Savings", "IF",
        "PriceEviction_Update_Time", "DesiredCount", "Score", "SPS_Update_Time"]

    if availability_zones:
        columns.insert(-2, "AvailabilityZone")  # "Score" 앞에 삽입

    join_df = join_df[columns]

    return join_df