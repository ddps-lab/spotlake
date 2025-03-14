import pandas as pd

def merge_price_saving_if_df(price_df, if_df):
    join_df = pd.merge(price_df, if_df,
                    left_on=['InstanceType', 'InstanceTier', 'armRegionName'],
                    right_on=['InstanceType', 'InstanceTier', 'Region'],
                    how='outer')
    
    join_df = join_df[['InstanceTier', 'InstanceType', 'Region_x', 'armRegionName', 'OndemandPrice_x', 'SpotPrice_x', 'Savings_x', 'IF']]
    join_df = join_df[~join_df['SpotPrice_x'].isna()]
    join_df.rename(columns={'Region_x' : 'Region', 'OndemandPrice_x' : 'OndemandPrice', 'SpotPrice_x' : 'SpotPrice', 'Savings_x' : 'Savings'}, inplace=True)

    return join_df


def merge_if_saving_price_sps_df(price_saving_if_df, sps_df, az=True):
    join_df = pd.merge(price_saving_if_df, sps_df, on=['InstanceTier', 'InstanceType', 'Region'], how='outer')
    join_df.rename(columns={'time_x': 'PriceEviction_Update_Time', 'time_y': 'SPS_Update_Time'}, inplace=True)
    join_df.drop(columns=['id', 'InstanceTypeSPS', 'RegionCodeSPS'], inplace=True)

    join_df['SPS_Update_Time'].fillna(join_df['PriceEviction_Update_Time'], inplace=True)

    columns = ["InstanceTier", "InstanceType", "Region", "OndemandPrice", "SpotPrice", "Savings", "IF",
        "DesiredCount", "Score", "SPS_Update_Time"]

    if az:
        columns.insert(-2, "AvailabilityZone")  # "Score" 앞에 삽입

    join_df = join_df[columns]

    join_df.fillna({
        "InstanceTier": "N/A",
        "InstanceType": "N/A",
        "Region": "N/A",
        "OndemandPrice": -1,
        "SpotPrice": -1,
        "Savings": -1,
        "IF": -1,
        "DesiredCount": -1,
        "Score": "N/A",
        "AvailabilityZone": "N/A",
        "SPS_Update_Time": "N/A"
    }, inplace=True)

    return join_df