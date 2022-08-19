import pandas as pd
import numpy as np

def build_join_df(spot_price_df, ondemand_price_df, spotinfo_df, sps_df):
    sps_df = sps_df[['InstanceType', 'Region', 'AvailabilityZoneId', 'SPS']]
    spotinfo_df = spotinfo_df[['InstanceType', 'Region', 'IF']]
    ondemand_price_df = ondemand_price_df[['InstanceType', 'Region', 'OndemandPrice']]
    spot_price_df = spot_price_df[['InstanceType', 'AvailabilityZoneId', 'SpotPrice']]

    # need to change to outer join
    join_df = pd.merge(spot_price_df, sps_df, how="left")

    join_df = pd.merge(join_df, ondemand_price_df, how="left")

    join_df = pd.merge(join_df, spotinfo_df, how="left")

    join_df['SpotPrice'] = join_df['SpotPrice'].astype('float')
    join_df['OndemandPrice'].fillna(join_df['SpotPrice'])
    join_df['Savings'] = 100.0 - (join_df['SpotPrice'] * 100 / join_df['OndemandPrice'])
    join_df['Savings'] = join_df['Savings'].fillna(0)

    join_df['Savings'] = join_df['Savings'].astype('int')

    return join_df