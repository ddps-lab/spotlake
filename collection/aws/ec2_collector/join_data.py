import pandas as pd
import numpy as np

def build_join_df(spot_price_df, ondemand_price_df, spotinfo_df, sps_df):
    sps_df = sps_df[['InstanceType', 'Region', 'AvailabilityZoneId', 'SPS']]
    spotinfo_df = spotinfo_df[['InstanceType', 'Region', 'IF']]
    ondemand_price_df = ondemand_price_df[['InstanceType', 'Region', 'OndemandPrice']]
    spot_price_df = spot_price_df[['InstanceType', 'AvailabilityZoneId', 'SpotPrice']]

    spot_price_df['SpotPrice'] = spot_price_df['SpotPrice'].astype('float')
    spot_price_df['SpotPrice'] = spot_price_df['SpotPrice'].round(5)
    ondemand_price_df['OndemandPrice'] = ondemand_price_df['OndemandPrice'].astype('float')
    ondemand_price_df['OndemandPrice'] = ondemand_price_df['OndemandPrice'].round(5)

    # need to change to outer join

    join_df = pd.merge(spot_price_df, sps_df, how="outer")

    join_df = pd.merge(join_df, ondemand_price_df, how="outer")

    join_df = pd.merge(join_df, spotinfo_df, how="outer")

    join_df['Savings'] = 100.0 - (join_df['SpotPrice'] * 100 / join_df['OndemandPrice'])
    join_df['Savings'] = join_df['Savings'].fillna(-1)
    join_df['SPS'] = join_df['SPS'].fillna(-1)
    join_df['SpotPrice'] = join_df['SpotPrice'].fillna(-1)
    join_df['OndemandPrice'] = join_df['OndemandPrice'].fillna(-1)
    join_df['IF'] = join_df['IF'].fillna(-1)

    join_df['Savings'] = join_df['Savings'].astype('int')
    join_df['SPS'] = join_df['SPS'].astype('int')
    join_df = join_df.rename({'AvailabilityZoneId': 'AZ'}, axis=1)

    print(join_df.info())

    return join_df
