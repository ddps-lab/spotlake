import pandas as pd

def build_join_df(spot_price_df, ondemand_price_df, spotinfo_df, sps_df):
    sps_df = sps_df[['InstanceType', 'Region', 'AvailabilityZoneId', 'SPS']]
    spotinfo_df = spotinfo_df[['InstanceType', 'Region', 'IF']]
    ondemand_price_df = ondemand_price_df[['InstanceType', 'Region', 'OndemandPrice']]
    spot_price_df = spot_price_df[['InstanceType', 'AvailabilityZoneId', 'SpotPrice']]

    # need to change to outer join
    join_df = pd.merge(spot_price_df, sps_df, how="left")

    join_df = pd.merge(join_df, ondemand_price_df, how="left")

    join_df = pd.merge(join_df, spotinfo_df, how="left")

    join_df['Savings'] = int(100.0 - float(join_df['SpotPrice']) * 100 / float(join_df['OndemandPrice']))

    return join_df