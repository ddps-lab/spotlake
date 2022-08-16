import pandas as pd

def build_join_df(spot_price_df, ondemand_price_df, spotinfo_df, sps_df):
    sps_df = sps_df[['InstanceType', 'Region', 'AvailabilityZoneId', 'SPS']]
    spotinfo_df = spotinfo_df[['InstanceType', 'Region', 'IF']]
    ondemand_price_df = ondemand_price_df[['InstanceType', 'Region', 'OndemandPrice']]
    spot_price_df = spot_price_df[['InstanceType', 'AvailabilityZoneId', 'SpotPrice']]

    # need to change to outer join
    join_df = pd.merge(sps_df, spotinfo_df, how="inner")

    sps_spotinfo_outer_df = pd.merge(sps_df, spotinfo_df, how="outer")

    join_df = pd.merge(join_df, ondemand_price_df, how="inner")

    join_df = pd.merge(join_df, spot_price_df, how="inner")

    return join_df