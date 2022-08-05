import pandas as pd

def build_join_df(spot_price_df, ondemand_price_df, spotinfo_df, sps_df):
    sps_df = sps_df[['InstanceType', 'Region', 'AvailabilityZoneId', 'SPS']]
    spotinfo_df = spotinfo_df[['InstanceType', 'Region', 'IF']]
    ondemand_price_df = ondemand_price_df[['InstanceType', 'Region', 'OndemandPrice']]
    spot_price_df = spot_price_df[['InstanceType', 'AvailabilityZoneId', 'SpotPrice']]

    print(f"length of spot price : {len(spot_price_df)}")
    print(f"length of ondemand price : {len(ondemand_price_df)}")
    print(f"length of spotinfo : {len(spotinfo_df)}")
    print(f"length of sps : {len(sps_df)}")

    # need to change to outer join
    join_df = pd.merge(sps_df, spotinfo_df, how="inner")

    print(f"length of sps spotinfo joined data : {len(join_df)}")

    sps_spotinfo_outer_df = pd.merge(sps_df, spotinfo_df, how="outer")
    print(f"length of sps spotinfo joined data (outer) : {len(sps_spotinfo_outer_df)}")

    join_df = pd.merge(join_df, ondemand_price_df, how="inner")
    print(f"length of sps spotinfo ondemand joined data : {len(join_df)}")

    join_df = pd.merge(join_df, spot_price_df, how="inner")
    print(f"length of complete data : {len(join_df)}")

    return join_df