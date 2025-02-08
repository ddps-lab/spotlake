import pandas as pd
import numpy as np

def merge_df(price_df, eviction_df):
    join_df = pd.merge(price_df, eviction_df,
                    left_on=['InstanceType', 'InstanceTier', 'armRegionName'],
                    right_on=['InstanceType', 'InstanceTier', 'Region'],
                    how='outer')
    
    join_df = join_df[['InstanceTier', 'InstanceType', 'Region_x', 'armRegionName', 'OndemandPrice_x', 'SpotPrice_x', 'Savings_x', 'IF']]
    join_df = join_df[~join_df['SpotPrice_x'].isna()]
    join_df.rename(columns={'Region_x' : 'Region', 'OndemandPrice_x' : 'OndemandPrice', 'SpotPrice_x' : 'SpotPrice', 'Savings_x' : 'Savings'}, inplace=True)

    return join_df
