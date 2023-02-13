import pandas as pd
import numpy as np


# get instancetier from armSkuName
def get_instaceTier(armSkuName):
    split_armSkuName = armSkuName.split('_')
    if len(split_armSkuName) == 0:
        InstaceTier = np.nan
        return InstaceTier

    if split_armSkuName[0] == 'standard' or split_armSkuName[0] == 'basic':
        InstanceTier = split_armSkuName[0]
    else:
        InstanceTier = np.nan

    return InstanceTier


# get instancetype from armSkuName
def get_instaceType(armSkuName):
    split_armSkuName = armSkuName.split('_')

    if len(split_armSkuName) == 0:
        InstaceType = np.nan

        return InstaceType

    if split_armSkuName[0] == 'standard' or split_armSkuName[0] == 'basic':
        if len(split_armSkuName) == 1:
            InstanceType = np.nan
            return InstanceType
        InstanceType = '_'.join(split_armSkuName[1:])
    else:
        InstanceType = split_armSkuName[0]

    return InstanceType


def merge_df(price_df, eviction_df):
    eviction_df['InstanceTier'] = eviction_df['skuName'].apply(lambda skuName: get_instaceTier(skuName))
    eviction_df['InstanceType'] = eviction_df['skuName'].apply(lambda skuName: get_instaceType(skuName))

    price_df['LowerInstanceType'] = [x.lower() if x == x else np.nan for x in price_df['InstanceType'].values.tolist()]
    price_df['LowerInstanceTier'] = [x.lower() if x == x else np.nan for x in price_df['InstanceTier'].values.tolist()]

    join_df = pd.merge(price_df, eviction_df,
                       left_on=['LowerInstanceType', 'LowerInstanceTier', 'armRegionName'],
                       right_on=['InstanceType', 'InstanceTier', 'location'],
                       how='outer')

    join_df = join_df[
        ['InstanceTier_x', 'InstanceType_x', 'Region', 'OndemandPrice', 'SpotPrice', 'Savings', 'evictionRate']]

    join_df = join_df[~join_df['SpotPrice'].isna()]

    join_df.rename(columns={'InstanceTier_x': 'InstanceTier', 'InstanceType_x': 'InstanceType', 'evictionRate': 'IF'},
                   inplace=True)

    frequency_map = {'0-5': 3.0, '5-10': 2.5, '10-15': 2.0, '15-20': 1.5, '20+': 1.0}
    join_df = join_df.replace({'IF': frequency_map})

    return join_df
