import requests
import pandas as pd
import numpy as np
import threading
from concurrent.futures import ThreadPoolExecutor
from utility import slack_msg_sender


API_LINK = 'https://prices.azure.com:443/api/retail/prices?$filter=serviceName%20eq%20%27Virtual%20Machines%27%20and%20priceType%20eq%20%27Consumption%27%20and%20unitOfMeasure%20eq%20%271%20Hour%27&$skip='
FILTER_LOCATIONS = ['GOV', 'EUAP', 'ATT', 'SLV', '']
price_list = []
MAX_SKIP = 2000
SKIP_NUM_LIST = [i*100 for i in range(MAX_SKIP)]
event = threading.Event()


# get instancetier from armSkuName
def get_instaceTier(armSkuName):
    split_armSkuName = armSkuName.split('_')

    if len(split_armSkuName) == 0:
        InstaceTier = np.nan
        return InstaceTier

    if split_armSkuName[0] == 'Standard' or split_armSkuName[0] == 'Basic':
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

    if split_armSkuName[0] == 'Standard' or split_armSkuName[0] == 'Basic':
        if len(split_armSkuName) == 1:
            InstanceType = np.nan
            return InstanceType
        InstanceType = '_'.join(split_armSkuName[1:])
    else:
        InstanceType = split_armSkuName[0]

    return InstanceType


# get price data using the API
def get_price(skip_num):
    get_link = API_LINK + str(skip_num)
    response = requests.get(get_link)

    for _ in range(5):
        if response.status_code == 200:
            break
        else:
            response = requests.get(get_link)

    if response.status_code != 200:
        slack_msg_sender.send_slack_message(response.status_code)
        raise Exception(f"api response status code is {response.status_code}")

    price_data = list(response.json()['Items'])

    if not price_data:
        event.set()

        return

    price_list.extend(price_data)

    return


# azure price dataframe preprocesing
def preprocessing_price(df):
    df = df[~df['productName'].str.contains('Windows')]
    df = df[~df['meterName'].str.contains('Priority')]
    df = df[~df['meterName'].str.contains('Expired')]
    df = df[~df['location'].str.split().str[0].isin(FILTER_LOCATIONS)]

    ondemand_df = df[~df['meterName'].str.contains('Spot')]
    spot_df = df[df['meterName'].str.contains('Spot')]

    list_meterName = list(spot_df['meterName'].str.split(' ').str[:-1].apply(' '.join))
    spot_df = spot_df.drop(['meterName'], axis=1)
    spot_df['meterName'] = list_meterName

    spot_df = spot_df[['location', 'armRegionName', 'armSkuName', 'retailPrice', 'meterName', 'effectiveStartDate']]
    spot_df.rename(columns={'retailPrice': 'SpotPrice'}, inplace=True)
    ondemand_df = ondemand_df[['location', 'armRegionName', 'armSkuName', 'retailPrice', 'meterName', 'effectiveStartDate']]
    ondemand_df.rename(columns={'retailPrice': 'OndemandPrice'}, inplace=True)

    drop_idx = ondemand_df.loc[(ondemand_df['location'] == '')].index
    ondemand_df.drop(drop_idx, inplace=True)

    join_df = pd.merge(ondemand_df, spot_df,
                       left_on=['location', 'armRegionName', 'armSkuName', 'meterName'],
                       right_on=['location', 'armRegionName', 'armSkuName', 'meterName'],
                       how='outer')

    join_df = join_df.dropna(subset=['SpotPrice'])

    join_df.loc[join_df['OndemandPrice'] == 0, 'OndemandPrice'] = None
    join_df['Savings'] = (join_df['OndemandPrice'] - join_df['SpotPrice']) / join_df['OndemandPrice'] * 100

    join_df['InstanceTier'] = join_df['armSkuName'].apply(lambda armSkuName: get_instaceTier(armSkuName))
    join_df['InstanceType'] = join_df['armSkuName'].apply(lambda armSkuName: get_instaceType(armSkuName))

    join_df = join_df.reindex(columns=['InstanceTier', 'InstanceType', 'location', 'OndemandPrice', 'SpotPrice', 'Savings'])

    join_df.rename(columns={'location': 'Region'}, inplace=True)

    return join_df


# collect azure price with multithreading
def collect_price_with_multithreading():
    with ThreadPoolExecutor(max_workers=16) as executor:
        for skip_num in SKIP_NUM_LIST:
            future = executor.submit(get_price, skip_num)
        event.wait()
        executor.shutdown(wait=True, cancel_futures=True)

    price_df = pd.DataFrame(price_list)
    savings_df = preprocessing_price(price_df)

    return savings_df
