import time
import requests
import pandas as pd
import threading
from concurrent.futures import ThreadPoolExecutor

API_LINK = 'https://prices.azure.com:443/api/retail/prices?$filter=serviceName%20eq%20%27Virtual%20Machines%27%20and%20priceType%20eq%20%27Consumption%27%20and%20unitOfMeasure%20eq%20%271%20Hour%27&$skip='
FILTER_LOCATIONS = ['GOV', 'EUAP', 'ATT', 'SLV', '']
price_list = []
MAX_SKIP = 2000
SKIP_NUM_LIST = [i*100 for i in range(MAX_SKIP)]
event = threading.Event()

def get_price(skip_num):
    get_link = API_LINK + str(skip_num)
    response = requests.get(get_link)
    for _ in range(5):
        if response.status_code == 200:
            break
        else:
            time.sleep(0.5)
            response = requests.get(get_link)
    if response.status_code != 200:
        raise Exception(f"api response status code is {response.status_code}")
    price_data = list(response.json()['Items'])
    if not price_data:
        event.set()
        return
    price_list.extend(price_data)
    return


def preprocessing_price(df):
    #필요한 spot데이터만 가져옵니다.
    df = df[~df['productName'].str.contains('Windows')]
    df = df[~df['meterName'].str.contains('Priority')]
    df = df[~df['meterName'].str.contains('Expired')]
    df = df[df['isPrimaryMeterRegion'] == True]
    df = df[~df['location'].str.split().str[0].isin(FILTER_LOCATIONS)]
    #ondemand_df과 spot_df으로 나눕니다.
    ondemand_df = df[~df['meterName'].str.contains('Spot')]
    spot_df = df[df['meterName'].str.contains('Spot')]
    #spot_df에서 meterName에서 spot을 제외합니다.
    list_meterName = list(spot_df['meterName'].str.split(' ').str[:-1].apply(' '.join))
    spot_df = spot_df.drop(['meterName'], axis=1)
    spot_df['meterName'] = list_meterName
    #spot_df와 ondemand_df에서 필요한 column만 사용하고 retailPrice를 spotPrice, ondemandPrice로 변경합니다.
    spot_df = spot_df[['location', 'armRegionName', 'armSkuName', 'retailPrice', 'meterName', 'effectiveStartDate']]
    spot_df.rename(columns={'retailPrice': 'spotPrice'}, inplace=True)
    ondemand_df = ondemand_df[['location', 'armRegionName', 'armSkuName', 'retailPrice', 'meterName', 'effectiveStartDate']]
    ondemand_df.rename(columns={'retailPrice': 'ondemandPrice'}, inplace=True)
    #location이 비어있는 경우를 제외합니다.
    drop_idx = ondemand_df.loc[(ondemand_df['location'] == '')].index
    ondemand_df.drop(drop_idx, inplace=True)
    #ondemand_df와 spot_df를 join합니다.
    join_df = pd.merge(ondemand_df, spot_df,
                       left_on=['location', 'armRegionName', 'armSkuName', 'meterName'],
                       right_on=['location', 'armRegionName', 'armSkuName', 'meterName'],
                       how='outer')
    #join으로 인해 변경된 이름을 다시 설정합니다.
    join_df = join_df.rename(columns={'effectiveStartDate_x': 'ondemandEffectiveStartDate','effectiveStartDate_y': 'spotEffectiveStartDate'}, inplace=True)
    #savings를 계산합니다.
    join_df.loc[join_df['ondemandPrice'] == 0, 'ondemandPrice'] = None
    join_df['savings'] = (join_df['ondemandPrice'] - join_df['spotPrice']) / join_df['ondemandPrice'] * 100
    #index를 다시 설정합니다.
    join_df = join_df.reindex(
        columns=['location', 'armRegionName', 'armSkuName', 'meterName', 'ondemandPrice',
                 'spotPrice','savings','ondemandEffectiveStartDate', 'spotEffectiveStartDate'
                ])
    return join_df

def collect_price_with_multithreading():
    with ThreadPoolExecutor(max_workers=16) as executor:
        for skip_num in SKIP_NUM_LIST:
            future = executor.submit(get_price, skip_num)
        event.wait()
        executor.shutdown(wait=True, cancel_futures=True)
    price_df = pd.DataFrame(price_list)
    savings_df = preprocessing_price(price_df)
    return savings_df
