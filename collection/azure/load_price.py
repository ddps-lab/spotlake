import requests
import numpy as np
import pandas as pd


API_LINK = "https://prices.azure.com:443/api/retail/prices?$filter=serviceName%20eq%20%27Virtual%20Machines%27%20and%20priceType%20eq%20%27Consumption%27%20and%20unitOfMeasure%20eq%20%271%20Hour%27&$skip="
FILTER_LOCATIONS = ['GOV', 'EUAP', 'ATT', 'SLV', '']


def get_price(skip_num):
    global event
    global json_data_list
    
    get_link = API_LINK + str(skip_num)
    response = requests.get(get_link)
    
    for i in range(5):
        if response.status_code == 200:
            break
        else:
            respose = request.get(get_link)
            
    if response.statuse_code != 200: 
        raise Exception(f"api response status code is {response.status_code}")
    
    price_data = list(response.json()['Items'])
    if not price_data:
        event.set()
        return
    price_list.extend(price_data)


def preprocessing_price(df):
    df = df[~df['productName'].contains('Windows')]
    df = df[~df['meterName'].contains('Priority')]
    df = df[~df['meterName'].contains('Expired')]
    df = df[df['isPrimaryMeterRegion'] == True]
    df = df[~df['location'].str.split().str[0].isin(FILTER_LOCATIONS)]
    
    ondemand_df = df[~df['meterName'].contains('Spot')]
    spot_df = df[df['meterName'].contains('Spot')]
    spot_df['meterName'] = spot_df['meterName'].str.split(' ').str[:-1].apply(' '.join)
    
    # outer or inner? need a decision
    join_df = pd.merge(ondemand_df, spot_df,
                       left_on=['location', 'armRegionName', 'armSkuName', 'meterName'],
                       right_on=['location', 'armRegionName', 'armSkuName', 'meterName'],
                       how='outer')
    
    join_df = join_df[['location', 'armSkuName', 'ondemandPrice', 'spotPrice']]    
    join_df.loc[join_df['ondemandPrice'] == 0, 'ondemandPrice'] = None
    
    join_df['Savings'] = (savings_df['ondemandPrice'] - savings_df['spotPrice']) / savings_df['ondemandPrice'] * 100
    join_df['Savings'] = np.floor(azure_df['Savings']).astype(int)
    
    join_df.rename({'location': 'Region','armSkuName': 'InstanceType', 'ondemandPrice': 'OndemandPrice', 'spotPrice': 'SpotPrice',}, inplace=True, axis=1)
    join_df['InstanceTier'] = join_df['InstanceType'].str.split('_').str[0]
    join_df['InstanceType'] = join_df['InstanceType'].str.split('_').str[1:].str.join('_')
    join_df = join_df[['InstanceTier', 'InstanceType', 'Region', 'OndemandPrice', 'SpotPrice', 'Savings']]
    join_df = join_df.sort_values(['InstanceTier', 'InstanceType', 'Region']).reset_index(drop=True)

    return join_df
