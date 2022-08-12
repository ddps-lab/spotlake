import time
import requests
import threading
import pandas as pd
from concurrent.futures import ThreadPoolExecutor


API_LINK = "https://prices.azure.com:443/api/retail/prices?$filter=serviceName%20eq%20%27Virtual%20Machines%27%20and%20priceType%20eq%20%27Consumption%27%20and%20unitOfMeasure%20eq%20%271%20Hour%27&$skip="


def get_price(skip_num):
    global event
    global json_data_list
    
    get_link = API_LINK + str(skip_num)
    response = requests.get(get_link)
    
    if response.status_code != 200:
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
    
    ondemand_df = df[~df['meterName'].contains('Spot')]
    spot_df = df[df['meterName'].contains('Spot')]
    return ondemand_df, spot_df
