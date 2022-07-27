import requests
import pandas as pd
import pickle
import time
from concurrent.futures import ThreadPoolExecutor


def get_data(page_link):   
    response = requests.get(page_link)  

    json_data = list(response.json()['Items'])
    if not json_data:
        print("No such pages:", page_link)
        return
    
    for j in json_data:
        spot_dict = dict()
        ondemand_dict = dict()

        if (('Priority' in j["meterName"]) | ('Expired' in j["meterName"]) | ('Windows' in j["productName"]) | (j["isPrimaryMeterRegion"] == False)):
            continue
        elif ('Spot' in j["meterName"]):   
            spot_dict["location"] = j["location"]
            spot_dict["armRegionName"] = j["armRegionName"]
            spot_dict["armSkuName"] = j["armSkuName"]
            spot_dict["spotPrice"] = j["retailPrice"]
            spot_dict["meterName"] = ' '.join(x for x in j["meterName"].split(' ')[:-1])
            spot_dict["effectiveStartDate"] = j["effectiveStartDate"]
            spot_data.append(spot_dict)
        else:
            ondemand_dict["location"] = j["location"]
            ondemand_dict["armRegionName"] = j["armRegionName"]
            ondemand_dict["armSkuName"] = j["armSkuName"]
            ondemand_dict["ondemandPrice"] = j["retailPrice"]
            ondemand_dict["meterName"] = j["meterName"]
            ondemand_dict["effectiveStartDate"] = j["effectiveStartDate"]
            ondemand_data.append(ondemand_dict)
            
    return 


def store_df_to_pickle(table, file_path):
    table.to_pickle(file_path)
        
        
def calculate_savings(savings_table):
    savings_table['savings'] = 0.0
    no_savings_rows = savings_table['ondemandPrice'].isnull() | savings_table['spotPrice'].isnull()

    for index, row in savings_table.iterrows():
        if no_savings_rows[index] == False:
            ondemand_price = row[3]
            spot_price = row[6]
            try:
                savings = (ondemand_price - spot_price) / ondemand_price * 100
            except ZeroDivisionError:
                savings = None
            savings_table['savings'][index] = savings
        else:
            savings_table['savings'][index] = None
    
    return savings_table


if __name__ == '__main__':
    global ondemand_data
    global spot_data
    ondemand_data = []
    spot_data = []
    
    pickle_file_path = './Azure-savings-fast.pkl'
    
    page = 'https://prices.azure.com:443/api/retail/prices?$filter=serviceName%20eq%20%27Virtual%20Machines%27%20and%20priceType%20eq%20%27Consumption%27%20and%20unitOfMeasure%20eq%20%271%20Hour%27&$skip='
    pages_list = []
    for i in range(1380): 
        pages_list.append(page + str(i*100))  

    with ThreadPoolExecutor(max_workers=16) as executor:
        for page in pages_list:
            executor.submit(get_data, page)
    
    spot_data = list(spot_data)
    ondemand_data = list(ondemand_data)
    
    azure_spot_table = pd.DataFrame(spot_data)
    azure_ondemand_table = pd.DataFrame(ondemand_data)
    
    drop_idx = azure_ondemand_table.loc[(azure_ondemand_table['location'] == '')].index 
    azure_ondemand_table.drop(drop_idx, inplace=True)

    savings_table = pd.merge(azure_ondemand_table, azure_spot_table, left_on=['location', 'armRegionName', 'armSkuName', 'meterName'], right_on=['location', 'armRegionName', 'armSkuName', 'meterName'], how='outer') 
    savings_table = savings_table.rename(columns = {'effectiveStartDate_x': 'ondemandEffectiveStartDate', 'effectiveStartDate_y': 'spotEffectiveStartDate'})

    savings_table = calculate_savings(savings_table)
    
    savings_table = savings_table.reindex(columns=['location', 'armRegionName', 'armSkuName', 'meterName', 'ondemandPrice', 'spotPrice', 'savings', 'ondemandEffectiveStartDate', 'spotEffectiveStartDate'])

    store_df_to_pickle(savings_table, pickle_file_path)
