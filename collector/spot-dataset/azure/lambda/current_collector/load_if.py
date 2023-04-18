import json
import boto3
import requests
import pandas as pd
import slack_msg_sender
from utill.azure_auth import get_token


def get_data(token, skip_token, retry=3):
    try:
        data = requests.post(
            "https://management.azure.com/providers/Microsoft.ResourceGraph/resources?api-version=2021-03-01", headers={
                "Authorization": "Bearer " + token
            }, json={
                "query": """spotresources\n
            | where type =~ \"microsoft.compute/skuspotevictionrate/location\"\n
            | project location = location, props = parse_json(properties)\n
            | project location = location, skuName = props.skuName, evictionRate = props.evictionRate\n
            | where isnotempty(skuName) and isnotempty(evictionRate) and isnotempty(location)
            """,
                "options": {
                    "resultFormat": "objectArray",
                    "$skipToken": skip_token
                }
            }).json()

        if not "data" in data:
            raise ValueError

        return data
    except:
        if retry == 1:
            raise
        return get_data(token, skip_token, retry - 1)


def load_if():
    try:
        token = get_token()

        datas = []
        skip_token = ""

        while True:
            data = get_data(token, skip_token)

            datas += data["data"]

            if not "$skipToken" in data:
                break
            skip_token = data["$skipToken"]
            
        eviction_df =  pd.DataFrame(datas)

        eviction_df['InstanceTier'] = eviction_df['skuName'].str.split('_', n=1, expand=True)[0].str.capitalize()
        eviction_df['InstanceType'] = eviction_df['skuName'].str.split('_', n=1, expand=True)[1].str.capitalize()
        
        frequency_map = {'0-5': 3.0, '5-10': 2.5, '10-15': 2.0, '15-20': 1.5, '20+': 1.0}
        eviction_df = eviction_df.replace({'evictionRate': frequency_map})

        eviction_df.rename(columns={'evictionRate': 'IF'}, inplace=True)
        eviction_df.rename(columns={'location': 'Region'}, inplace=True)
        
        eviction_df['OndemandPrice'] = -1.0
        eviction_df['SpotPrice'] = -1.0
        eviction_df['Savings'] = 1.0
        
        eviction_df = eviction_df[['InstanceTier', 'InstanceType', 'Region', 'OndemandPrice', 'SpotPrice', 'Savings', 'IF']]

        return eviction_df
    
    except Exception as e:
        result_msg = """AZURE Exception when load_if\n %s""" % (e)
        data = {'text': result_msg}
        slack_msg_sender.send_slack_message(result_msg)


