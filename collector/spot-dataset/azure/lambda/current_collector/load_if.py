import requests
import pandas as pd
from utils.pub_service import send_slack_message
from utils.azure_auth import get_sps_token_and_subscriptions

pd.set_option('future.no_silent_downcasting', True)

def get_data(sps_token, skip_token, retry=3):
    try:
        headers = {
            "Authorization": f"Bearer {sps_token}",
        }
        data = requests.post(
            "https://management.azure.com/providers/Microsoft.ResourceGraph/resources?api-version=2024-04-01",
            headers=headers,
            json={
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

        if len(data['data']) > 0:
            return data
        else:
            return None

    except:
        if retry == 1:
            raise
        return get_data(sps_token, skip_token, retry - 1)


def load_if():
    try:
        sps_token, _ = get_sps_token_and_subscriptions()
        datas = []
        skip_token = ""

        while True:
            data = get_data(sps_token, skip_token)
            if not data:
                break

            datas += data["data"]
            skip_token = data.get("$skipToken", None)

            if skip_token is None:
                break

        if not datas:
            return None

        eviction_df = pd.DataFrame(datas)

        eviction_df['InstanceTier'] = eviction_df['skuName'].str.split('_', n=1, expand=True)[0].str.capitalize()
        eviction_df['InstanceType'] = eviction_df['skuName'].str.split('_', n=1, expand=True)[1].str.capitalize()

        frequency_map = {'0-5': 3.0, '5-10': 2.5, '10-15': 2.0, '15-20': 1.5, '20+': 1.0}
        eviction_df = eviction_df.replace({'evictionRate': frequency_map})

        eviction_df.rename(columns={'evictionRate': 'IF'}, inplace=True)
        eviction_df.rename(columns={'location': 'Region'}, inplace=True)

        eviction_df['OndemandPrice'] = -1.0
        eviction_df['SpotPrice'] = -1.0
        eviction_df['Savings'] = 1.0

        eviction_df = eviction_df[
            ['InstanceTier', 'InstanceType', 'Region', 'OndemandPrice', 'SpotPrice', 'Savings', 'IF']]

        return eviction_df

    except Exception as e:
        result_msg = """AZURE Exception when load_if\n %s""" % (e)
        data = {'text': result_msg}
        send_slack_message(result_msg)