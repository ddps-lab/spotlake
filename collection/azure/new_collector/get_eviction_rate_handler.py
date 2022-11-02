import json
import boto3
import requests
from util.auth import get_token
import traceback

SLACK_WEBHOOK_URL = ""


def get_data(token, skip_token, retry=3):
    try:
        data = requests.post("https://management.azure.com/providers/Microsoft.ResourceGraph/resources?api-version=2021-03-01", headers={
            "Authorization": "Bearer " + token
        }, json={
            "query": "spotresources\n| where type =~ \"microsoft.compute/skuspotevictionrate/location\"\n| project location = location, props = parse_json(properties)\n| project location = location, skuName = props.skuName, evictionRate = props.evictionRate\n| where isnotempty(skuName) and isnotempty(evictionRate) and isnotempty(location)",
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


def lambda_handler(event, context):
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

        return datas
    except Exception as e:
        requests.post(SLACK_WEBHOOK_URL, json={
                      "text": f"get_eviction_rate_handler\n\n{traceback.format_exc()}"})
        return []
