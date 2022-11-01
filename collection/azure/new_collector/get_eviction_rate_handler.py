import json
import boto3
import requests
from util.auth import get_token


def lambda_handler(event, context):
    token = get_token()

    datas = []
    skip_token = ""

    while True:
        data = requests.post("https://management.azure.com/providers/Microsoft.ResourceGraph/resources?api-version=2021-03-01", headers={
            "Authorization": "Bearer " + token
        }, json={
            "query": "spotresources\n| where type =~ \"microsoft.compute/skuspotevictionrate/location\"\n| project location = location, props = parse_json(properties)\n| project location = location, skuName = props.skuName, evictionRate = props.evictionRate\n| where isnotempty(skuName) and isnotempty(evictionRate) and isnotempty(location)",
            "options": {
                "resultFormat": "objectArray",
                "$skipToken": skip_token
            }
        }).json()

        datas += data["data"]

        if not "$skipToken" in data:
            break
        skip_token = data["$skipToken"]

    return datas
