import aiohttp
import asyncio
import requests
from util.auth import get_token
from util.s3 import S3
import traceback

AZURE_SUBSCRIPTION_ID = ""
LIMIT = 2000
SLACK_WEBHOOK_URL = ""

datas = {}


def get_hardwaremap():
    s3 = S3("azure-hardware-map")
    return s3.get_json("hardwaremap.json")


async def get_data(token, data, retry=3):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post("https://s2.billing.ext.azure.com/api/Billing/Subscription/GetSpecsCosts?SpotPricing=true", json=data, headers={"Authorization": "Bearer " + token}) as resp:
                data = await resp.json()
                for i in data["costs"]:
                    region, size = i["id"].split(",")
                    price = i["firstParty"][0]["meters"][0]["amount"]
                    if not region in datas:
                        datas[region] = {}
                    datas[region][size] = price
    except:
        if retry == 1:
            raise
        return await get_data(token, data, retry - 1)


async def run_async(token, spec_resource_sets, specs_to_allow_zero_cost):
    return await asyncio.gather(*[
        get_data(
            token,
            {
                "subscriptionId": AZURE_SUBSCRIPTION_ID,
                "specResourceSets": spec_resource_sets[i:i + LIMIT],
                "specsToAllowZeroCost": specs_to_allow_zero_cost[i:i + LIMIT],
                "specType": "Microsoft_Azure_Compute"
            }
        ) for i in range(0, len(spec_resource_sets), LIMIT)
    ])


def lambda_handler(event, context):
    try:
        token = get_token()
        hardwaremap = get_hardwaremap()

        spec_resource_sets = []
        specs_to_allow_zero_cost = []
        for region in hardwaremap:
            data = hardwaremap[region]
            for size in data:
                resource_id = data[size]
                spec_resource_sets.append({
                    "id": f"{region},{size}",
                    "firstParty": [
                        {
                            "id": f"{region},{size}",
                            "resourceId": resource_id,
                            "quantity": 1
                        }
                    ],
                    "thirdParty": []
                })
                specs_to_allow_zero_cost.append(f"{region},{size}")

        asyncio.run(
            run_async(token, spec_resource_sets, specs_to_allow_zero_cost))

        return datas
    except Exception as e:
        requests.post(SLACK_WEBHOOK_URL, json={
                      "text": f"get_price_handler\n\n{traceback.format_exc()}"})
        return {}
