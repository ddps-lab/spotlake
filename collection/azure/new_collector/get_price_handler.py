import boto3
import aiohttp
import json
import asyncio

AZURE_SUBSCRIPTION_ID = "862ad8bc-ae09-4e2e-ba9c-6d073e706f58"
LIMIT = 2000

lambda_client = boto3.client('lambda')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('AzureHardwareMap')

datas = {}


def get_token():
    token = lambda_client.invoke(
        FunctionName='AzureAuth', InvocationType='RequestResponse')["Payload"].read()
    return json.loads(token.decode("utf-8"))


def get_all_item():
    global table
    return table.scan()["Items"]


def get_hardwaremap():
    hardwaremap = {}
    for i in get_all_item():
        hardwaremap[i["id"]] = i["data"]
    return hardwaremap


async def get_data(token, data, retry=3):
    if retry == 0:
        raise Exception("Max retry")
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
        return get_data(token, data, retry - 1)


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
