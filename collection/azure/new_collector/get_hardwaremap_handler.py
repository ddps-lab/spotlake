import boto3
import requests
import re

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('AzureHardwareMap')


def put_item(id, data):
    global table
    table.put_item(Item={'id': id, 'data': data})


def get_hardwaremap_urls():
    data = requests.get(
        "https://afd.hosting.portal.azure.net/compute/?environmentjson=true&extensionName=Microsoft_Azure_Compute&l=en&trustedAuthority=portal.azure.com").text
    tmp = re.findall(
        r"afd\.hosting\.portal\.azure\.net\/compute\/Content\/Dynamic\/[^\"]{12}\":{\"SpecPicker\/Data\/HardwareMap\.\w+\.Linux\.AzureSpot", data)

    urls = []
    for i in tmp:
        url, _, region = i.split("\"")
        url = f"https://{url}.js"
        region = region.split(".")[1]
        urls.append({"url": url, "region": region})

    return urls


def get_hardwaremap(url):
    data = requests.get(url).text
    tmp = re.findall(r"[\w_\- ]+", data.split("t.hardwareMap={")[1])

    hardwaremap = {}
    for i in range(len(tmp) // 2):
        hardwaremap[tmp[i * 2]] = tmp[i * 2 + 1]

    return hardwaremap


def lambda_handler(event, context):
    for i in get_hardwaremap_urls():
        tmp = get_hardwaremap(i["url"])
        if tmp:
            put_item(i["region"], tmp)
