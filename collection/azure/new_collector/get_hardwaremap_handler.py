import requests
import re
from util.dynamodb import DynamoDB


def get_hardwaremap_urls(retry=3):
    try:
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
    except:
        if retry == 1:
            raise
        return get_hardwaremap_urls(retry - 1)


def get_hardwaremap(url, retry=3):
    try:
        data = requests.get(url).text
        tmp = re.findall(r"[\w_\- ]+", data.split("t.hardwareMap={")[1])

        hardwaremap = {}
        for i in range(len(tmp) // 2):
            hardwaremap[tmp[i * 2]] = tmp[i * 2 + 1]

        return hardwaremap
    except:
        if retry == 1:
            raise
        return get_hardwaremap(url, retry - 1)


def lambda_handler(event, context):
    try:
        db = DynamoDB("AzureHardwareMap")

        for i in get_hardwaremap_urls():
            tmp = get_hardwaremap(i["url"])
            if tmp:
                db.put_item(i["region"], tmp)
    except Exception as e:
        pass
