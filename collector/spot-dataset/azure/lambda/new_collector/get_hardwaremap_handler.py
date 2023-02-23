import requests
import re
from const_config import AzureCollector
from util.s3 import S3
import traceback

AZURE_CONST = AzureCollector()

def get_hardwaremap_urls(retry=3):
    try:
        data = requests.get(
            AZURE_CONST.GET_HARDWAREMAP_URL).text
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
        s3 = S3("azure-hardware-map")
        hardwaremap = {}

        for i in get_hardwaremap_urls():
            tmp = get_hardwaremap(i["url"])
            if tmp:
                hardwaremap[i["region"]] = tmp

        s3.put_json("hardwaremap.json", hardwaremap)

    except Exception as e:
        requests.post(AZURE_CONST.SLACK_WEBHOOK_URL, json={
                      "text": f"get_hardwaremap_handler\n\n{traceback.format_exc()}"})
