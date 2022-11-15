import requests
from util.s3 import S3
from util.auth import get_token
import datetime
import os
import traceback

SLACK_WEBHOOK_URL = ""


def lambda_handler(event, context):
    try:
        s3 = S3("hardware-feature")
        hardware = {}
        token = get_token()
        resp = requests.get(
            f"https://management.azure.com/subscriptions/{os.environ['AZURE_SUBSCRIPTION_ID']}/providers/Microsoft.Compute/skus?api-version=2021-01-01&includeExtendedLocations=true",
            headers={"Authorization": "Bearer " + token}
        ).json()
        for i in resp["value"]:
            if i["resourceType"] != "virtualMachines":
                continue
            tmp = {}
            for j in i["capabilities"]:
                tmp[j["name"]] = j["value"]

            cpus, mem, iops, family, gpus = tmp.get("vCPUs"), tmp.get(
                "MemoryGB"), tmp.get("UncachedDiskIOPS"), i["family"], tmp.get("GPUs")

            for location in i["locations"]:
                location = location.lower()
                if not location in hardware:
                    hardware[location] = {}
                hardware[location][i["name"].lower()] = {
                    "cpu": cpus, "mem": mem, "iops": iops, "family": family, "gpu": gpus
                }
        s3.put_json("azure/" + str(datetime.datetime.utcnow()) +
                    ".json", hardware)
    except:
        requests.post(SLACK_WEBHOOK_URL, json={
                      "text": f"get_hardwaremap_handler\n\n{traceback.format_exc()}"})
