import requests
from util.s3 import S3
from util.auth import get_token
import datetime
import os
import traceback

SLACK_WEBHOOK_URL = ""


def put_json(s3, data):
    hardware = {}
    for i in data["value"]:
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


def none_to_str(data):
    for i in range(len(data)):
        if data[i] is None:
            data[i] = "None"
    return data


def put_csv(s3, data):
    csv = [",".join(["region", "instance_type", "cpu",
                    "mem", "iops", "family", "gpu"])]
    for i in data["value"]:
        if i["resourceType"] != "virtualMachines":
            continue
        tmp = {}
        for j in i["capabilities"]:
            tmp[j["name"]] = j["value"]

        cpus, mem, iops, family, gpus = tmp.get("vCPUs"), tmp.get(
            "MemoryGB"), tmp.get("UncachedDiskIOPS"), i["family"], tmp.get("GPUs")

        for region in i["locations"]:
            region = region.lower()
            instance_type = i["name"].lower()
            csv.append(
                ",".join(none_to_str([region, instance_type, cpus, mem, iops, family, gpus])))

    csv = "\n".join(csv)
    s3.put("azure/" + str(datetime.datetime.utcnow()) + ".csv", csv)


def lambda_handler(event, context):
    try:
        s3 = S3("hardware-feature")
        token = get_token()
        resp = requests.get(
            f"https://management.azure.com/subscriptions/{os.environ['AZURE_SUBSCRIPTION_ID']}/providers/Microsoft.Compute/skus?api-version=2021-01-01&includeExtendedLocations=true",
            headers={"Authorization": "Bearer " + token}
        ).json()

        # put_json(s3, resp)
        put_csv(s3, resp)

    except:
        requests.post(SLACK_WEBHOOK_URL, json={
                      "text": f"get_hardwaremap_handler\n\n{traceback.format_exc()}"})
