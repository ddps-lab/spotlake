import requests
from util.s3 import S3
from util.auth import get_token
import datetime
import os
import traceback

SLACK_WEBHOOK_URL = ""


def save_as_json(s3, data):
    s3.put_json("azure/hardware_feature.json", data)


def none_to_str(data):
    return "None" if data is None else data


def save_as_csv(s3, data):
    keys = [*data[[*data.keys()][0]].keys()]

    csv = [",".join(["instance_type", *keys])]
    for instance_type in data:
        tmp = [instance_type]
        for key in keys:
            tmp.append(none_to_str(data[instance_type][key]))
        csv.append(",".join(tmp))

    csv = "\n".join(csv)
    s3.put("azure/hardware_feature.csv", csv)


def lambda_handler(event, context):
    try:
        s3 = S3("hardware-feature")
        token = get_token()
        resp = requests.get(
            f"https://management.azure.com/subscriptions/{os.environ['AZURE_SUBSCRIPTION_ID']}/providers/Microsoft.Compute/skus?api-version=2021-01-01&includeExtendedLocations=true",
            headers={"Authorization": "Bearer " + token}
        ).json()

        data = {}
        for i in resp["value"]:
            if i["resourceType"] != "virtualMachines":
                continue
            tmp = {}
            for j in i["capabilities"]:
                tmp[j["name"]] = j["value"]

            cpus, mem, iops, family, gpus = tmp.get("vCPUs"), tmp.get(
                "MemoryGB"), tmp.get("UncachedDiskIOPS"), i["family"], tmp.get("GPUs")
            instance_type = i["name"].lower()

            data[instance_type] = {
                "cpu": cpus, "mem": mem, "iops": iops, "family": family, "gpu": gpus
            }

        save_as_json(s3, data)
        save_as_csv(s3, data)

    except:
        requests.post(SLACK_WEBHOOK_URL, json={
                      "text": f"get_hardwaremap_handler\n\n{traceback.format_exc()}"})
