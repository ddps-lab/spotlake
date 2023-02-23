from azure.identity import AzureCliCredential
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.network import NetworkManagementClient
import os
import argparse
from uuid import uuid4
import base64
from typing import Optional
import time
import requests
import boto3
import traceback

INIT_SCRIPT = """#!/bin/bash

PORT=80
CONTENT=$(lscpu)
CONTENT_LENGTH=${#CONTENT}

(while true
do
	echo -e "HTTP/1.1 200 OK\r\nContent-Length: $CONTENT_LENGTH\r\n\r\n$CONTENT" | sudo nc -l -p $PORT -w 0 > /dev/null 2>&1
done) &"""
TIMEOUT = 3

credential = AzureCliCredential()
subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]

resource_client = ResourceManagementClient(credential, subscription_id)
network_client = NetworkManagementClient(credential, subscription_id)
compute_client = ComputeManagementClient(credential, subscription_id)


def base64_encode(content: str) -> str:
    return base64.b64encode(content.encode("utf-8")).decode("utf-8")


def create_group(group_name: str, location: str):
    resource_client.resource_groups.create_or_update(
        group_name, {"location": location})


def delete_group(group_name: str):
    poller = resource_client.resource_groups.begin_delete(group_name)
    # poller.result()


def get_status(group_name: str, name: str):
    vm_result = compute_client.virtual_machines.instance_view(group_name, name)
    if len(vm_result.statuses) < 2:
        return "Unknown"
    return vm_result.statuses[1].display_status


def create_instance(group_name: str, location: str, vm_size: str, name: str, init_script: Optional[str] = None) -> str:
    poller = network_client.virtual_networks.begin_create_or_update(group_name, f"VNET-{name}", {
        "location": location,
        "address_space": {
            "address_prefixes": ["10.0.0.0/16"]
        }
    })
    vnet_result = poller.result()

    poller = network_client.subnets.begin_create_or_update(group_name, f"VNET-{name}", f"SUBNET-{name}", {
        "address_prefix": "10.0.0.0/24"
    })
    subnet_result = poller.result()

    poller = network_client.public_ip_addresses.begin_create_or_update(group_name, f"IP-{name}", {
        "location": location,
        "sku": {
            "name": "Standard"
        },
        "public_ip_allocation_method": "Static",
        "public_ip_address_version": "IPV4"
    })
    ip_address_result = poller.result()
    ip_address = ip_address_result.ip_address

    poller = network_client.network_security_groups.begin_create_or_update(group_name, f"NSG-{name}", {
        "location": location,
        "security_rules": [{
            "name": "open-port-80",
            "properties": {
                "protocol": "*",
                "sourcePortRange": "*",
                "destinationPortRange": "80",
                "sourceAddressPrefix": "*",
                "destinationAddressPrefix": "*",
                "access": "allow",
                "priority": 900,
                "direction": "inbound"
            }
        }]
    })
    nsg_result = poller.result()

    poller = network_client.network_interfaces.begin_create_or_update(group_name, f"NIC-{name}", {
        "location": location,
        "ip_configurations": [{
            "name": f"IPCONFIG-{name}",
            "subnet": {
                "id": subnet_result.id
            },
            "public_ip_address": {
                "id": ip_address_result.id
            }
        }],
        "network_security_group": {
            "id": nsg_result.id
        }
    })
    nic_result = poller.result()

    poller = compute_client.virtual_machines.begin_create_or_update(group_name, name, {
        "location": location,
        "storage_profile": {
            "image_reference": {
                "publisher": "Canonical",
                "offer": "UbuntuServer",
                "sku": "18.04-LTS",
                "version": "latest"
            }
        },
        "hardware_profile": {
            "vm_size": vm_size  # "Standard_B1ls"
        },
        "os_profile": {
            "computer_name": name,
            "admin_username": "azureadmin",
            "admin_password": uuid4(),
            "custom_data": None if init_script is None else base64_encode(init_script)
        },
        "network_profile": {
            "network_interfaces": [{
                "id": nic_result.id
            }]
        },
    })

    return ip_address


def get_result(ip_address: str) -> str:
    result = None
    for i in range(180 // TIMEOUT):
        print("try", i + 1)
        try:
            result = requests.get(f"http://{ip_address}", timeout=TIMEOUT).text
        except requests.exceptions.Timeout:
            print("Read/Connection Timeout!")
            continue
        except Exception as e:
            print(e)
            time.sleep(TIMEOUT)
            continue
        break
    if result is None:
        raise Exception("Max retry")
    return result


parser = argparse.ArgumentParser(description="LSCPU For Azure")
parser.add_argument("--instance_name", type=str,
                    default=str(uuid4()))
parser.add_argument("--instance_type", type=str, default="Standard_B1s")
parser.add_argument("--zone", type=str, default="eastus")
args = parser.parse_args()

instance_name = args.instance_name
instance_type = args.instance_type
instance_zone = args.zone

group_name = f"{instance_zone}_{instance_type}_{instance_name}_lscpu"

print(
    f"""Instance Name: {instance_name}\nInstance Type: {instance_type}\nInstance Zone: {instance_zone}\nGroup Name: {group_name}""")

create_group(group_name, instance_zone)
try:
    ip_address = create_instance(
        group_name, instance_zone, instance_type, instance_name, INIT_SCRIPT)

    print(ip_address)

    result = get_result(ip_address)
    print(result)

    s3 = boto3.resource("s3", aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"], region_name=os.environ["AWS_REGION_NAME"])
    bucket = s3.Bucket("spotlake")
    bucket.Object(key=f"azure-cpuinfo/{instance_type}.txt").put(Body=result)
except Exception as e:
    with open(f"errors/{instance_type}.txt", "w") as f:
        f.write(traceback.format_exc())
    print(e)
finally:
    print("deleting group")
    delete_group(group_name)
