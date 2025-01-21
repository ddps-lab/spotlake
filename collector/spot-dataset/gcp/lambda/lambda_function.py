import os
import time
import boto3
import requests
import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2 import service_account
from google.cloud import compute_v1
from utility.slack_msg_sender import send_slack_message
from datetime import datetime, timezone
from s3_management import save_raw, update_latest, upload_timestream, update_query_selector
from compare_data import compare
from const_config import GcpCollector, Storage
import json
import botocore

STORAGE_CONST = Storage()
GCP_CONST = GcpCollector()

# Path to the service account JSON file
SERVICE_ACCOUNT_FILE = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')

# URLs to call
urls = {
    'v2beta/skus': {
        'BASE_URL': 'https://cloudbilling.googleapis.com/v2beta/skus',
        'QUERY_STRING': '?pageSize=5000&filter=service="services/6F81-5844-456A"',
    },
    'v1beta/skus': {
        'BASE_URL': 'https://cloudbilling.googleapis.com/v1beta/skus',
        'QUERY_STRING': '/prices?pageSize=5000&currencyCode=USD',
    },
}

def get_url(version, sku_id=None):
    if version == 'v2beta/skus':
        return urls[version]['BASE_URL'] + urls[version]['QUERY_STRING']
    elif version == 'v1beta/skus':
        return urls[version]['BASE_URL'] + '/' + sku_id + urls[version]['QUERY_STRING']
    else:
        return None

# Get authentication token
def get_access_token():
    try:
        credentials = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        credentials.refresh(Request())
        return credentials.token
    except Exception as e:
        send_slack_message(f"[GCP Collector]\nError in get_access_token: {str(e)}")
        raise

# str1: GPU model name string (e.g. nvidia-h100-mega-80gb)
# str2: List of GPU model names to compare similarity with (e.g. ['A100', 'H100', 'L4'])
def jaccard_similarity(str1, str2):
    max_similarity = 0
    max_str = str1
    str1_tokened = str1.split("-")
    check = False
    new_str1 = []
    for token in str1_tokened:
        if any(char.isdigit() for char in token):
            check = True
        if check:
            new_str1.append(token)
    new_str1 = "".join(new_str1).upper()
    for str in str2:
        set1, set2 = set(new_str1), set(str)
        intersection = set1 & set2
        union = set1 | set2
        similarity = len(intersection) / len(union)
        if similarity > max_similarity and new_str1[0:2] == str[0:2]:
            max_similarity = similarity
            max_str = str
    return max_str

def call_api(version=None, sku_id=None, page_token=None):
    token = get_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    try:
        response = requests.get(
            get_url(version, sku_id), headers=headers, params={'pageToken': page_token}, timeout=60
        )
        if response.status_code == 200:
            return response.json()
        else:
            error_msg = f"API call failed: {response.status_code}, {response.text}"
            error_msg = f"[GCP Collector]\n{error_msg}"
            send_slack_message(error_msg)
            raise Exception(error_msg)
    except requests.exceptions.RequestException as e:
        error_msg = f"Network error: {e}"
        error_msg = f"[GCP Collector]\n{error_msg}"
        send_slack_message(error_msg)
        raise

# Get SKU information
def get_sku_infos(response):
    try:
        skus = response['skus']
        sku_infos = []
        gpu_sku_infos = []
        for sku in skus:
            info_type = None
            if (len(sku['productTaxonomy']['taxonomyCategories']) == 6 and
                sku['productTaxonomy']['taxonomyCategories'][0]['category'] == 'GCP' and 
                sku['productTaxonomy']['taxonomyCategories'][1]['category'] == 'Compute' and 
                sku['productTaxonomy']['taxonomyCategories'][2]['category'] == 'GCE' and 
                (sku['productTaxonomy']['taxonomyCategories'][3]['category'] == 'VMs Preemptible' or sku['productTaxonomy']['taxonomyCategories'][3]['category'] == 'VMs On Demand') and 
                (sku['productTaxonomy']['taxonomyCategories'][4]['category'] == 'Memory: Per GB' or sku['productTaxonomy']['taxonomyCategories'][4]['category'] == 'Cores: Per Core' or sku['productTaxonomy']['taxonomyCategories'][4]['category'] == 'Cores: 1 to 64') and
                'Custom' not in sku['displayName'] and
                'Sole Tenancy' not in sku['displayName'] and
                sku['productTaxonomy']['taxonomyCategories'][5]['category'] != 'Cross VM'):
                info_type = "VMs"
            elif (len(sku['productTaxonomy']['taxonomyCategories']) == 5 and
                sku['productTaxonomy']['taxonomyCategories'][0]['category'] == 'GCP' and 
                sku['productTaxonomy']['taxonomyCategories'][1]['category'] == 'Compute' and 
                sku['productTaxonomy']['taxonomyCategories'][2]['category'] == 'GPUs' and 
                (sku['productTaxonomy']['taxonomyCategories'][3]['category'] == 'GPUs Preemptible' or sku['productTaxonomy']['taxonomyCategories'][3]['category'] == 'GPUs On Demand')):
                info_type = "GPUs"
            elif (len(sku['productTaxonomy']['taxonomyCategories']) == 6 and
                sku['productTaxonomy']['taxonomyCategories'][0]['category'] == 'GCP' and 
                sku['productTaxonomy']['taxonomyCategories'][1]['category'] == 'Compute' and 
                sku['productTaxonomy']['taxonomyCategories'][2]['category'] == 'GPUs' and 
                (sku['productTaxonomy']['taxonomyCategories'][3]['category'] == 'GPUs Preemptible' or sku['productTaxonomy']['taxonomyCategories'][3]['category'] == 'GPUs On Demand')):
                info_type = "GPUs_with_Core_and_Memory"
            else:
                continue
            if info_type == "VMs":
                machine_family = sku['productTaxonomy']['taxonomyCategories'][5]['category']
                priceResource = sku['productTaxonomy']['taxonomyCategories'][4]['category']
                if priceResource == 'Cores: 1 to 64':
                    machine_family = 'A2'
                priceResource = priceResource.split(":")[0]
                machine_model = "Standard" if 'Custom' not in sku['displayName'] else "Custom"
                price_model = "On-demand" if sku['productTaxonomy']['taxonomyCategories'][3]['category'] == 'VMs On Demand' else "Preemptible"
                if sku['geoTaxonomy']['type'] == 'TYPE_REGIONAL':
                    region = sku['geoTaxonomy']['regionalMetadata']['region']['region']
                    sku_infos.append({
                        'skuId': sku['skuId'],
                        'machineFamily': machine_family,
                        'machineModel': machine_model,
                        'region': region,
                        'priceModel': price_model,
                        'priceResource': priceResource,
                        'displayName': sku['displayName']
                    })
                elif sku['geoTaxonomy']['type'] == 'TYPE_MULTI_REGIONAL':
                    regions = sku['geoTaxonomy']['multiRegionalMetadata']['regions']
                    for region in regions:
                        sku_infos.append({
                            'skuId': sku['skuId'],
                            'machineFamily': machine_family,
                            'machineModel': machine_model,
                            'region': region['region'],
                            'priceModel': price_model,
                            'priceResource': priceResource,
                            'displayName': sku['displayName']
                        })
                else:
                    continue
            elif info_type == "GPUs":
                gpu_type = sku['productTaxonomy']['taxonomyCategories'][4]['category']
                price_model = "On-demand" if sku['productTaxonomy']['taxonomyCategories'][3]['category'] == 'GPUs On Demand' else "Preemptible"
                if sku['geoTaxonomy']['type'] == 'TYPE_REGIONAL':
                    region = sku['geoTaxonomy']['regionalMetadata']['region']['region']
                    gpu_sku_infos.append({
                        'skuId': sku['skuId'],
                        'gpuType': gpu_type,
                        'region': region,
                        'priceModel': price_model,
                        'priceResource': 'GPU',
                        'displayName': sku['displayName']
                    })
                elif sku['geoTaxonomy']['type'] == 'TYPE_MULTI_REGIONAL':
                    regions = sku['geoTaxonomy']['multiRegionalMetadata']['regions']
                    for region in regions:
                        gpu_sku_infos.append({
                            'skuId': sku['skuId'],
                            'gpuType': gpu_type,
                            'region': region['region'],
                            'priceModel': price_model,
                            'priceResource': 'GPU',
                            'displayName': sku['displayName']
                        })
                else:
                    continue
            elif info_type == "GPUs_with_Core_and_Memory":
                gpu_type = sku['productTaxonomy']['taxonomyCategories'][4]['category']
                price_model = "On-demand" if sku['productTaxonomy']['taxonomyCategories'][3]['category'] == 'GPUs On Demand' else "Preemptible"
                price_resource = sku['productTaxonomy']['taxonomyCategories'][5]['category'].split(":")[0]
                if sku['geoTaxonomy']['type'] == 'TYPE_REGIONAL':
                    region = sku['geoTaxonomy']['regionalMetadata']['region']['region']
                    gpu_sku_infos.append({
                        'skuId': sku['skuId'],
                        'gpuType': gpu_type,
                        'priceModel': price_model,
                        'priceResource': price_resource,
                        'region': region,
                        'displayName': sku['displayName']
                    })
                elif sku['geoTaxonomy']['type'] == 'TYPE_MULTI_REGIONAL':
                    regions = sku['geoTaxonomy']['multiRegionalMetadata']['regions']
                    for region in regions:
                        gpu_sku_infos.append({
                            'skuId': sku['skuId'],
                            'gpuType': gpu_type,
                            'priceModel': price_model,
                            'priceResource': price_resource,
                            'region': region['region'],
                            'displayName': sku['displayName']
                        })
                else:
                    continue
        return sku_infos, gpu_sku_infos
    except KeyError as e:
        error_msg = f"[GCP Collector]\nKeyError in get_sku_infos: {str(e)}\nSKU data structure: {json.dumps(response, indent=2)}"
        send_slack_message(error_msg)
        raise

def get_price_infos(response, sku_ids, gpu_sku_ids):
    try:
        prices = response['prices']
        price_infos = []
        gpu_price_infos = []
        for price in prices:
            sku_id = price['name'].split('/')[1]
            if sku_id in sku_ids:
                price_value = None
                try:
                    price_value = int(price['rate']['tiers'][0]['listPrice']['units']) + price['rate']['tiers'][0]['listPrice']['nanos'] * 0.000000001
                except:
                    price_value = price['rate']['tiers'][0]['listPrice']['nanos'] * 0.000000001
                price_infos.append({
                    'skuId': sku_id,
                    'currencyCode': price['currencyCode'],
                    'price': price_value,
                    'unit': price['rate']['unitInfo']['unit'],
                    'unitQuantity': price['rate']['unitInfo']['unitQuantity']['value'],
                })
                sku_ids.remove(sku_id)
            elif sku_id in gpu_sku_ids:
                price_value = None
                try:
                    try:
                        price_value = int(price['rate']['tiers'][0]['listPrice']['units']) + price['rate']['tiers'][0]['listPrice']['nanos'] * 0.000000001
                    except:
                        price_value = price['rate']['tiers'][0]['listPrice']['nanos'] * 0.000000001
                except:
                    price_value = None
                gpu_price_infos.append({
                    'skuId': sku_id,
                    'currencyCode': price['currencyCode'],
                    'price': price_value,
                    'unit': price['rate']['unitInfo']['unit'],
                    'unitQuantity': price['rate']['unitInfo']['unitQuantity']['value'],
                })
                gpu_sku_ids.remove(sku_id)
        return price_infos, gpu_price_infos
    except KeyError as e:
        send_slack_message(f"[GCP Collector]\nKeyError in get_price_infos: {str(e)}")
        raise

def list_regions_and_machine_types(gpu_families):
    try:
        # Create Compute Engine API client (using JSON key file)
        client = compute_v1.RegionsClient.from_service_account_file(SERVICE_ACCOUNT_FILE)
        machine_types_client = compute_v1.MachineTypesClient.from_service_account_file(SERVICE_ACCOUNT_FILE)
        
        # Get project ID (read from JSON file)
        with open(SERVICE_ACCOUNT_FILE, 'r') as f:
            import json
            project_id = json.load(f)['project_id']
        
        # Get all regions
        regions = client.list(project=project_id)
        
        # Save results
        region_machine_types = []

        finded_region_machine_types = set()

        # Get machine types for each region
        for region in regions:
            zone_list = list_zones_in_region(region.name, project_id)
            
            for zone in zone_list:
                machine_types = machine_types_client.list(project=project_id, zone=zone)
                for machine_type in machine_types:
                    if (region.name, machine_type.name) in finded_region_machine_types:
                        continue
                    finded_region_machine_types.add((region.name, machine_type.name))
                    gpu_count = 0
                    gpu_type = None
                    if "accelerators" in machine_type:
                        gpu_count = machine_type.accelerators[0].guest_accelerator_count
                        gpu_type = jaccard_similarity(machine_type.accelerators[0].guest_accelerator_type, gpu_families)
                    region_machine_types.append({
                        "machineFamily": machine_type.name.split('-')[0].upper(),
                        "machineType": machine_type.name,
                        "region": region.name,
                        "vcpus": machine_type.guest_cpus,
                        "memory": machine_type.memory_mb / 1024,  # Convert MB to GB
                        "gpuCount": gpu_count,
                        "gpuType": gpu_type,
                    })
        
        return region_machine_types
    except Exception as e:
        send_slack_message(f"[GCP Collector]\nError in list_regions_and_machine_types: {str(e)}")
        raise

def list_zones_in_region(region_name, project_id):
    try:
        """
        Get all available zones in the given region
        """
        zones_client = compute_v1.ZonesClient.from_service_account_file(SERVICE_ACCOUNT_FILE)
        zones = zones_client.list(project=project_id)
        
        return [
            zone.name for zone in zones 
            if zone.name.startswith(region_name)
        ]
    except Exception as e:
        send_slack_message(f"[GCP Collector]\nError in list_zones_in_region: {str(e)}")
        raise

# Define price calculation function
def calculate_price(row, cores_key, memory_key, gpu_key):
    cores_price = row[cores_key]
    memory_price = row[memory_key]
    gpu_price = row[gpu_key]
    if pd.isna(cores_price) and pd.isna(memory_price):
        return None  # Remove if both price information is missing
    if pd.isna(cores_price):
        return None  # Remove if only core price is missing
    if pd.isna(memory_price):
        memory_price = 0  # Treat as 0 if only memory price is missing
    if pd.isna(gpu_price):
        gpu_price = 0  # Treat as 0 if only GPU price is missing
    return max(row["vcpus"], 1) * cores_price + row["memory"] * memory_price + row["gpuCount"] * gpu_price

def upload_cloudwatch(df_current, timestamp):
    ondemand_count = len(df_current.drop(columns=['Spot Price', 'Savings']).dropna())
    spot_count = len(df_current.drop(columns=['OnDemand Price', 'Savings']).dropna())
    
    cw_client = boto3.client('logs')

    log_event = {
        'timestamp': int(timestamp.timestamp()) * 1000,
        'message': f'GCPONDEMAND: {ondemand_count} GCPSPOT: {spot_count}'
    }

    cw_client.put_log_events(
        logGroupName=GCP_CONST.SPOT_DATA_COLLECTION_LOG_GROUP_NAME,
        logStreamName=GCP_CONST.LOG_STREAM_NAME, 
        logEvents=[log_event]
    )

def lambda_handler(event, context):
    try:
        start_time = time.time()
        str_datetime = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M")
        timestamp = datetime.strptime(str_datetime, "%Y-%m-%dT%H:%M")
        
        response = call_api(version='v2beta/skus')
        sku_infos, gpu_sku_infos = get_sku_infos(response)
        while 'nextPageToken' in response:
            response = call_api(version='v2beta/skus', page_token=response['nextPageToken'])
            new_sku_infos, new_gpu_sku_infos = get_sku_infos(response)
            sku_infos += new_sku_infos
            gpu_sku_infos += new_gpu_sku_infos
        print("Complete to get sku_infos")

        sku_df = pd.DataFrame(sku_infos).sort_values(by=["machineFamily", "region", "priceModel", "priceResource"], ascending=True).reset_index(drop=True)

        gpu_sku_df = pd.DataFrame(gpu_sku_infos).sort_values(by=["gpuType", "region", "priceModel", "priceResource"], ascending=True).reset_index(drop=True)

        sku_ids = set([sku_info['skuId'] for sku_info in sku_infos])
        gpu_sku_ids = set([gpu_sku_info['skuId'] for gpu_sku_info in gpu_sku_infos])

        response = call_api(version='v1beta/skus', sku_id='-')
        price_infos, gpu_price_infos = get_price_infos(response, sku_ids, gpu_sku_ids)
        while 'nextPageToken' in response:
            response = call_api(version='v1beta/skus', sku_id='-', page_token=response['nextPageToken'])
            new_price_infos, new_gpu_price_infos = get_price_infos(response, sku_ids, gpu_sku_ids)
            price_infos += new_price_infos
            gpu_price_infos += new_gpu_price_infos
        print("Complete to get price_infos")

        price_df = pd.DataFrame(price_infos)

        gpu_price_df = pd.DataFrame(gpu_price_infos)

        total_df = pd.merge(sku_df, price_df, on='skuId', how='left')

        gpu_df = pd.merge(gpu_sku_df, gpu_price_df, on='skuId', how='left')

        print("Complete to get total_infos")

        machine_types_infos = list_regions_and_machine_types(list(gpu_df['gpuType'].unique()))

        machine_types_df = pd.DataFrame(machine_types_infos).sort_values(by=["machineFamily", "machineType", "region", "vcpus", "memory"], ascending=True).reset_index(drop=True)
        machine_types_df['machineModel'] = 'Standard'

        # DataFrame transformation code
        reshaped_df = (
            total_df.pivot_table(
                index=["machineFamily", "machineModel", "region"],
                columns=["priceModel", "priceResource"],
                values="price",
                aggfunc="first"
            )
            .reset_index()
        )

        # Rename columns
        reshaped_df.columns = [
            "machineFamily", "machineModel", "region",
            "ondemandCorePrice", "ondemandMemoryPrice",
            "preemptibleCorePrice", "preemptibleMemoryPrice"
        ]

        # DataFrame transformation code
        gpu_reshaped_df = (
            gpu_df.pivot_table(
                index=["gpuType", "region"],
                columns=["priceModel", "priceResource"],
                values="price",
                aggfunc="first"
            )
            .reset_index()
        )

        # Rename columns
        gpu_reshaped_df.columns = [
            "gpuType", "region",
            "ondemandCorePrice", "ondemandGPUPrice", "ondemandMemoryPrice",
            "preemptibleCorePrice", "preemptibleGPUPrice", "preemptibleMemoryPrice"
        ]

        # Merge DataFrame 1 and 2 (based on machineFamily, machineModel, region)
        df_4 = pd.merge(
            machine_types_df, reshaped_df,
            on=["machineFamily", "machineModel", "region"],
            how="left"
        )

        # Merge DataFrame 4 and 3 (based on gpuType, region)
        # Update only if existing ondemandCorePrice, ondemandMemoryPrice, preemptibleCorePrice, preemptibleMemoryPrice values are NaN
        df_final = pd.merge(
            df_4, gpu_reshaped_df,
            on=["gpuType", "region"],
            how="left",
            suffixes=('', '_new')  # Add suffix '_new' to new values
        )

        # Update necessary columns (only if NaN values exist)
        for col in ["ondemandCorePrice", "ondemandMemoryPrice", "preemptibleCorePrice", "preemptibleMemoryPrice"]:
            df_final[col] = df_final[col].fillna(df_final[f"{col}_new"])

        # Remove temporary columns with '_new' suffix
        df_final = df_final.drop(columns=[f"{col}_new" for col in ["ondemandCorePrice", "ondemandMemoryPrice", "preemptibleCorePrice", "preemptibleMemoryPrice"]])

        df_final['ondemandPrice'] = df_final.apply(lambda row: calculate_price(row, "ondemandCorePrice", "ondemandMemoryPrice", "ondemandGPUPrice"), axis=1)
        df_final['preemptiblePrice'] = df_final.apply(lambda row: calculate_price(row, "preemptibleCorePrice", "preemptibleMemoryPrice", "preemptibleGPUPrice"), axis=1)

        # Construct final DataFrame
        df_final['Time'] = timestamp.strftime("%Y-%m-%d %H:%M:%S")
        df_final['Savings'] = ((df_final['ondemandPrice'] - df_final['preemptiblePrice']) / df_final['ondemandPrice']) * 100
        df_final = df_final[['Time', 'machineType', 'region', 'ondemandPrice', 'preemptiblePrice', 'Savings']]

        # Change column names to match CSV format
        df_final.columns = ['Time', 'InstanceType', 'Region', 'OnDemand Price', 'Spot Price', 'Savings']

        # Remove rows with NaN values from df_final
        df_final = df_final.dropna(how='any')

        # Upload data to CloudWatch
        upload_cloudwatch(df_final, timestamp)

        # Update latest data in S3
        update_latest(df_final, timestamp)

        # Save raw data to S3
        save_raw(df_final, timestamp)

        # Compare with previous data
        s3 = boto3.resource('s3')
        try:
            obj = s3.Object(STORAGE_CONST.BUCKET_NAME, GCP_CONST.S3_LATEST_DATA_SAVE_PATH)
            response = obj.get()
            data = json.load(response['Body'])
            df_previous = pd.DataFrame(data).dropna(how='any')
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                df_previous = pd.DataFrame()
            else:
                raise

        workload_cols = ['InstanceType', 'Region']
        feature_cols = ['OnDemand Price', 'Spot Price']

        changed_df, removed_df = compare(df_previous, df_final, workload_cols, feature_cols)

        # Update changed data
        update_query_selector(changed_df)

        # Upload data to Timestream
        upload_timestream(changed_df, timestamp)
        upload_timestream(removed_df, timestamp)

        end_time = time.time()
        print(f"Total time taken: {end_time - start_time} seconds")
    except Exception as e:
        send_slack_message(f"[GCP Collector]\nUnhandled exception in main: {str(e)}")
        raise

if __name__ == "__main__":
    lambda_handler({}, {})
