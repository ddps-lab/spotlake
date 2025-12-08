# ------ import module ------
from datetime import datetime, timezone
import pandas as pd
import boto3.session
import os, pickle, gzip
import io
import argparse
import json
import concurrent.futures

# ------ import user module ------
# ------ import user module ------
import sys
# sys.path.append("/home/ubuntu/spotlake")
# from const_config import AwsCollector, Storage
from utility.slack_msg_sender import send_slack_message
from load_price import get_spot_price, get_regions

# get ondemand price by all instance type in single region
def get_ondemand_price_region(region, pricing_client):
    response_list = []

    filters = [
    {'Type': 'TERM_MATCH', 'Field': 'capacitystatus', 'Value': 'Used'},
    {'Type': 'TERM_MATCH', 'Field': 'regionCode', 'Value': region},
    {'Type': 'TERM_MATCH', 'Field': 'tenancy', 'Value': 'Shared'},
    {'Type': 'TERM_MATCH', 'Field': 'operatingSystem', 'Value': 'Linux'},
    {'Type': 'TERM_MATCH', 'Field': 'preInstalledSw', 'Value': 'NA'},
    {'Type': 'TERM_MATCH', 'Field': 'licenseModel', 'Value': 'No License required'}
    ]

    response = pricing_client.get_products(ServiceCode='AmazonEC2', Filters=filters) 
    response_list = response['PriceList']

    while "NextToken" in response:
        response = pricing_client.get_products(ServiceCode='AmazonEC2', Filters=filters, NextToken=response["NextToken"]) 
        response_list.extend(response['PriceList'])
                
    return response_list


# get all ondemand price with regions
def get_ondemand_price():
    session = boto3.session.Session()
    regions = get_regions(session)
    
    pricing_client = session.client('pricing', region_name='us-east-1')

    ondemand_dict = {"Region": [], "InstanceType": [], "OndemandPrice": []}

    def process_region(region):
        print(f"Collecting on-demand price for region: {region}")
        local_data = {"Region": [], "InstanceType": [], "OndemandPrice": []}
        try:
            price_infos = get_ondemand_price_region(region, pricing_client)
            for price_info in price_infos:
                instance_type = json.loads(price_info)['product']['attributes']['instanceType']
                instance_price = float(list(list(json.loads(price_info)['terms']['OnDemand'].values())[0]['priceDimensions'].values())[0]['pricePerUnit']['USD'])

                # case of instance-region is not available
                if instance_price == 0.0:
                    continue

                local_data['Region'].append(region)
                local_data['InstanceType'].append(instance_type)
                local_data['OndemandPrice'].append(instance_price)
            return local_data
        except Exception as e:
            print(f"Error collecting on-demand price for region {region}: {e}")
            return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_region = {executor.submit(process_region, region): region for region in regions}
        for future in concurrent.futures.as_completed(future_to_region):
            result = future.result()
            if result:
                ondemand_dict['Region'].extend(result['Region'])
                ondemand_dict['InstanceType'].extend(result['InstanceType'])
                ondemand_dict['OndemandPrice'].extend(result['OndemandPrice'])
    
    ondemand_price_df = pd.DataFrame(ondemand_dict)

    return ondemand_price_df

def main():
    # ------ Set Constants ------
    # Constants are now imported from const_config
    S3_PATH_PREFIX = "rawdata/aws"
    BUCKET_NAME = "spotlake-test"

    # ------ Set time data ------
    parser = argparse.ArgumentParser()
    parser.add_argument('--timestamp', dest='timestamp', action='store', help='Timestamp in format YYYY-MM-DDTHH:MM')
    args = parser.parse_args()
    
    if args.timestamp:
        # Handle EventBridge timestamp format (YYYY-MM-DDTHH:MM:SSZ)
        if args.timestamp.endswith('Z'):
            timestamp_utc = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M:%SZ")
        else:
            try:
                timestamp_utc = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                timestamp_utc = datetime.strptime(args.timestamp, "%Y-%m-%dT%H:%M")
    else:
        start_time = datetime.now(timezone.utc)
        timestamp_utc = start_time.replace(minute=((start_time.minute // 10) * 10), second=0)

    S3_DIR_NAME = timestamp_utc.strftime('%Y/%m/%d')
    S3_OBJECT_PREFIX = timestamp_utc.strftime('%H-%M')
    
    print(f"Collecting Spot Price and On-Demand Price for timestamp: {timestamp_utc}")
    start_time = datetime.now(timezone.utc)

    # ------ Collect Spot Price ------
    try:
        session = boto3.session.Session()
        regions = get_regions(session)
        spot_price_df_list = []
        spot_price_df_list = []
        
        def process_spot_price_region(region):
            print(f"Collecting price for region: {region}")
            try:
                return get_spot_price(region)
            except Exception as e:
                print(f"Error collecting price for region {region}: {e}")
                return None

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_region = {executor.submit(process_spot_price_region, region): region for region in regions}
            for future in concurrent.futures.as_completed(future_to_region):
                result = future.result()
                if result is not None:
                    spot_price_df_list.append(result)
                
        if spot_price_df_list:
            spot_price_df = pd.concat(spot_price_df_list).reset_index(drop=True)
            print(f"Collected {len(spot_price_df)} rows of Spot Price data")
        else:
            print("No spot price data collected")
            return

    except Exception as e:
        send_slack_message(f"Error during spot price collection\n{e}")
        raise e
        
    end_time = datetime.now(timezone.utc)
    print(f"Collecting Spot Price time is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")

    # ------ Collect On-Demand Price ------
    start_time = datetime.now(timezone.utc)
    try:
        print("Collecting On-Demand Price...")
        ondemand_price_df = get_ondemand_price()
        print(f"Collected {len(ondemand_price_df)} rows of On-Demand Price data")
    except Exception as e:
        send_slack_message(f"Error during on-demand price collection\n{e}")
        raise e
    end_time = datetime.now(timezone.utc)
    print(f"Collecting On-Demand Price time is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")

    # ------ Save Raw Data in S3 ------
    start_time = datetime.now(timezone.utc)
    try:
        s3 = session.client('s3')

        # Save Spot Price
        buffer = io.BytesIO()
        pickle.dump(spot_price_df, buffer)
        buffer.seek(0)

        compressed_buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=compressed_buffer, mode='wb') as gz:
            gz.write(buffer.getvalue())
        compressed_buffer.seek(0)
        
        key = f"{S3_PATH_PREFIX}/spot_price/{S3_DIR_NAME}/{S3_OBJECT_PREFIX}_spot_price.pkl.gz"
        s3.upload_fileobj(compressed_buffer, BUCKET_NAME, key)
        print(f"Uploaded Spot Price data to s3://{BUCKET_NAME}/{key}")

        # Save On-Demand Price
        buffer = io.BytesIO()
        pickle.dump(ondemand_price_df, buffer)
        buffer.seek(0)

        compressed_buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=compressed_buffer, mode='wb') as gz:
            gz.write(buffer.getvalue())
        compressed_buffer.seek(0)
        
        key = f"{S3_PATH_PREFIX}/ondemand_price/{S3_DIR_NAME}/ondemand_price.pkl.gz"
        s3.upload_fileobj(compressed_buffer, BUCKET_NAME, key)
        print(f"Uploaded On-Demand Price data to s3://{BUCKET_NAME}/{key}")
        
    except Exception as e:
        send_slack_message(f"Store spot price data to be stored in the cloud in memory\n{e}")
        raise e
    
    end_time = datetime.now(timezone.utc)
    print(f"Upload time is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")

if __name__ == "__main__":
    start_time = datetime.now(timezone.utc)
    main()
    end_time = datetime.now(timezone.utc)
    print(f"Running time is {(end_time - start_time).total_seconds() * 1000 / 60000:.2f} min")
