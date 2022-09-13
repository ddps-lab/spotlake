import boto3
import json
import pickle
import os
import gzip
import pandas as pd
from decimal import Decimal
from datetime import datetime, timedelta

from load_metadata import get_regions


BUCKET_NAME = 'spotlake'
LOCAL_PATH = '/home/ubuntu/spot-score/collection/aws/ec2_collector'


# get spot price by all availability zone in single region
def get_spot_price_region(session: boto3.session.Session, region: str, start=None, end=None) -> tuple:
    client = session.client('ec2', region)
    describe_args = {
        'MaxResults': 300,
        'StartTime': start,
        'EndTime': end
    }
    while True:
        response = client.describe_spot_price_history(**describe_args)
        for obj in response['SpotPriceHistory']:
            az, it, os, price, timestamp = obj.values()
            # get only Linux price
            if os != 'Linux/UNIX':
                continue
            yield it, az, float(price), timestamp
        if not response['NextToken']:
            break
        describe_args['NextToken'] = response['NextToken']


# get all spot price with regions
def get_spot_price(region):
    session = boto3.session.Session()
    
    end_date = datetime.utcnow().replace(microsecond=0)
    start_date = end_date - timedelta(microseconds=1)

    spotprice_dict = {"InstanceType": [], "AvailabilityZoneId": [], "SpotPrice": []}
    
    for it, az, price, timestamp in get_spot_price_region(session, region, start_date, end_date):
        spotprice_dict["InstanceType"].append(it)
        spotprice_dict["AvailabilityZoneId"].append(az)
        spotprice_dict["SpotPrice"].append(price)
    
    spot_price_df = pd.DataFrame(spotprice_dict)

    # filter to change az-name to az-id
    az_map = dict()
    ec2 = session.client('ec2', region_name=region)
    response = ec2.describe_availability_zones()

    for val in response['AvailabilityZones']:
        az_map[val['ZoneName']] = val['ZoneId']
    
    spot_price_df = spot_price_df.replace({"AvailabilityZoneId":az_map})
    
    return spot_price_df


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

        json.loads(response['PriceList'][0])['product']['attributes']['instanceType']
                
    return response_list


# get all ondemand price with regions
def get_ondemand_price(filedate):
    DIRLIST = os.listdir(f'{LOCAL_PATH}/')
    if f"{filedate}_ondemand_price_df.pkl" in DIRLIST:
        ondemand_price_df = pickle.load(open(f"{LOCAL_PATH}/{filedate}_ondemand_price_df.pkl", 'rb'))
        return ondemand_price_df
    else:
        for filename in DIRLIST:
            if "ondemand_price_df.pkl" in filename:
                os.remove(f"{LOCAL_PATH}/{filename}")
    
    session = boto3.session.Session()
    regions = get_regions(session)
    
    session = boto3.session.Session()
    pricing_client = session.client('pricing', region_name='us-east-1')

    ondemand_dict = {"Region": [], "InstanceType": [], "OndemandPrice": []}

    for region in regions:
        for price_info in get_ondemand_price_region(region, pricing_client):
            instance_type = json.loads(price_info)['product']['attributes']['instanceType']
            instance_price = float(list(list(json.loads(price_info)['terms']['OnDemand'].values())[0]['priceDimensions'].values())[0]['pricePerUnit']['USD'])

            # case of instance-region is not available
            if instance_price == 0.0:
                continue

            ondemand_dict['Region'].append(region)
            ondemand_dict['InstanceType'].append(instance_type)
            ondemand_dict['OndemandPrice'].append(instance_price)
    
    ondemand_price_df = pd.DataFrame(ondemand_dict)

    s3_client = boto3.client('s3')
    pickle.dump(ondemand_price_df, open(f"{LOCAL_PATH}/{filedate}_ondemand_price_df.pkl", "wb"))
    gzip.open(f"{LOCAL_PATH}/{filedate}_ondemand_price_df.pkl.gz", "wb").writelines(open(f"{LOCAL_PATH}/{filedate}_ondemand_price_df.pkl", "rb"))
    s3_client.upload_fileobj(open(f"{LOCAL_PATH}/{filedate}_ondemand_price_df.pkl.gz", "rb"), BUCKET_NAME, f"rawdata/aws/ondemand_price/{'/'.join(filedate.split('-'))}/ondemand_price_df.pkl.gz")
    
    return ondemand_price_df
