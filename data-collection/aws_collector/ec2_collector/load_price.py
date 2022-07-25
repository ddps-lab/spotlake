import boto3
import json
import time
import pandas as pd
from decimal import Decimal
from pkg_resources import resource_filename
from datetime import datetime, timedelta


# get spot price by all instance type in single region
def get_spot_price(session: boto3.session.Session, region: str, start=None, end=None) -> tuple:
    if type(end) is not datetime:
        end = datetime.utcnow()
    if type(start) is not datetime:
        start = end - timedelta(hours=1)
    
    client = session.client('ec2', region)
    describe_args = {
        'MaxResults': 300,
        'EndTime': end,
        'StartTime': start
    }
    while True:
        response = client.describe_spot_price_history(**describe_args)
        for obj in response['SpotPriceHistory']:
            az, it, os, price, timestamp = obj.values()
            yield it, az, os, Decimal(price), timestamp
        if not response['NextToken']:
            break
        describe_args['NextToken'] = response['NextToken']


# preprocess result data of get_spot_price to dataframe
def preprocess_spot_price(params: tuple):
    session, region, start, end = params
    spot_price_by_region = pd.DataFrame({"Region" : [], "InstanceType" : [], "AZ" : [], "SpotPrice" : [], "TimeStamp" : []})
    for it, az, os, price, timestamp in get_spot_price(session, region, start, end):
        # use only Linux price
        if os != 'Linux/UNIX':
            continue
        # if price info is not in dataframe, append info
        if len(spot_price_by_region[(spot_price_by_region['Region'] == region) & (spot_price_by_region['InstanceType'] == it) & (spot_price_by_region['AZ'] == az)]) == 0:
            spot_price_by_region = spot_price_by_region.append(pd.DataFrame({"Region":[region],"InstanceType":[it],"AZ":[az],"SpotPrice":[price],"TimeStamp":[timestamp]}), 
                              ignore_index=True)
        # or price info is in dataframe, write latest info
        elif spot_price_by_region[(spot_price_by_region['Region'] == region) & (spot_price_by_region['InstanceType'] == it) & (spot_price_by_region['AZ'] == az)]['TimeStamp'] < timestamp:
            spot_price_by_region[(spot_price_by_region['Region'] == region) & (spot_price_by_region['InstanceType'] == it) & (spot_price_by_region['AZ'] == az)]['SpotPrice'] = price

    return spot_price_by_region


# get all regions in aws to use load_spot_price
def get_regions(session: boto3.session.Session, region='us-east-1') -> list:
    client = session.client('ec2', region_name=region)
    describe_args = {
        'AllRegions': False
    }
    return [region['RegionName'] for region in client.describe_regions(**describe_args)['Regions']]


# get all spot price by azs
def load_spot_price():
    session = boto3.session.Session()
    
    regions = get_regions(session)
    
    end_date = datetime.utcnow().replace(microsecond=0)
    start_date = end_date - timedelta(microseconds=1)
    spot_price = pd.DataFrame()
    
    args = [(session, region, start_date, end_date) for region in regions]

    for arg in args:
        buffer = preprocess_spot_price(arg)
        spot_price = pd.concat([spot_price, buffer])

    # filter to change az name to id
    az_map = dict()
    for region in regions:
        print(region)
        ec2 = session.client('ec2', region_name=region)
        response = ec2.describe_availability_zones()

        for val in response['AvailabilityZones']:
            az_map[val['ZoneName']] = val['ZoneId']
        time.sleep(1)
    
    spot_price = spot_price.replace({"AZ":az_map})
    
    return spot_price


# get region names and codes
def get_region_names():
    endpoint_file = resource_filename('botocore', 'data/endpoints.json') # get data file from botocore module

    with open(endpoint_file, 'r') as f:
        endpoint_data = json.load(f)

    region_codes = list(endpoint_data['partitions'][0]['regions'].keys())
    region_names = []
    for region_name in list(endpoint_data['partitions'][0]['regions'].values()):
        region_name = region_name['description']
        if 'Europe' in region_name:
            region_name = region_name.replace('Europe', 'EU') 
        region_names.append(region_name)
    
    return region_codes, region_names


# get ondemand price by all instance type in single region
def get_ec2_info(region_names, pricing_client):

    response_dict = dict()

    for region in region_names:
        filters = [
        {'Type': 'TERM_MATCH', 'Field': 'capacitystatus', 'Value': 'Used'},
        {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': region},
        {'Type': 'TERM_MATCH', 'Field': 'tenancy', 'Value': 'Shared'},
        {'Type': 'TERM_MATCH', 'Field': 'operatingSystem', 'Value': 'Linux'},
        {'Type': 'TERM_MATCH', 'Field': 'preInstalledSw', 'Value': 'NA'},
        {'Type': 'TERM_MATCH', 'Field': 'licenseModel', 'Value': 'No License required'}
        ]

        response = pricing_client.get_products(ServiceCode='AmazonEC2', Filters=filters) 
        response_dict[region] = response['PriceList']

        while "NextToken" in response:
            response = pricing_client.get_products(ServiceCode='AmazonEC2', Filters=filters, NextToken=response["NextToken"]) 
            response_dict[region].extend(response['PriceList'])

            try:
                json.loads(response['PriceList'][0])['product']['attributes']['instanceType']
            except:
                continue
                
    return response_dict


# get all ondemand price by region-instance
def load_ondemand_price():
    region_codes, region_names = get_region_names()
    
    session = boto3.session.Session()
    pricing_client = session.client('pricing', region_name='us-east-1')
    
    response_dict = get_ec2_info(region_names, pricing_client)

    ondemand_price = pd.DataFrame()

    for i in range(len(region_names)):
        region_name = region_names[i]
        region_code = region_codes[i]

        for price_info in response_dict[region_name]: 
            try:
                ondemandinfo_dict = dict()
                instance_type = json.loads(price_info)['product']['attributes']['instanceType']
                instance_price = float(list(list(json.loads(price_info)['terms']['OnDemand'].values())[0]['priceDimensions'].values())[0]['pricePerUnit']['USD'])

                if instance_price == 0.0:
                    continue

                ondemandinfo_dict['Region'] = [region_code]
                ondemandinfo_dict['InstanceType'] = [instance_type]
                ondemandinfo_dict['OndemandPrice'] = [instance_price]

                ondemand_price = pd.concat([ondemand_price, pd.DataFrame(ondemandinfo_dict)])

            except:
                continue
                
    return ondemand_price