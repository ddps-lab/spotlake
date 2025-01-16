# ------ import module ------
import boto3.session
import json
import pandas as pd


# get all available regions
def get_regions(session: boto3.session.Session, region='us-east-1') -> list:
    client = session.client('ec2', region_name=region)
    describe_args = {
        'AllRegions': False
    }
    return [region['RegionName'] for region in client.describe_regions(**describe_args)['Regions']]


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

    return ondemand_price_df
