import boto3
import json
from pkg_resources import resource_filename
import csv


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
                print(json.loads(response['PriceList'][0])['product']['attributes']['instanceType'])
            except:
                print("no instance in ", region, ", attributes: ", json.loads(price_info)['product']['attributes'])
                
    return response_dict


def info_to_json(response_dict, region_names, region_codes):
    file_path = "./ondemand-region-instance-price.json"

    data = []

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

                ondemandinfo_dict['Region'] = region_code
                ondemandinfo_dict['InstanceType'] = instance_type
                ondemandinfo_dict['OndemandPrice'] = instance_price

                data.append(ondemandinfo_dict)

            except:
                print("no instance in ", region_name, ", attributes: ", json.loads(price_info)['product']['attributes'])
                
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=4)                

                
if __name__ == "__main__":
    region_codes, region_names = get_region_names()
    
    session = boto3.session.Session()
    pricing_client = session.client('pricing', region_name='us-east-1')
    
    response_dict = get_ec2_info(region_names, pricing_client)
    info_to_json(response_dict, region_names, region_codes)
