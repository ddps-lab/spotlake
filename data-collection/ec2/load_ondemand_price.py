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
            region_name = region_name.replace('Europe', 'EU') # 형식 맞추기
        region_names.append(region_name)
    
    return region_codes, region_names
  
region_codes, region_names = get_region_names()

session = boto3.session.Session()
pricing_client = session.client('pricing', region_name='us-east-1')

response_dict = dict()

for region in region_names:
    filters = [
      {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': region},
    # {'Type': 'TERM_MATCH', 'Field': 'terms', 'Value': 'OnDemand'},
    # {'Type': 'TERM_MATCH', 'Field': 'capacitystatus', 'Value': capacity_status},    
    # {'Type': 'TERM_MATCH', 'Field': 'instanceType', 'Value': instance_type},
    # {'Type': 'TERM_MATCH', 'Field': 'tenancy', 'Value': tenancy},
    #  {'Type': 'TERM_MATCH', 'Field': 'operatingSystem', 'Value': operating_system},
    #  {'Type': 'TERM_MATCH', 'Field': 'preInstalledSw', 'Value': preinstalled_software},
    # {'Type': 'TERM_MATCH', 'Field': 'licenseModel', 'Value': license_model},
    ]

    # TODO NextToken 이용해서 response 모두 받아오기
    response = pricing_client.get_products(ServiceCode='AmazonEC2', Filters=filters) 
    response_dict[region] = response['PriceList']
    
    while "NextToken" in response:
        
        response = pricing_client.get_products(ServiceCode='AmazonEC2', Filters=filters, NextToken=response["NextToken"]) 
        response_dict[region].extend(response['PriceList'])
        
        try:
            print(json.loads(response['PriceList'][0])['product']['attributes']['instanceType'])
        except:
            print("no instance")

#TODO instance가 없어서 price가 0.0인 경우 처리
with open('./region-instance-price.csv', 'w', newline='') as f: 
    wr = csv.writer(f)
    wr.writerow(['Region Code', 'Instance Type', 'Price'])
    
    for i in range(len(region_names)):
        region_name = region_names[i]
        region_code = region_codes[i]

        for price_info in response_dict[region_name]: # 이거 그냥 위에 포문에 붙여도 될듯
            try:
                instance_type = json.loads(price_info)['product']['attributes']['instanceType']
                instance_price = float(list(list(json.loads(price_info)['terms']['OnDemand'].values())[0]['priceDimensions'].values())[0]['pricePerUnit']['USD'])

                if instance_price == 0.0:
                    continue

                wr.writerow([region_code, instance_type, instance_price])
            except:
                print("no instance in ", region_name, ", attributes: ", json.loads(price_info)['product']['attribute

