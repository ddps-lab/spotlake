import boto3
import pickle
from collections import Counter


# get all available regions
def get_regions(session: boto3.session.Session, region='us-east-1') -> list:
    client = session.client('ec2', region_name=region)
    describe_args = {
        'AllRegions': False
    }
    return [region['RegionName'] for region in client.describe_regions(**describe_args)['Regions']]


# get instance-az information by region
def get_region_instances(session: boto3.session.Session, region: str):
    client = session.client('ec2', region_name=region)
    describe_args = {
        'LocationType': 'availability-zone',
    }
    region_instances = []
    while True:
        response = client.describe_instance_type_offerings(**describe_args)
        for obj in response['InstanceTypeOfferings']:
            it, _, az = obj.values()
            region_instances.append((region, it))
        if 'NextToken' not in response:
            break
        describe_args['NextToken'] = response['NextToken']
    
    return region_instances


# calculate number of az by region
# first, get region information using get_regions
# second, get az information by region using get_region_instances
def num_az_by_region():
    session = boto3.session.Session()
    
    regions = get_regions(session)
    
    total_counter = Counter()
    for idx, region in enumerate(regions):
        region_counter = Counter(get_region_instances(session, region))
        total_counter += region_counter
    
    workloads = dict()
    for key, cnt in total_counter.items():
        region, it = key
        if it not in workloads:
            workloads[it] = []
        workloads[it].append((region, cnt))
    
    return workloads
