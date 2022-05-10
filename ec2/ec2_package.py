from boto3 import Session
from datetime import datetime, timedelta
from decimal import Decimal


def get_regions(session: Session, region='us-east-1') -> list:
    client = session.client('ec2', region_name=region)
    describe_args = {
        'AllRegions': False
    }
    return [region['RegionName'] for region in client.describe_regions(**describe_args)['Regions']]


def get_instance_types(session: Session, region: str) -> list:
    client = session.client('ec2', region_name=region)
    describe_args = {
        'MaxResults': 300
    }
    while True:
        describe_result = client.describe_instance_types(**describe_args)
        yield from [instance['InstanceType'] for instance in describe_result['InstanceTypes']]
        if 'NextToken' not in describe_result:
            break
        describe_args['NextToken'] = describe_result['NextToken']


def get_availability_zones(session: Session, region: str) -> list:
    client = session.client('ec2', region_name=region)
    describe_args = {
        'AllAvailabilityZones': False
    }
    return [az['ZoneName'] for az in client.describe_availability_zones(**describe_args)['AvailabilityZones']]


def get_it_az(session: Session, region: str) -> str:
    client = session.client('ec2', region_name=region)
    describe_args = {
        'LocationType': 'availability-zone',
    }
    while True:
        response = client.describe_instance_type_offerings(**describe_args)
        for obj in response['InstanceTypeOfferings']:
            it, region, az = obj.values()
            yield it
        if 'NextToken' not in response:
            break
        describe_args['NextToken'] = response['NextToken']


def get_spot_price(session: Session, region: str, start=None, end=None) -> tuple:
    if type(end) is not datetime:
        end = datetime.utcnow()
    if type(start) is not datetime:
        start = end - timedelta(hours=1)
    client = session.client('ec2', region)
    describe_args = {
        # 'AvailabilityZone': az,
        # 'InstanceTypes': [it],
        # 'ProductDescriptions': ['Linux/UNIX (Amazon VPC)'],
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


def store_spot_price(params: tuple) -> str:
    session, region, start, end = params
    buffer = dict()
    for it, az, os, price, timestamp in get_spot_price(session, region, start, end):
        if it not in buffer:
            buffer[it] = {az: {os: {'price': price, 'timestamp': timestamp}}}
        elif az not in buffer[it]:
            buffer[it][az] = {os: {'price': price, 'timestamp': timestamp}}
        else:
            if os in buffer[it][az] and timestamp < buffer[it][az][os]['timestamp']:
                continue
            buffer[it][az][os] = {'price': price, 'timestamp': timestamp}
    return buffer
