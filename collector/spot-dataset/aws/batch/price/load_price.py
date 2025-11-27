# ------ import module ------
import boto3.session
import pandas as pd
from datetime import datetime, timedelta, timezone


# get all available regions
def get_regions(session: boto3.session.Session, region='us-east-1') -> list:
    client = session.client('ec2', region_name=region)
    describe_args = {
        'AllRegions': False
    }
    return [region['RegionName'] for region in client.describe_regions(**describe_args)['Regions']]


# get spot price by all availability zone in single region
def get_spot_price_region(session: boto3.session.Session, region: str, start=None, end=None) -> tuple: # type: ignore
    client = session.client('ec2', region)
    describe_args = {
        'MaxResults': 300,
        'StartTime': start,
        'EndTime': end
    }
    while True:
        response = client.describe_spot_price_history(**describe_args)
        for obj in response['SpotPriceHistory']:
            az = obj['AvailabilityZone']
            it = obj['InstanceType']
            os = obj['ProductDescription']
            price = obj['SpotPrice']
            timestamp = obj['Timestamp']
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
    
    end_date = datetime.now(timezone.utc).replace(microsecond=0)
    start_date = end_date - timedelta(microseconds=1)

    spotprice_dict = {"InstanceType": [], "AZ": [], "SpotPrice": []}
    
    for it, az, price, _ in get_spot_price_region(session, region, start_date, end_date):
        spotprice_dict["InstanceType"].append(it)
        spotprice_dict["AZ"].append(az)
        spotprice_dict["SpotPrice"].append(price)
    
    spot_price_df = pd.DataFrame(spotprice_dict)

    # filter to change az-name to az-id
    az_map = dict()
    ec2 = session.client('ec2', region_name=region)
    response = ec2.describe_availability_zones()

    for val in response['AvailabilityZones']:
        az_map[val['ZoneName']] = val['ZoneId']
    
    spot_price_df = spot_price_df.replace({"AZ":az_map})
    
    return spot_price_df
