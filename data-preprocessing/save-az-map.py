import boto3
import time

session = boto3.session.Session(profile_name='sungjae')

region_list_default = ['us-east-2', 'ap-south-1', 'us-west-2', 'ap-northeast-3', 'ap-southeast-1', 'ap-northeast-2', 'ca-central-1', 'eu-central-1', 'eu-west-1', 'eu-west-2', 'eu-west-3', 'eu-north-1', 'sa-east-1', 'ap-northeast-1', 'ap-southeast-2', 'us-east-1', 'us-west-1']

## if you want to use all available regions, use get_available_regions method
# region_list_session = session.get_available_regions('ec2')

az_map = []
for region in region_list_default:
    # eu-weat-1 does not working
    if region == 'eu-west-1':
        continue
    print(region)
    ec2 = session.client('ec2', region_name=region)
    response = ec2.describe_availability_zones()

    for val in response['AvailabilityZones']:
        az_map.append([val['RegionName'], val['ZoneName'], val['ZoneId']])
    time.sleep(1)

# for eu-west-1 region
ec2 = session.client('ec2', region_name='eu-west-1')
response = ec2.describe_availability_zones()
eu_west_1_map = [[x['RegionName'], x['ZoneName'], x['ZoneId']] for x in response['AvailabilityZones']]

az_map_df = pd.DataFrame(az_map + eu_west_1_map, columns=['region', 'az-name', 'az-id'])
pickle.dump(az_map_df, open('./az_map.pkl', 'wb'))
