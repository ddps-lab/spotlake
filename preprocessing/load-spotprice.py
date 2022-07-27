import boto3
import pickle
import pandas as pd
from datetime import datetime

session = boto3.session.Session(profile_name='dev-session')
region_list_default = ['us-east-2', 'ap-south-1', 'us-west-2', 'ap-northeast-3', 'ap-southeast-1', 'ap-northeast-2', 'ca-central-1', 'eu-central-1', 'eu-west-1', 'eu-west-2', 'eu-west-3', 'eu-north-1', 'sa-east-1', 'ap-northeast-1', 'ap-southeast-2', 'us-east-1', 'us-west-1']
az_map = pickle.load(open('./az_map.pkl', 'rb'))

# get all name of instances
ec2 = session.client('ec2', region_name='us-west-2')
response = ec2.describe_instance_types()
results = response['InstanceTypes']
while "NextToken" in response:
    response = ec2.describe_instance_types(NextToken=response["NextToken"])
    results.extend(response['InstanceTypes'])
instance_list = sorted([x['InstanceType'] for x in results])

# 
# start_time = datetime(2021, 11, 11)
# end_time = datetime(2021, 12, 11)

results = []
for region in region_list_default:
    ec2 = session.client('ec2', region_name=region)
    
    response = ec2.describe_spot_price_history(
        InstanceTypes=instance_list,
#         StartTime=start_time,
#         EndTime=end_time,
        ProductDescriptions=['Linux/UNIX (Amazon VPC)'])
    
    results.extend(response['SpotPriceHistory'])
    print(len(results), end=' ')
    while response["NextToken"] != '':
        response = ec2.describe_spot_price_history(
            InstanceTypes=instance_list,
    #         StartTime=start_time,
    #         EndTime=end_time,
            ProductDescriptions=['Linux/UNIX (Amazon VPC)'],
            NextToken=response["NextToken"])
        results.extend(response['SpotPriceHistory'])
        print(len(results), end=' ')

price_df = pd.DataFrame(results)
price_df = price.merge(az_map, how='left', left_on='AvailabilityZone', right_on='az-name')
price_df = price[['InstanceType', 'region', 'AvailabilityZone', 'az-id', 'SpotPrice', 'Timestamp']]
price_df = price.sort_values(by=['Timestamp', 'InstanceType', 'region', 'az-id'])

pickle.dump(price_df, open('./spotprice_df.pkl', 'wb'))
