import boto3
import pickle
from datetime import datetime

session = boto3.session.Session(profile_name='dev-session')

result_list = []
for region in region_list_default:
    if region == 'eu-west-1':
        continue
    ec2 = session.client('ec2', region_name=region)

    response = ec2.describe_instance_types()
    results = response['InstanceTypes']
    while "NextToken" in response:
        response = ec2.describe_instance_types(NextToken=response["NextToken"])
        results.extend(response['InstanceTypes'])
    result_list += results
    print(f"{region}: {len(results)}")
    
print(len(set([x['InstanceType'] for x in result_list])))
