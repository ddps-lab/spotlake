import boto3
import botocore
import time
import pickle
from tqdm import tqdm

session = boto3.session.Session(profile_name='kmubigdata', region_name='us-west-2')
ec2_client = session.client('ec2')
ec2_resource = session.resource('ec2')
s3 = session.resource('s3')
credentials = session.get_credentials().get_frozen_credentials()

INSTANCE_TYPE = 'a1.xlarge'
AWS_ACCESS_KEY_ID = credentials.access_key
AWS_SECRET_ACCESS_KEY = credentials.secret_key
BUCKET_NAME = 'spotlake-lscpu'

describe_args = {
    'LocationType': 'availability-zone',
}
region_instances = []
while True:
    response = ec2_client.describe_instance_type_offerings(**describe_args)
    for obj in response['InstanceTypeOfferings']:
        it, _, az = obj.values()
        region_instances.append(it)
    if 'NextToken' not in response:
        break
    describe_args['NextToken'] = response['NextToken']

print("Region Data Load Complete")

instance_workloads = sorted(region_instances)
userdata_workload = []

for instanceType in instance_workloads:
    userdata = f'''#!/bin/bash
sudo apt-get update
sudo apt install awscli -y
lscpu > /tmp/{instanceType}_cpudata.txt
aws configure set aws_access_key_id {AWS_ACCESS_KEY_ID}
aws configure set aws_secret_access_key {AWS_SECRET_ACCESS_KEY}
aws s3 cp /tmp/{instanceType}_cpudata.txt s3://{BUCKET_NAME}/aws/
'''
    userdata_workload.append((instanceType, userdata))

cnt = 0
print(f'{cnt}/{len(userdata_workload)}')

for instanceType, userData in userdata_workload:
    cnt+=1
    KEY = f'aws/{instanceType}_cpudata.txt'
    try:
        s3.Object(BUCKET_NAME, KEY).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            pass
        else:
            print("Something else has gone wrong.")
            raise
    else:
        print(f'{instanceType} is already completed')
        continue
    start = time.time()
    instances = []
    try:
        # Intel processor
        instances = ec2_resource.create_instances(InstanceType=instanceType, ImageId='ami-0ecc74eca1d66d8a6', MaxCount=1, MinCount=1, KeyName='lscpu-pem', SecurityGroups=['SSH'], UserData=userData)
    except botocore.exceptions.ClientError as e1:
        try:
            # ARM processor
            instances = ec2_resource.create_instances(InstanceType=instanceType, ImageId='ami-06e2dea2cdda3acda', MaxCount=1, MinCount=1, KeyName='lscpu-pem', SecurityGroups=['SSH'], UserData=userData)
        except botocore.exceptions.ClientError as e2:
            #error
            print(f'{instanceType} failed')
            continue
            
    check = True
    while check:
        try:
            s3.Object(BUCKET_NAME, KEY).load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                pass
            else:
                print("Something else has gone wrong.")
                raise
        else:
            response = ec2_client.terminate_instances(InstanceIds=[instances[0].id])
            check = False
    
    print(f'{instanceType} complete : {cnt}/{len(userdata_workload)}, {time.time()-start}(s)')

print("Progress End")

