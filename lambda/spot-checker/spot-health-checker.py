import pytz
import time
import boto3
import pickle
import datetime

region_ami = pickle.load(open('./region_ami_dict.pkl', 'rb')) # {x86/arm: {region: (ami-id, ami-info), ...}}
az_map_dict = pickle.load(open('./az_map_dict.pkl', 'rb')) # {(region, az-id): az-name, ...}
arm64_family = ['a1', 't4g', 'c6g', 'c6gd', 'c6gn', 'im4gn', 'is4gen', 'm6g', 'm6gd', 'r6g', 'r6gd', 'x2gd']

workload_list = [
    {'instance_type': 't4g.nano',
     'region': 'eu-west-1',
     'az_id': 'euw1-az1'},
    {'instance_type': 'm3.medium',
     'region': 'eu-central-1',
     'az_id': 'euc1-az3'},
    {'instance_type': 'c6gd.medium',
     'region': 'us-west-2',
     'az_id': 'usw2-az1'},
    {'instance_type': 't3.nano',
     'region': 'eu-west-3',
     'az_id': 'euw3-az3'},
    {'instance_type': 'm1.small',
     'region': 'ap-southeast-2',
     'az_id': 'apse2-az3'},
    {'instance_type': 'a1.medium',
     'region': 'ap-northeast-1',
     'az_id': 'apne1-az2'},
    {'instance_type': 't2.large',
     'region': 'ap-southeast-2',
     'az_id': 'apse2-az2'},
    {'instance_type': 'r5dn.large',
     'region': 'eu-west-1',
     'az_id': 'euw1-az3'},
    {'instance_type': 'c5ad.2xlarge',
     'region': 'us-east-2',
     'az_id': 'use2-az2'},
    {'instance_type': 't1.micro',
     'region': 'ap-southeast-2',
     'az_id': 'apse2-az1'},
    {'instance_type': 'c1.medium',
     'region': 'us-west-2',
     'az_id': 'usw2-az1'},
    {'instance_type': 'inf1.xlarge',
     'region': 'us-east-2',
     'az_id': 'use2-az2'}
]

workload = workload_list[6] # t2.large / ap-southeast-2 (sydney)
workload

instance_type = workload['instance_type']
instance_family = instance_type.split('.')[0]
instance_arch = 'arm' if (instance_family in arm64_family) else 'x86'
region = workload['region']
az_id = workload['az_id']
az_name = az_map_dict[(region, az_id)]
ami_id = region_ami[instance_arch][region][0]

launch_spec = {
    'ImageId': ami_id,
    'InstanceType': instance_type,
    'Placement': {'AvailabilityZone': az_name}
}

print(f"""Instance Type: {instance_type}\nInstance Family: {instance_family}\nInstance Arhictecture: {instance_arch}
Region: {region}\nAZ-ID: {az_id}\nAZ-Name:{az_name}\nAMI ID: {ami_id}""")

launch_time = datetime.datetime.now() + datetime.timedelta(minutes=1)
stop_time = datetime.datetime.now() + datetime.timedelta(minutes=6)

session = boto3.session.Session(profile_name='sungjae')
ec2 = session.client('ec2', region_name=region)

response = ec2.request_spot_instances(
    InstanceCount=1,
    LaunchSpecification=launch_spec,
#     SpotPrice=spot_price, # default value for on-demand price
    ValidFrom=launch_time.astimezone(pytz.UTC),
    ValidUntil=stop_time.astimezone(pytz.UTC),
    Type='persistent' # not 'one-time', persistent request
)

# get spot request id
request_id = response['SpotInstanceRequests'][0]['SpotInstanceRequestId']
time.sleep(1)

# get spot request status
request_describe = ec2.describe_spot_instance_requests(SpotInstanceRequestIds=[request_id])
request_status = describe['SpotInstanceRequests'][0]['Status']['Code']
print(request_status)

# Status Checker
# 1. 현재 request 상태를 받기
# 2. 만약 instance 가 run 중이라면, instance 상태도 받기
while True:
    current_time = datetime.datetime.now()
    request_describe = ec2.describe_spot_instance_requests(SpotInstanceRequestIds=[request_id])
    request_status = request_describe['SpotInstanceRequests'][0]['Status']['Code']
    print(f"{request_status} / {current_time}")

    if request_status == 'fulfilled':
        instance_id = request_describe['SpotInstanceRequests'][0]['InstanceId']
        instance_describe = ec2.describe_instance_status(InstanceIds=[instance_id])
        ec2.create_tags(Resources=[instance_id], Tags=[{'Key':'Name', 'Value':'sungjae-spot-test'}])
        instance_status = instance_describe['InstanceStatuses']
        print(f"{instance_status} / {current_time}")
    
    if request_status == 'capacity-not-available':
        print("Capacity Not Available")
        
    if current_time > stop_time:
        print("Stop Loop")
        if status == 'fulfilled':
            print("Terminate Spot Instance")
            terminate_status = ec2.terminate_instances(InstanceIds=[instance_id])
            print(terminate_status)
#             cancel_state = ec2.cancel_spot_instance_requests(SpotInstanceRequestIds=[request_id])
        break
    time.sleep(5)
