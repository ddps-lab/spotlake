import pytz
import time
import boto3
import pickle
import datetime
import argparse
import pandas as pd


### Spot Checker Mapping Data
region_ami = pickle.load(open('./region_ami_dict.pkl', 'rb')) # {x86/arm: {region: (ami-id, ami-info), ...}}
az_map_dict = pickle.load(open('./az_map_dict.pkl', 'rb')) # {(region, az-id): az-name, ...}
arm64_family = ['a1', 't4g', 'c6g', 'c6gd', 'c6gn', 'im4gn', 'is4gen', 'm6g', 'm6gd', 'r6g', 'r6gd', 'x2gd']


### Spot Checker Arguments
parser = argparse.ArgumentParser(description='Spot Checker Workload Information')
parser.add_argument('--instance_type', type=str, default='t2.large')
parser.add_argument('--region', type=str, default='ap-southeast-2')
parser.add_argument('--az_id', type=str, default='apse2-az2')
parser.add_argument('--wait', type=str, default='1', help='wait before request, minutes')
parser.add_argument('--time_minutes', type=str, default='5', help='how long check spot instance, hours')
parser.add_argument('--time_hours', type=str, default='0', help='how long check spot instance, hours')
args = parser.parse_args()


### Spot Checker Arguments Parsing
instance_type = args['instance_type']
instance_family = instance_type.split('.')[0]
instance_arch = 'arm' if (instance_family in arm64_family) else 'x86'
region = args['region']
az_id = args['az_id']
az_name = az_map_dict[(region, az_id)]
ami_id = region_ami[instance_arch][region][0]
launch_time = datetime.datetime.now() + datetime.timedelta(minutes=args['wait'])
launch_time = launch_time.astimezone(pytz.UTC)
stop_time = datetime.datetime.now() + datetime.timedelta(hours=args['time'], minutes=(args['time'] + args['wait']))
stop_time = stop_time.astimezone(pytz.UTC)
spot_data_dict = {}


### Spot Launch Specifications
launch_spec = {
    'ImageId': ami_id,
    'InstanceType': instance_type,
    'Placement': {'AvailabilityZone': az_name}
}
launch_info = [instance_type, instance_family, instance_arch, region, az_id, az_name, ami_id]
print(f"""Instance Type: {instance_type}\nInstance Family: {instance_family}\nInstance Arhictecture: {instance_arch}
Region: {region}\nAZ-ID: {az_id}\nAZ-Name:{az_name}\nAMI ID: {ami_id}""")
spot_data_dict['launch_spec'] = launch_spec
spot_data_dict['launch_info'] = launch_info
spot_data_dict['start_time'] = launch_time
spot_data_dict['end_time'] = stop_time


### Start Spot Checker
session = boto3.session.Session(profile_name='sungjae')
ec2 = session.client('ec2', region_name=region)

create_request_response = ec2.request_spot_instances(
    InstanceCount=1,
    LaunchSpecification=launch_spec,
#     SpotPrice=spot_price, # default value for on-demand price
    ValidFrom=launch_time,
    ValidUntil=stop_time,
    Type='persistent' # not 'one-time', persistent request
)
spot_data_dict['create_request'] = create_request_response
request_id = create_request_response['SpotInstanceRequests'][0]['SpotInstanceRequestId']
time.sleep(1)


### Status Log Variables
log_list = []
instance_tag = False


### First Log
current_time = datetime.datetime.now()
current_time = current_time.astimezone(pytz.UTC)
request_describe = ec2.describe_spot_instance_requests(SpotInstanceRequestIds=[request_id])
request_status = describe['SpotInstanceRequests'][0]['Status']['Code']
instance_describe = ''
timestamps.append(current_time)
request_describes.append(request_describe)
instance_describes.append(instance_describe)


### Loop Log
while True:
    current_time = datetime.datetime.now()
    current_time = current_time.astimezone(pytz.UTC)
    request_describe = ec2.describe_spot_instance_requests(SpotInstanceRequestIds=[request_id])
    request_status = describe['SpotInstanceRequests'][0]['Status']['Code']
    log_list.append((current_time, request_describe, instance_describe))
    
    if request_status == 'fulfilled':
        instance_id = request_describe['SpotInstanceRequests'][0]['InstanceId']
        if instance_tag == False:
            instance_tag = True
            ec2.create_tags(Resources=[instance_id], Tags=[{'Key':'Name', 'Value':'spot-checker-target'}])
        
        instance_describe = ec2.describe_instance_status(InstanceIds=[instance_id])
        instance_status = instance_describe['InstanceStatuses']
        print("Fulfilled")
        
    if request_status == 'capacity-not-available':
        print("Capacity Not Available")
            
    if current_time > stop_time:
        print("Stop Loop Logging")
        if status == 'fulfilled':
            print("Terminate Spot Instance")
            terminate_response = ec2.terminate_instances(InstanceIds=[instance_id])
            spot_data_dict['terminate_response'] = terminate_response
            
            current_time = datetime.datetime.now()
            current_time = current_time.astimezone(pytz.UTC)
            request_describe = ec2.describe_spot_instance_requests(SpotInstanceRequestIds=[request_id])
            instance_describe = ec2.describe_instance_status(InstanceIds=[instance_id])
            log_list.append((current_time, request_describe, instance_describe))
        break
    time.sleep(5)
    
spot_data_dict['logs'] = log_list
filename = f"{instance_type}_{region}_{az_id}_{launch_time}"
pickle.dump(spot_data_dict, open(filename, 'wb'))
