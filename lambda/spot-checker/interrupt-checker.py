import pytz
import time
import boto3
import pickle
import datetime

region_ami = pickle.load(open('./region_ami_dict.pkl', 'rb')) # {x86/arm: {region: (ami-id, ami-info), ...}}
az_map_dict = pickle.load(open('./az_map_dict.pkl', 'rb')) # {(region, az-id): az-name, ...}
arm64_family = ['t4g', 'c6g', 'c6gd', 'c6gn', 'im4gn', 'is4gen', 'm6g', 'm6gd', 'r6g', 'r6gd', 'x2gd']

instance_type = 't4g.medium'
instance_family = instance_type.split('.')[0]
instance_arch = 'arm' if (instance_family in arm64_family) else 'x86'
region = 'us-east-2'
az_id = 'use2-az1'
az_name = az_map_dict[(region, az_id)]
ami_id = region_ami[instance_arch][region][0]

launch_time = datetime.datetime.now() + datetime.timedelta(minutes=1)
stop_time = datetime.datetime.now() + datetime.timedelta(hours=1, minutes=1)

launch_spec = {
    'ImageId': ami_id,
    'InstanceType': instance_type,
    'Placement': {'AvailabilityZone': az_name}
}

print(f"""Instance Type: {instance_type}\nInstance Family: {instance_family}\nInstance Arhictecture: {instance_arch}
Region: {region}\nAZ-ID: {az_id}\nAZ-Name:{az_name}\nAMI ID: {ami_id}""")

session = boto3.session.Session(profile_name='dev-session')
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
describe = ec2.describe_spot_instance_requests(SpotInstanceRequestIds=[request_id])
status = describe['SpotInstanceRequests'][0]['Status']['Code']
print(status)

# check spot request status
while True:
    # get current status of spot request
    describe = ec2.describe_spot_instance_requests(SpotInstanceRequestIds=[request_id])

    # if status was changed, print status
    if status != describe['SpotInstanceRequests'][0]['Status']['Code']:
        status = describe['SpotInstanceRequests'][0]['Status']['Code']
        print(status)

    # if status is "price-too-low", cancel and break
    if status == 'price-too-low':
        print("Cancel Spot Request")
        cancel_state = ec2.cancel_spot_instance_requests(SpotInstanceRequestIds=[request_id])
        print(cancel_state)
        break

    # if status is "fulfilled", cancel and break
    if status == 'fulfilled':
        print("Instance Fulfilled!")
        break
    time.sleep(0.5)
