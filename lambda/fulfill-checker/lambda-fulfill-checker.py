import json
import time
import boto3

# Amazon Linux 2 AMI (HVM), SSD Volume Type
region_ami = {
    'us-west-2': 'ami-013a129d325529d4d',
    'ap-northeast-2': 'ami-0e4a9ad2eb120e054',
    'us-east-1': 'ami-0ed9277fb7eb570c9'
    }

def lambda_handler(event, context):
    instance_type = event['instance_type']
    region = event['region']
    az = event['az']
    spot_price = event['spot_price']
    ami = region_ami[region]
    
    # launch spec setting
    launch_spec = {
    'ImageId': ami,
    'InstanceType': instance_type,
    'Placement': {'AvailabilityZone': az}
    }
    
    # ec2 client setting
    ec2 = boto3.client('ec2', region_name=region)
    
    # spot request
    response = ec2.request_spot_instances(
    InstanceCount=1,
    LaunchSpecification=launch_spec,
    SpotPrice=spot_price,
    Type='one-time'
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
            instance_id = describe['SpotInstanceRequests'][0]['InstanceId']
            print("Terminate Spot Instance")
            terminate_status = ec2.terminate_instances(InstanceIds=[instance_id])
            print(terminate_status)
            print("Cancel Spot Request")
            cancel_state = ec2.cancel_spot_instance_requests(SpotInstanceRequestIds=[request_id])
            print(cancel_state)
            break

    return {
        'statusCode': 200,
        'body': json.dumps(f"Spot Status: {status}")
    }
