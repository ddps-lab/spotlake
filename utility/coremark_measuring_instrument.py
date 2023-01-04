import boto3
import botocore

session = boto3.session.Session(profile_name='kmubigdata', region_name='us-west-2')
ec2_client = session.client('ec2')
ec2_resource = session.resource('ec2')
s3 = session.resource('s3')
credentials = session.get_credentials().get_frozen_credentials()

AWS_ACCESS_KEY_ID = credentials.access_key
AWS_SECRET_ACCESS_KEY = credentials.secret_key

# x86 ami
ami = 'ami-0a65996d2713f71f1'

instance_type = 'c4.xlarge'
BUCKET_NAME = 'instance-coremark-result'
result_file = f'{instance_type}_coremark_result.txt'
core_num = 4

userdata = f'''#!/bin/bash
apt-get update -y
apt-get install -y git
apt-get install -y build-essential
apt-get install awscli -y

aws configure set aws_access_key_id {AWS_ACCESS_KEY_ID}
aws configure set aws_secret_access_key {AWS_SECRET_ACCESS_KEY}

git clone https://github.com/eembc/coremark.git

cd coremark/

touch {result_file}

make XCFLAGS="-DMULTITHREAD={core_num} -DUSE_PTHREAD -pthread" REBUILD=1
cat run1.log | grep "CoreMark 1.0" >> {result_file}

make XCFLAGS="-DMULTITHREAD={core_num} -DUSE_PTHREAD -pthread" REBUILD=1
cat run1.log | grep "CoreMark 1.0" >> {result_file}

make XCFLAGS="-DMULTITHREAD={core_num} -DUSE_PTHREAD -pthread" REBUILD=1
cat run1.log | grep "CoreMark 1.0" >> {result_file}

aws s3 cp {result_file} s3://{BUCKET_NAME}/
'''

try:
    # Intel processor
    instances = ec2_resource.create_instances(InstanceType=instance_type, ImageId=ami,
                                              MaxCount=1, MinCount=1, KeyName='kh-oregon', SecurityGroups=['SSH'],
                                              UserData=userdata)
except Exception:
    print("create error")

check = True
while check:
    try:
        s3.Object(BUCKET_NAME, result_file).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            pass
        else:
            print("Something else has gone wrong.")
            raise
    else:
        waiter = ec2_client.get_waiter('instance_terminated')
        response = ec2_client.terminate_instances(InstanceIds=[instances[0].id])
        waiter.wait(InstanceIds=[instances[0].id])
        check = False
