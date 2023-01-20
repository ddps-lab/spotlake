import boto3
import botocore

session = boto3.session.Session(profile_name='kmubigdata', region_name='us-west-2')
ec2_client = session.client('ec2')
ec2_resource = session.resource('ec2')
s3 = session.resource('s3')
credentials = session.get_credentials().get_frozen_credentials()

AWS_ACCESS_KEY_ID = credentials.access_key
AWS_SECRET_ACCESS_KEY = credentials.secret_key

# 설정 값들
BUCKET_NAME = 'instance-coremark-result'
FOLDER_NAME = 'aws/test_data_instances_coremark/'
MAX_MEASURE_NUM_ONCE = 5

# x86 ami
ami_x86 = 'ami-0ee93c90bc65c86c2'
# arm ami
ami_arm = 'ami-016b1f9568b08fffb'

instance_workloads = []

with open('instances_to_measure_coremark.txt', 'r') as fr:
    while True:
        instance = fr.readline().split('\n')[0]
        if not instance:
            break
        instance_workloads.append(instance)

print(f'All instances to measure : {instance_workloads}\n(total {len(instance_workloads)} instances)')

measure_instances = []
userdata_workload = []
instances_to_wait = []
instance_ids_to_wait = []

is_completed_one = False

for instance in instance_workloads:
    if is_completed_one:
        measure_instances.clear()
        userdata_workload.clear()
        instances_to_wait.clear()
        instance_ids_to_wait.clear()
        is_completed_one = False

    measure_instances.append(instance)
    if len(measure_instances) < MAX_MEASURE_NUM_ONCE and instance != instance_workloads[-1]:
        continue

    print(f'\nInstances to measure currently : {measure_instances}')

    for instance_type in measure_instances:
        result_file = f'{instance_type}_coremark_result.txt'

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

        core_num=$(lscpu | grep -m 1 "CPU(s)" | cut -d ':' -f 2 | sed 's/ //g')

        make XCFLAGS="-DMULTITHREAD=$core_num -DUSE_PTHREAD -pthread" REBUILD=1
        cat run1.log | grep "CoreMark 1.0" >> {result_file}

        make XCFLAGS="-DMULTITHREAD=$core_num -DUSE_PTHREAD -pthread" REBUILD=1
        cat run1.log | grep "CoreMark 1.0" >> {result_file}

        make XCFLAGS="-DMULTITHREAD=$core_num -DUSE_PTHREAD -pthread" REBUILD=1
        cat run1.log | grep "CoreMark 1.0" >> {result_file}

        aws s3 cp {result_file} s3://{BUCKET_NAME}/{FOLDER_NAME}
        '''

        userdata_workload.append((instance_type, userdata, result_file))

    for instance_type, userdata, result_file in userdata_workload:
        try:
            s3.Object(BUCKET_NAME, f'{FOLDER_NAME}{result_file}').load()
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                pass
            else:
                print("Something else has gone wrong.")
                raise
        else:
            print(f'{instance_type} is already completed')
            continue

        try:
            instances = ec2_resource.create_instances(InstanceType=instance_type, ImageId=ami_x86,
                                                      MaxCount=1, MinCount=1, KeyName='kh-oregon',
                                                      SecurityGroups=['SSH'],
                                                      UserData=userdata)
        except botocore.exceptions.ClientError as e1:
            try:
                instances = ec2_resource.create_instances(InstanceType=instance_type, ImageId=ami_arm,
                                                          MaxCount=1, MinCount=1, KeyName='kh-oregon',
                                                          SecurityGroups=['SSH'],
                                                          UserData=userdata)
            except botocore.exceptions.ClientError as e2:
                print(f"error to create instance: {instance_type}")
                continue
            else:
                print(f"complete to create arm instance: {instance_type}")
        else:
            print(f"complete to create x86 instance: {instance_type}")

        instances_to_wait.append((instance_type, instances, result_file))

    total_instance_num = len(instances_to_wait)
    completed_num = 0

    print(f"\nwaiting for measurement to complete... (the number of instances : {total_instance_num})")

    while len(instances_to_wait) > 0:
        for instance_type, instances, result_file in instances_to_wait:
            try:
                s3.Object(BUCKET_NAME, f'{FOLDER_NAME}{result_file}').load()
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == '404':
                    continue
                else:
                    print("Something else has gone wrong.")
                    raise
            else:
                instance_id_to_terminate = instances[0].id
                response = ec2_client.terminate_instances(InstanceIds=[instance_id_to_terminate])
                instance_ids_to_wait.append(instance_id_to_terminate)

                instances_to_wait.remove((instance_type, instances, result_file))
                completed_num += 1
                print(f"{instance_type} instance type measurement completed ({completed_num}/{total_instance_num})")

    print(f"\nwaiting for terminating completed... (the number of terminating instances : {len(instance_ids_to_wait)})")

    while len(instance_ids_to_wait) > 0:
        for instance_id in instance_ids_to_wait:
            waiter = ec2_client.get_waiter('instance_terminated')
            waiter.wait(InstanceIds=[instance_id])
            instance_ids_to_wait.remove(instance_id)
            print(f'{instance_id} instance termination completed')

    is_completed_one = True
