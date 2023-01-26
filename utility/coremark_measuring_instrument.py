import boto3
import botocore

# 설정 값들
REGION_NAME = 'us-west-2' # 여기에 들어가는 리전은 아래의 aim_info에 리전이름과 ami 정보가 들어있어야한다.
BUCKET_NAME = 'instance-coremark-result' # 결과값을 저장할 버킷이름
FOLDER_NAME = 'aws/test/' # 결과값을 저장할 버킷내에서의 폴더이름, 폴더가 없다면 ''로 비워둔다. 폴더가 있다면 끝은 '/'를 넣어준다.
MAX_MEASURE_NUM_ONCE = 5 # 한번에 몇개씩 측정할지 개수설정
KEY_NAME = 'kh-oregon' # 인스턴스를 생성할 때 사용할 키페어이름
MEASUREMENTS_NUM = 2 # 인스턴스당 코어마크점수 측정 횟수

session = boto3.session.Session(profile_name='kmubigdata', region_name='us-west-2')
ec2_client = session.client('ec2')
ec2_resource = session.resource('ec2')
s3 = session.resource('s3')
credentials = session.get_credentials().get_frozen_credentials()

AWS_ACCESS_KEY_ID = credentials.access_key
AWS_SECRET_ACCESS_KEY = credentials.secret_key

# region name과 [x86 ami, arm ami]를 딕셔너리로 구성
ami_info = {'us-west-2': ['ami-0ee93c90bc65c86c2', 'ami-016b1f9568b08fffb']}

# x86 ami
ami_x86 = ami_info.get(REGION_NAME)[0]
# arm ami
ami_arm = ami_info.get(REGION_NAME)[1]

instance_workloads = []

with open('instances_to_measure_coremark.txt', 'r') as fr:
    while True:
        instance = fr.readline().split('\n')[0]
        if not instance:
            break
        instance_workloads.append(instance)

print(f'All instances to measure : {instance_workloads}\n(total {len(instance_workloads)} instances)')

measure_instances = [] # 이번에 측정할 인스턴스타입 이름이 들어가는 리스트, MAX_MEASURE_NUM_ONCE 보다 적거나 같게 들어간다.
instances_to_wait = [] # 생성된 인스턴스 정보들이 들어간다. 측정이 완료됐는지 s3 버킷을 확인하고 결과파일이 올라가면 측정완료로 간주하고 종료한다.
instance_ids_to_wait = [] # 종료된 인스턴스 id가 들어간다. 인스턴스들이 종료되면 종료가 완료될 때까지 새로운 인스턴스 생성을 미룬다.

is_completed_one = False

for instance in instance_workloads:
    if is_completed_one:
        measure_instances.clear()
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

        for ((i=0; i<{MEASUREMENTS_NUM}; i++))
        do
            make XCFLAGS="-DMULTITHREAD=$core_num -DUSE_PTHREAD -pthread" REBUILD=1
            cat run1.log | grep "CoreMark 1.0" >> {result_file}
        done
        
        aws s3 cp {result_file} s3://{BUCKET_NAME}/{FOLDER_NAME}
        '''

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
            created_instance = ec2_resource.create_instances(InstanceType=instance_type, ImageId=ami_x86,
                                                      MaxCount=1, MinCount=1, KeyName=KEY_NAME,
                                                      SecurityGroups=['SSH'],
                                                      UserData=userdata)[0]
        except botocore.exceptions.ClientError as e1:
            try:
                created_instance = ec2_resource.create_instances(InstanceType=instance_type, ImageId=ami_arm,
                                                          MaxCount=1, MinCount=1, KeyName=KEY_NAME,
                                                          SecurityGroups=['SSH'],
                                                          UserData=userdata)[0]
            except botocore.exceptions.ClientError as e2:
                print(f"error to create instance: {instance_type}\n{e1}\n{e2}")
                continue
            else:
                print(f"complete to create arm instance: {instance_type}")
        else:
            print(f"complete to create x86 instance: {instance_type}")

        instances_to_wait.append((instance_type, created_instance, result_file))

    total_instance_num = len(instances_to_wait)
    completed_num = 0

    print(f"\nwaiting for measurement to complete... (the number of instances : {total_instance_num})")

    while len(instances_to_wait) > 0:
        for instance_type, created_instance, result_file in instances_to_wait:
            try:
                s3.Object(BUCKET_NAME, f'{FOLDER_NAME}{result_file}').load()
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == '404':
                    continue
                else:
                    print("Something else has gone wrong.")
                    raise
            else:
                instance_id_to_terminate = created_instance.id
                response = ec2_client.terminate_instances(InstanceIds=[instance_id_to_terminate])
                instance_ids_to_wait.append(instance_id_to_terminate)

                instances_to_wait.remove((instance_type, created_instance, result_file))
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
