import json
import time
import boto3
import pickle
import datetime

BUCKET_NAME = ''

s3 = boto3.resource('s3')
credentials = pickle.loads(s3.Bucket(BUCKET_NAME).Object("credential/a.pickle").get()['Body'].read())
user_workloads = pickle.loads(s3.Bucket(BUCKET_NAME).Object("credential/b.pickle").get()['Body'].read())
capacity = 1

def get_sps(ec2, instance_list, regions):
    before = {}
    for instance in instance_list:
        response = ec2.get_spot_placement_scores(InstanceTypes=[instance],
                                                 TargetCapacity=capacity,SingleAvailabilityZone=True,RegionNames=regions)
        before[instance] = response['SpotPlacementScores']
    multi_response = ec2.get_spot_placement_scores(InstanceTypes=instance_list,
                                                   TargetCapacity=capacity,SingleAvailabilityZone=True,RegionNames=regions)
    multi = multi_response['SpotPlacementScores']
    after = {}
    for instance in instance_list:
        response = ec2.get_spot_placement_scores(InstanceTypes=[instance],
                                                 TargetCapacity=capacity,SingleAvailabilityZone=True,RegionNames=regions)
        after[instance] = response['SpotPlacementScores']
    return before, multi, after

def lambda_handler(event, context):
    user_idx = 12
    results_list = []
    for workloads in user_workloads:
        user = credentials[user_idx]
        access_id = user['access_id']
        secret_key = user['secret_key']
        user_num = user['user_num']
        
        session = boto3.session.Session(
            aws_access_key_id=access_id,
            aws_secret_access_key=secret_key)
        ec2 = session.client('ec2', region_name='us-west-2')

        for workload in workloads:
            regions = [workload[0]]
            instances = workload[1]
            before, multi, after = get_sps(ec2, instances, regions)
            results_list.append([regions, instances, before, multi, after])
        user_idx += 1

    results_list_obj = pickle.dumps(results_list)
    now = datetime.datetime.now()
    now_time = now.strftime('%H_%M_%S')
    filename = f'sps_{now_time}.pkl'
    s3_path = f'data_multi_instance/{now.year}/{now.month}/{now.day}/{filename}'
    s3.Object(BUCKET_NAME, s3_path).put(Body=results_list_obj)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Success')
    }
