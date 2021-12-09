import json
import time
import boto3
import pickle
import datetime

BUCKET_NAME = ''

s3 = boto3.resource('s3')
credentials = pickle.loads(s3.Bucket(BUCKET_NAME).Object("credential/a.pickle").get()['Body'].read())
user_workloads = pickle.loads(s3.Bucket(BUCKET_NAME).Object("credential/b.pickle").get()['Body'].read())

def lambda_handler(event, context):
    results_list = []
    user_idx = 0
    for capacity in [2, 4, 8, 16, 32, 50]:
        for workload_idx in [0, 1]:
            user = credentials[user_idx]
            access_id = user['access_id']
            secret_key = user['secret_key']
            user_num = user['user_num']
            
            session = boto3.session.Session(
                aws_access_key_id=access_id,
                aws_secret_access_key=secret_key)
            ec2 = session.client('ec2', region_name='us-west-2')
            
            single_user_workloads = user_workloads[workload_idx]
            for workload in single_user_workloads:
                single_instance = workload[0]
                regions = workload[1]
                
                response = ec2.get_spot_placement_scores(
                    InstanceTypes=[single_instance],
                    TargetCapacity=capacity,
                    SingleAvailabilityZone=True,
                    RegionNames=regions)
                scores = response['SpotPlacementScores']
                results = ({'user_num': user_num}, single_instance, regions, scores, capacity)
                results_list.append(results)
            user_idx += 1
                
    results_list_obj = pickle.dumps(results_list)
    now = datetime.datetime.now()
    now_time = now.strftime('%H_%M_%S')
    filename = f'sps_{now_time}.pkl'
    s3_path = f'data_capacity/{now.year}/{now.month}/{now.day}/{filename}'
    s3.Object(BUCKET_NAME, s3_path).put(Body=results_list_obj)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Success')
    }
