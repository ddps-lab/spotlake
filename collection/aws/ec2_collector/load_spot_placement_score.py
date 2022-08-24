import boto3
import pandas as pd
import pickle


# get spot placement score
def get_sps(workload):
    sps_list = []

    user = workload[1]
    access_id = user['AccessKeyId']
    secret_key = user['SecretAccessKey']
    # credentials start with spotrank+002@kookmin.ac.kr at index 0
    user_num = workload[0] + 2

    session = boto3.session.Session(
            aws_access_key_id=access_id,
            aws_secret_access_key=secret_key)
    ec2 = session.client('ec2', region_name='us-west-2')
        
    for query in workload[2]:
        response = ec2.get_spot_placement_scores(
                InstanceTypes=[query[0]],
                TargetCapacity=1,
                SingleAvailabilityZone=True,
                RegionNames=query[1])
        scores = response['SpotPlacementScores']
        results = (query[0], scores)
        sps_list.append(results)

    sps_dict = {'InstanceType' : [],
                'Region' : [],
                'AvailabilityZoneId': [],
                'SPS' : []}

    for sps in sps_list:
        for info in sps[1]:
            sps_dict['InstanceType'].append(sps[0])
            sps_dict['Region'].append(info['Region'])
            sps_dict['AvailabilityZoneId'].append(info['AvailabilityZoneId'])
            sps_dict['SPS'].append(int(info['Score']))

    sps_df = pd.DataFrame(sps_dict)

    return sps_df
