import boto3
import botocore
import time
import pandas as pd

IDX_INSTANCE_TYPE = 0
IDX_REGION_NAMES = 1
IDX_NUMBER_RESPONSE = 2

from utility.utils import get_region

# SPS 점수를 계정별로 받아오는 함수입니다.
# args는 다음과 같이 구성된 튜플이어야 합니다
# (credential, scenarios, target_capacity)
# credential은 dataframe의 행 형태로 주어져야 합니다.
# scenarios는 쿼리 시나리오 50개 묶음이어야 합니다.
def query_sps(args):
    credential = args[0]
    scenarios = args[1]
    target_capacity = args[2]
    region = get_region()
    
    session = boto3.session.Session(
        aws_access_key_id = credential["AccessKeyId"],
        aws_secret_access_key = credential["SecretAccessKey"]
    )
    ec2 = session.client('ec2', region_name = region)
    
    sps_dict = {
        "InstanceType" : [],
        "Region" : [],
        "AZ" : [],
        "SPS" : [],
        "TargetCapacity" : [],
        "T3": [],
        "T2": []
    }
    
    for scenario in scenarios:
        instance_type = scenario[IDX_INSTANCE_TYPE]
        region_names = scenario[IDX_REGION_NAMES]

        # exponential backoff 전략을 사용합니다.
        retries = 0
        max_retries = 10
        while retries <= max_retries:
            try:
                response = ec2.get_spot_placement_scores(
                    InstanceTypes = [instance_type],
                    RegionNames = region_names,
                    SingleAvailabilityZone = True,
                    TargetCapacity = target_capacity
                )
                scores = response["SpotPlacementScores"]
                break
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == "RequestLimitExceeded":
                    wait_time = 2 ** retries
                    print(f"RequestLimitExceeded! {wait_time}초 후 재시도합니다.")
                    time.sleep(wait_time)
                    retries += 1
                else:
                    raise e
                
        for score in scores:
            sps_dict["InstanceType"].append(instance_type)
            sps_dict["Region"].append(score["Region"])
            sps_dict["AZ"].append(score['AvailabilityZoneId'])
            sps_dict["SPS"].append(int(score["Score"]))
            sps_dict["TargetCapacity"].append(target_capacity)
            
            if score['Score'] == 3:
                sps_dict["T3"].append(target_capacity)
            else:
                sps_dict["T3"].append(0)

            if score['Score'] == 2:
                sps_dict["T2"].append(target_capacity)
            else:
                sps_dict["T2"].append(0)
    
    return pd.DataFrame(sps_dict)
