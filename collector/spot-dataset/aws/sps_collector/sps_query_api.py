import boto3
import botocore
import time
import requests
import pandas as pd

IDX_INSTANCE_TYPE = 0
IDX_REGION_NAMES = 1
IDX_NUMBER_RESPONSE = 2

REGION=None

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
    
    return pd.DataFrame(sps_dict)

def get_token():
    token_url = "http://169.254.169.254/latest/api/token"
    headers = {"X-aws-ec2-metadata-token-ttl-seconds": "5"}
    response = requests.put(token_url, headers=headers)
    if response.status_code == 200:
        return response.text
    else:
        raise Exception("토큰을 가져오는 데 실패했습니다. 상태 코드: {}".format(response.status_code))

def get_region():
    if REGION is not None:
        return REGION
    token = get_token()
    if token:
        metadata_url = "http://169.254.169.254/latest/dynamic/instance-identity/document"
        headers = {"X-aws-ec2-metadata-token": token}
        response = requests.get(metadata_url, headers=headers)
        if response.status_code == 200:
            document = response.json()
            REGION = document.get("region")
            return REGION
        else:
            raise Exception("메타데이터를 가져오는 데 실패했습니다. 상태 코드: {}".format(response.status_code))
    else:
        raise Exception("토큰이 없습니다.")