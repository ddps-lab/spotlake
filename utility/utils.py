import requests
import os

REGION = None

def get_token():
    token_url = "http://169.254.169.254/latest/api/token"
    headers = {"X-aws-ec2-metadata-token-ttl-seconds": "5"}
    try:
        response = requests.put(token_url, headers=headers, timeout=5)
        if response.status_code == 200:
            return response.text
        else:
            raise Exception("토큰을 가져오는 데 실패했습니다. 상태 코드: {}".format(response.status_code))
    except Exception as e:
        print(f"IMDSv2 토큰 요청 실패 (로컬 환경일 수 있음): {e}")
        raise e

def get_region():
    global REGION
    if REGION is not None:
        return REGION
    
    try:
        token = get_token()
        if token:
            metadata_url = "http://169.254.169.254/latest/dynamic/instance-identity/document"
            headers = {"X-aws-ec2-metadata-token": token}
            response = requests.get(metadata_url, headers=headers, timeout=5)
            if response.status_code == 200:
                document = response.json()
                REGION = document.get("region")
                return REGION
            else:
                raise Exception("메타데이터를 가져오는 데 실패했습니다. 상태 코드: {}".format(response.status_code))
        else:
            raise Exception("토큰이 없습니다.")
    except Exception as e:
        print(f"리전 정보 획득 실패: {e}")
        # Fallback to environment variable if available (for local testing)
        if os.environ.get('AWS_REGION'):
            return os.environ.get('AWS_REGION')
        if os.environ.get('AWS_DEFAULT_REGION'):
            return os.environ.get('AWS_DEFAULT_REGION')
        raise e
