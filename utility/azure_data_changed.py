import pandas as pd
import os
import boto3
from compare_data import compare

pd.set_option('display.max_columns', None)
WORKLOAD_COLS = ['instanceTier', 'instanceType', 'region']
FEATURE_COLS = ['ondemandPrice', 'spotPrice']

# s3에 rawdata를 local로 가져옵니다.
s3 = boto3.resource('s3', aws_access_key_id='', aws_secret_access_key='')
bucket = s3.Bucket('tmp-azure')
prefix = 'rawdata'
for object in bucket.objects.filter(Prefix = 'rawdata'):
    os.makedirs(os.path.dirname(f'./{object.key}'), exist_ok=True)
    bucket.download_file(object.key, object.key)

# local에 잇는 rawdata의 filepath를 list로 저장합니다.
file_list = []
for (path, dir, files) in os.walk("./rawdata"):
    for filename in files:
        ext = os.path.splitext(filename)[-1]
        if ext == '.gz':
            file_list.append("%s/%s" % (path, filename))

# 순서대로 비교할 수 있게 정렬
file_list.sort()

# 맨 처음 rawdata를 previous_df로 설정 10분뒤 데이터를 current_df로 하여 계속 10분뒤 데이터와 비교
for i in range(1, len(file_list)):
    previous_df = pd.read_csv(file_list[i-1], compression='gzip')
    current_df = pd.read_csv(file_list[i], compression='gzip')
    try:
        changed_df = compare(previous_df, current_df, WORKLOAD_COLS, FEATURE_COLS)
        if not changed_df.empty:
            print(i)
            print(changed_df)
    except Exception as e:
        print(f"exception{e} : {i}")
