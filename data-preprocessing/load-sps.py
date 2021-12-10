import boto3
import pickle
import pandas as pd
from tqdm import tqdm

session = boto3.session.Session(profile_name='dev-session')
s3 = session.client('s3')

BUCKET_SPS = 'sungjae-sps-data'

pagenator = s3.get_paginator('list_objects').paginate(Bucket=BUCKET_SPS, Prefix='data/')
pages = [page for page in pagenator]
contents = [content['Contents'] for content in pages]
flat_contents = [content for sublist in contents for content in sublist]
sps_objects = [(x['Key'], x['LastModified']) for x in flat_contents if 'pkl' in x['Key']]

s3 = session.resource('s3')

sps_list = []
for sps_filename, sps_datetime in tqdm(sps_objects):
    sps_queries = pickle.loads(s3.Bucket(BUCKET_SPS).Object(sps_filename).get()['Body'].read())
    for query in sps_queries:
        instance_type = query[1]
        scores = query[3]
        for score in scores:
            sps_list.append([instance_type, score['Region'], score['AvailabilityZoneId'], score['Score'], sps_datetime])
            
sps_df = pd.DataFrame(sps_list, columns=['InstanceType', 'Region', 'AvailabilityZoneId', 'Score', 'TimeStamp'])
sps_df = sps_df.sort_values(by=['TimeStamp', 'InstanceType', 'Region', 'AvailabilityZoneId'], ignore_index=True)

pickle.dump(sps_df, open('./sps_df.pkl', 'wb'))
