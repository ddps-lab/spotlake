import boto3
import pickle
import pandas as pd
from tqdm import tqdm
from io import StringIO

session = boto3.session.Session(profile_name='dev-session')
s3_client = session.client('s3')
s3_resource = session.resource('s3')

BUCKET_SPOTINFO = 'sungjae-spotinfo-data'

pagenator = s3_client.get_paginator('list_objects').paginate(Bucket=BUCKET_SPOTINFO)
pages = [page for page in pagenator]
contents = [content['Contents'] for content in pages]
flat_contents = [content for sublist in contents for content in sublist]
spotinfo_objects = [(x['Key'], x['LastModified']) for x in flat_contents if 'txt' in x['Key']]

spotinfo_df_list = []
for spotinfo_filename, spotinfo_datetime in tqdm(spotinfo_objects):
    spotinfo_object = s3_resource.Object(bucket_name=BUCKET_SPOTINFO, key=spotinfo_filename)
    spotinfo_response = spotinfo_object.get()
    
    spotinfo_df = pd.read_csv(StringIO(spotinfo_response['Body'].read().decode('utf-8')), skiprows=1)
    spotinfo_df = spotinfo_df[['Region', 'Instance Info', 'Frequency of interruption', 'USD/Hour']]
    spotinfo_df = spotinfo_df.rename(columns={'Instance Info': 'InstanceType',
                                              'Frequency of interruption': 'Frequency',
                                              'USD/Hour': 'Price'})
    spotinfo_df = spotinfo_df[['InstanceType', 'Region', 'Frequency', 'Price']]
    spotinfo_df['TimeStamp'] = spotinfo_datetime
    spotinfo_df = spotinfo_df.sort_values(by=['InstanceType', 'Region'], ignore_index=True)
    spotinfo_df_list.append(spotinfo_df)
