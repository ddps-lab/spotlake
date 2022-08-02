import os
import time
import boto3
import tsquery
import tsupload
import pandas as pd


SAVE_FILENAME = 'latest.csv.gz'
PROFILE_NAME = 'default'
BUCKET_NAME = 'spotlake'
START_DATE = '2022-01-01'
END_DATE = '2022-08-01'


# compress data as gzip file, save to local file system, upload file to s3
def save_gz_s3(df, timestamp):
    # compress and save to LFS
    df.to_csv(SAVE_FILENAME, index=False, compression="gzip")
    
    # upload compressed file to S3
    session = boto3.Session(profile_name=PROFILE_NAME)
    s3 = session.client('s3')
    s3_dir_name = '/'.join(timestamp.split('T')[0].split('-'))
    s3_obj_name = timestamp.split('T')[1]
    
    with open(SAVE_FILENAME, 'rb') as f:
        s3.upload_fileobj(f, BUCKET_NAME, f"rawdata/{s3_dir_name}/{s3_obj_name}.csv.gz")


# compare previous_df and current_df for changed row detection
def compare(previous_df, current_df):
    previous_df['Workload'] = previous_df['InstanceType'] + ":" + previous_df['Region'] + ":" + previous_df['AZ']
    previous_df['Feature'] = previous_df['SPS'].astype(str) + ":" + previous_df['IF'] + ":" + previous_df['Savings'].astype(str)
    current_df['Workload'] = current_df['InstanceType'] + ":" + current_df['Region'] + ":" + current_df['AZ']
    current_df['Feature'] = current_df['SPS'].astype(str) + ":" + current_df['IF'] + ":" + current_df['Savings'].astype(str)
    changed_idx_list = []
    for idx, row in current_df.iterrows():
        cmp = previous_df[previous_df['Workload'] == row['Workload']]
        if len(cmp) == 0:
            changed_idx_list.append(idx)
            continue
        cmp = cmp.iloc[0]
        if cmp['Feature'] != row['Feature']:
            changed_idx_list.append(idx)
    changed_df = current_df.loc[changed_idx_list]
    return changed_df[changed_df.columns.difference(['Workload', 'Feature'])]


if __name__ == "__main__":
    # get every unique timestamp information as a list
    timestamp_list = tsquery.get_timestamps(START_DATE, END_DATE)

    for timestamp in timestamp_list[:10]:
        timestamp = 'T'.join(timestamp.split())
        current_df = tsquery.get_timestream(timestamp, timestamp)

        if SAVE_FILENAME not in os.listdir('./'):
            save_gz_s3(current_df, timestamp)
            tsupload.upload_timestream(current_df, PROFILE_NAME)
        else:
            previous_df = pd.read_csv(SAVE_FILENAME, compression='gzip', header=0, sep=',', quotechar='"')
            save_gz_s3(current_df, timestamp)
            changed_df = compare(previous_df, current_df)
            tsupload.upload_timestream(changed_df, PROFILE_NAME)
