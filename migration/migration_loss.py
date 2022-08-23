import os
import boto3
import tsupload
import pandas as pd

import time
import pytz
from datetime import datetime, timedelta

SAVE_FILENAME = 'latest.csv.gz'
PROFILE_NAME = 'default'
BUCKET_NAME = 'spotlake'
REGION_NAME = "us-west-2"
DATABASE_NAME = 'spotlake'
TABLE_NAME = 'aws'

start_date = datetime(2022, 4, 13, 0, 0, 0, 0, pytz.UTC)
end_date = datetime(2022, 5, 1, 0, 0, 0, 0, pytz.UTC)

workload_cols = ['InstanceType', 'Region', 'AZ']
feature_cols = ['SPS', 'IF', 'SpotPrice']

tsupload.PROFILE_NAME = PROFILE_NAME
tsupload.REGION_NAME = REGION_NAME
tsupload.DATABASE_NAME = DATABASE_NAME
tsupload.TABLE_NAME = TABLE_NAME

# compress data as gzip file, save to local file system, upload file to s3
def save_gz_s3(df, timestamp):
    # compress and save to LFS
    df.to_csv(SAVE_FILENAME, index=False, compression="gzip")
    
    # upload compressed file to S3
    session = boto3.Session(profile_name=PROFILE_NAME)
    s3 = session.client('s3')
    s3_dir_name = '/'.join(timestamp.split()[0].split('-'))
    s3_obj_name = timestamp.split()[1]
    
    with open(SAVE_FILENAME, 'rb') as f:
        s3.upload_fileobj(f, BUCKET_NAME, f"rawdata/{s3_dir_name}/{s3_obj_name}.csv.gz")

def compare_nparray(previous_df, current_df, workload_cols, feature_cols):  
    previous_df.loc[:,'Workload'] = previous_df[workload_cols].apply(lambda row: ':'.join(row.values.astype(str)), axis=1)
    previous_df.loc[:,'Feature'] = previous_df[feature_cols].apply(lambda row: ':'.join(row.values.astype(str)), axis=1)
    current_df.loc[:,'Workload'] = current_df[workload_cols].apply(lambda row: ':'.join(row.values.astype(str)), axis=1)
    current_df.loc[:,'Feature'] = current_df[feature_cols].apply(lambda row: ':'.join(row.values.astype(str)), axis=1)

    current_indices = current_df[['Workload', 'Feature']].sort_values(by='Workload').index
    current_values = current_df[['Workload', 'Feature']].sort_values(by='Workload').values
    previous_indices = previous_df[['Workload', 'Feature']].sort_values(by='Workload').index
    previous_values = previous_df[['Workload', 'Feature']].sort_values(by='Workload').values
    
    changed_indices = []
    removed_indices = []
    
    prev_idx = 0
    curr_idx = 0
    while True:
        if (curr_idx == len(current_indices)) and (prev_idx == len(previous_indices)):
            break
        elif curr_idx == len(current_indices):
            prev_workload = previous_values[prev_idx][0]
            if prev_workload not in current_values[:,0]:
                removed_indices.append(previous_indices[prev_idx])
                prev_idx += 1
                continue
            else:
                raise Exception('workload error')
            break
        elif prev_idx == len(previous_indices):
            curr_workload = current_values[curr_idx][0]
            curr_feature = current_values[curr_idx][1]
            if curr_workload not in previous_values[:,0]:
                changed_indices.append(current_indices[curr_idx])
                curr_idx += 1
                continue
            else:
                raise Exception('workload error')
            break
            
        prev_workload = previous_values[prev_idx][0]
        prev_feature = previous_values[prev_idx][1]
        curr_workload = current_values[curr_idx][0]
        curr_feature = current_values[curr_idx][1]
        
        if prev_workload != curr_workload:
            if curr_workload not in previous_values[:,0]:
                changed_indices.append(current_indices[curr_idx])
                curr_idx += 1
            elif prev_workload not in current_values[:,0]:
                removed_indices.append(previous_indices[prev_idx])
                prev_idx += 1
            else:
                raise Exception('workload error')
        else:
            if prev_feature != curr_feature:
                changed_indices.append(current_indices[curr_idx])
            curr_idx += 1
            prev_idx += 1
    
    changed_df = current_df.loc[changed_indices].drop(['Workload', 'Feature'], axis=1)
    removed_df = previous_df.loc[removed_indices].drop(['Workload', 'Feature'], axis=1)
    for col in feature_cols:
        removed_df[col] = 0

    # removed_df have one more column, 'Ceased'
    removed_df['Ceased'] = True
    return changed_df, removed_df
        
def date_range(start, end):
    delta = end - start
    days = [start + timedelta(days=i) for i in range(delta.days + 1)]
    return days

def time_format(timestamp):
    return 'T'.join(str(timestamp).split())
  
days = date_range(start_date, end_date)
all_df = pd.read_pickle('./df_0413_0501.pkl')
perf_start_total = time.time()
for day in days:
    perf_start = time.time()
    day_cond = (str(day) <= all_df['time'] ) & (all_df['time'] < str(day + timedelta(days=1)))
    day_df = all_df[day_cond].copy()
    frequency_map = {'<5%': 3.0, '5-10%': 2.5, '10-15%': 2.0, '15-20%': 1.5, '>20%': 1.0}
    day_df = day_df.replace({'IF': frequency_map})
    day_df['SPS'] = day_df['SPS'].astype(int)
    day_df['SpotPrice'] = day_df['SpotPrice'].astype(float)
    day_df['SpotPrice'] = day_df['SpotPrice'].round(5)
    
    print(f"elapsed time - single day query: {time.time() - perf_start}")
    # day_df['OndemandPrice'] = (100 * day_df['SpotPrice']) / (100 - day_df['Savings'])
    
    day_timestamps = sorted(list(day_df['time'].unique()))
    for timestamp in day_timestamps:
        perf_start = time.time()
        current_df = day_df[day_df['time'] == timestamp].copy()
        print(f"elapsed time - select by time: {time.time() - perf_start}")
        if SAVE_FILENAME not in os.listdir('./'):
            save_gz_s3(current_df, timestamp)
            tsupload.upload_timestream(current_df)
        else:
            perf_start = time.time()
            previous_df = pd.read_csv(SAVE_FILENAME, compression='gzip', header=0, sep=',', quotechar='"')
            save_gz_s3(current_df, timestamp)
            print(f"elapsed time - read and save: {time.time() - perf_start}")
            perf_start = time.time()
            changed_df, removed_df = compare_nparray(previous_df, current_df, workload_cols, feature_cols)
            print(f"elapsed time - compare: {time.time() - perf_start}")
            perf_start = time.time()
            # changed_df and removed_df have different shape, because of 'Ceased' column
            tsupload.upload_timestream(changed_df)
            tsupload.upload_timestream(removed_df)
            print(f"elapsed time - upload: {time.time() - perf_start}")
print(f"elapsed time - total single day: {time.time() - perf_start_total}")
