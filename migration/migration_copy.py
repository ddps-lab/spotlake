import os
import boto3
import tsquery
import tsupload
import pandas as pd
from multiprocessing import Pool

import time
import pytz
from datetime import datetime, timedelta


SAVE_FILENAME = 'latest.csv.gz'
PROFILE_NAME = 'default'
BUCKET_NAME = 'spotlake'
REGION_NAME = "us-west-2"
QUERY_DATABASE_NAME = "spotlake"
QUERY_TABLE_NAME = "temp"
UPLOAD_DATABASE_NAME = 'spotlake'
UPLOAD_TABLE_NAME = 'aws'
NUM_CPUS = 8
if 24 % NUM_CPUS != 0:
    raise Exception('use only 1, 2, 3, 4, 6, 8, 12, 24')
CHUNK_HOUR = 24 / NUM_CPUS

start_date = datetime(2022, 1, 1, 0, 0, 0, 0, pytz.UTC)
end_date = datetime(2022, 8, 23, 0, 0, 0, 0, pytz.UTC)

tsquery.PROFILE_NAME = PROFILE_NAME # tsquery.PROFILE_NAME must be credential of source database
tsquery.REGION_NAME = REGION_NAME
tsquery.DATABASE_NAME = QUERY_DATABASE_NAME
tsquery.TABLE_NAME = QUERY_TABLE_NAME
tsupload.PROFILE_NAME = PROFILE_NAME
tsupload.REGION_NAME = REGION_NAME
tsupload.DATABASE_NAME = UPLOAD_DATABASE_NAME
tsupload.TABLE_NAME = UPLOAD_TABLE_NAME


def date_range(start, end):
    delta = end - start
    days = [start + timedelta(days=i) for i in range(delta.days + 1)]
    return days


def time_format(timestamp):
    return 'T'.join(str(timestamp).split())


days = date_range(start_date, end_date)

perf_start_total = time.time()
for idx in range(len(days)-1):
    perf_start = time.time()
    start_timestamp = days[idx]
    end_timestamp = days[idx+1]
    
    start_end_time_process_list = []
    for i in range(NUM_CPUS):
        start_time_process = start_timestamp + timedelta(hours = CHUNK_HOUR*i)
        end_time_process = start_timestamp + timedelta(hours = CHUNK_HOUR*(i+1))
        start_end_time_process_list.append((time_format(start_time_process), time_format(end_time_process)))
        
    with Pool(NUM_CPUS) as p:
        process_df_list = p.starmap(tsquery.get_timestream, start_end_time_process_list)
        
    day_df = pd.concat(process_df_list, axis=0, ignore_index=True)
    day_df['SPS'] = day_df['SPS'].astype(int)
    day_df['SpotPrice'] = day_df['SpotPrice'].astype(float)
    day_df['SpotPrice'] = day_df['SpotPrice'].round(5)

    tsupload.upload_timestream(day_df)
    print(f"elapsed time - single day query: {time.time() - perf_start}")
print(f"elapsed time - total: {time.time() - perf_start_total}")
