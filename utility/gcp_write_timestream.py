import boto3
from botocore.config import Config
import time
import os
import pandas as pd
import gzip
from datetime import datetime

### write stored rawdata on tsdb
### before this, have to aws sync spotlake bucket
### plz check file paths, DATABASE_NAME and TABLE_NAME before running this

session = boto3.session.Session(region_name='us-west-2')
write_client = session.client('timestream-write', config=Config(read_timeout=20, max_pool_connections=5000, retries={'max_attempts':10}))
client = session.client('timestream-query')

DATABASE_NAME = 'spotlake'
TABLE_NAME = 'gcp'

FILE_PATH = '/home/ubuntu/gcp_rawdata'          # 2022/MM/dd
TEMP_FILE_PATH = './'
NEW_FILE_PATH = '/home/ubuntu/gcp_newrawdata'  
workload_cols = ['InstanceType', 'Region']
feature_cols = ['Calculator OnDemand Price', 'Calculator Preemptible Price', 'VM Instance OnDemand Price', 'VM Instance Preemptible Price']


# Submit Batch To Timestream
def submit_batch(records, counter, recursive):
    if recursive == 10:
        return
    try:
        result = write_client.write_records(DatabaseName=DATABASE_NAME, TableName = TABLE_NAME, Records=records, CommonAttributes={})
    except write_client.exceptions.RejectedRecordsException as err:
        re_records = []
        for rr in err.response["RejectedRecords"]:
            print(rr['Reason'])
            re_records.append(records[rr["RecordIndex"]])
        submit_batch(re_records, counter, recursive + 1)
    except Exception as err:
        print(err)
        exit()


# Check Database And Table Are Exist and Upload Data to Timestream
def upload_timestream(data, timestamp):
    print(len(data))

    time_value = time.strptime(timestamp.strftime("%Y-%m-%d %H:%M"), '%Y-%m-%d %H:%M')
    time_value = time.mktime(time_value)
    time_value = str(int(round(time_value * 1000)))


    records = []
    counter = 0
    for idx, row in data.iterrows():

        dimensions = []
        for column in data.columns:
            if column in ['InstanceType', 'Region', 'Ceased']:
                dimensions.append({'Name':column, 'Value': str(row[column])})

        submit_data = {
                'Dimensions': dimensions,
                'MeasureName': 'gcp_values',
                'MeasureValues': [],
                'MeasureValueType': 'MULTI',
                'Time': time_value
        }
        for column, types in [('Calculator OnDemand Price', 'DOUBLE'), ('Calculator Preemptible Price', 'DOUBLE'), ('VM Instance OnDemand Price', 'DOUBLE'), ('VM Instance Preemptible Price', 'DOUBLE')]:
            submit_data['MeasureValues'].append({'Name': column, 'Value': str(row[column]), 'Type' : types})
        records.append(submit_data)
        counter += 1
        if len(records) == 100:
            submit_batch(records, counter, 0)
            records = []

    if len(records) != 0:
        submit_batch(records, counter, 0)
    
    print(f"end : {counter}")


def compare(previous_df, current_df, workload_cols, feature_cols):  
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
                continue
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


# write first rawdata to timestream

paths = []
for (path, dir, files) in os.walk(NEW_FILE_PATH):
    for filename in files:
        final_path = path + '/' + filename
        paths.append(final_path)
paths.sort()


path  = paths[0]

changed_time = path.split('gcp_newrawdata/')[1].split('.csv.gz')[0]
timestamp = datetime.strptime(changed_time, '%Y/%m/%d/%H:%M:%S')
print(timestamp)
df = pd.DataFrame()
with gzip.open(path, 'rb') as f:
    df = pd.read_csv(f)

upload_timestream(df, timestamp)

for i in range (1, len(paths)):
    prev_path = paths[i-1]
    curr_path = paths[i]
    changed_time = curr_path.split('gcp_newrawdata/')[1].split('.csv.gz')[0]
    timestamp = datetime.strptime(changed_time, '%Y/%m/%d/%H:%M:%S')

    df_prev = pd.DataFrame()
    df_curr = pd.DataFrame()
    
    with gzip.open(prev_path, 'rb') as f:
        df_prev = pd.read_csv(f)
    with gzip.open(curr_path, 'rb') as f:
        df_curr = pd.read_csv(f)
    
    changed_df, removed_df = compare(df_prev, df_curr, workload_cols, feature_cols)
    upload_timestream(changed_df, timestamp)
    upload_timestream(removed_df, timestamp)