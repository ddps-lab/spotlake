import os
import time
import tsquery

# compress data as gzip file, save to local file system, upload file to s3
def save_gz_s3(data):
    # compress to gz
    # save to local file system
    # upload to s3

# compare previous_df and current_df for changed row detection
def compare(previous_df, current_df):
    previous_df['Workload'] = previous_df['InstanceType'] + ":" + previous_df['Region'] + ":" + previous_df['AZ']
    previous_df['Feature'] = previous_df['SPS'] + ":" + previous_df['IF'] + ":" + previous_df['SpotPrice'] + ":" + previous_df['Savings']
    current_df['Workload'] = current_df['InstanceType'] + ":" + current_df['Region'] + ":" + current_df['AZ']
    current_df['Feature'] = current_df['SPS'] + ":" + current_df['IF'] + ":" + current_df['SpotPrice'] + ":" + current_df['Savings']
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

# write changed row dataframe to tiemstream
def write_timestream(data):
    # write

if __name__ == "__main__":
    # get every unique timestamp information as a list, elapsed time: 10s for 7months
    start_date = '2022-01-01'
    end_date = '2022-08-01'
    timestamp_list = tsquery.get_timestamps(start_date, end_date)
    
    # query for single timestamp data, elapsed time: 12s
    for timestamp in timestamp_list:
        timestamp = 'T'.join(timestamp.split())
        current_df = tsquery.get_timestream(timestamp, timestamp)    
        
        if 'latest_df.pkl' not in os.listdir('./'):
            save_gz_s3(current_data)
            write_timestream(changed_df)
        else:    
            previous_df = pickle.load(open('./latest_df.pkl', 'rb'))
            save_gz_s3(current_data)
            changed_df = compare(previous_df, current_df)
            write_timestream(changed_df)
