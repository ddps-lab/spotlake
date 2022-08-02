import os
import time
import tsquery

BUCKET_NAME = 'spotlake'

# compress data as gzip file, save to local file system, upload file to s3
def save_gz_s3(df, filename):
    # compress and save to LFS
    df.to_csv(filename, index=False, compression="gzip")
    
    # upload compressed file to S3
    s3 = boto3.client('s3')
    with open(filename, 'rb') as f:
        s3.upload_fileobj(f, BUCKET_NAME, f"rawdata/{filename}")

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
    # get every unique timestamp information as a list
    start_date = '2022-01-01'
    end_date = '2022-08-01'
    timestamp_list = tsquery.get_timestamps(start_date, end_date)

    for timestamp in timestamp_list:
        timestamp = 'T'.join(timestamp.split())
        print(timestamp)
        current_df = tsquery.get_timestream(timestamp, timestamp)

        if 'latest_df.pkl' not in os.listdir('./'):
            save_gz_s3(current_df, 'latest.csv.gz')
            write_timestream(current_df)
        else:    
            previous_df = pd.read_csv('latest.csv.gz', compression='gzip', header=0, sep=',', quotechar='"', error_bad_lines=False)
            save_gz_s3(current_df, 'latest.csv.gz')
            changed_df = compare(previous_df, current_df)
            write_timestream(changed_df)
