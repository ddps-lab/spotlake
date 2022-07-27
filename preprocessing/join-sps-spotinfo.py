import pickle
import pandas as pd
from tqdm import tqdm

# filtering data with date range
date_range = ('2021-11-23', '2021-12-10')

# load and filter dataset
sps = pickle.load(open('./sps_df.pkl', 'rb'))
spotinfo = pickle.load(open('./spotinfo_df.pkl', 'rb'))

sps = sps[(sps['TimeStamp'] >= date_range[0]) & (sps['TimeStamp'] <= date_range[1])]
spotinfo = spotinfo[(spotinfo['TimeStamp'] >= date_range[0]) & (spotinfo['TimeStamp'] <= date_range[1])]

# matching timestamps
sps_timestamps = sps['TimeStamp'].unique()
spotinfo_timestamps = spotinfo['TimeStamp'].unique()
sps_spotinfo_matching = []
for spotinfo_timestamp in tqdm(spotinfo_timestamps):
    try:
        sps_timestamp = min(filter(lambda i: i > spotinfo_timestamp, sps_timestamps))
        sps_spotinfo_matching.append([sps_timestamp, spotinfo_timestamp])
    except:
        print(spotinfo_timestamp)
        
# inner join on matched timestamp dataframes
merge_df_list = []
for sps_timestamp, spotinfo_timestamp in tqdm(sps_spotinfo_matching):
    sps_time_df = sps[sps['TimeStamp'] == sps_timestamp]
    spotinfo_time_df = spotinfo[spotinfo['TimeStamp'] == spotinfo_timestamp]
    merge_df = sps_time_df.merge(spotinfo_time_df,
                                 how='inner',
                                 on = ['InstanceType', 'Region'],
                                 suffixes=('_sps', '_spotinfo'))
    merge_df_list.append(merge_df)
merge_df = pd.concat(merge_df_list)
merge_df = merge_df[['InstanceType', 'Region', 'AvailabilityZoneId', 'Score', 'Frequency', 'Price', 'TimeStamp_sps', 'TimeStamp_spotinfo']]

pickle.dump(merge_df, open('./sps_spotinfo_df.pkl', 'wb'))
