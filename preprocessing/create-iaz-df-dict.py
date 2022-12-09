import pickle
import pandas as pd
from tqdm import tqdm

data = pickle.load(open('./sps_spotinfo_df.pkl', 'rb'))

frequency_map = {'<5%': 5, '5-10%': 4, '10-15%': 3, '15-20%': 2, '>20%': 1}
data = data.replace({'Frequency': frequency_map})

data['i-az'] = data['InstanceType'] + '_' + data['AvailabilityZoneId']
iaz_list = data['i-az'].unique()
data = data.set_index('i-az')

iaz_df_dict = {}
for iaz in tqdm(iaz_list):
    cond_df = data.loc[iaz]
    iaz_df_dict[iaz] = cond_df
    
pickle.dump(iaz_df_dict, open('./iaz_df_dict.pkl', 'wb'))
