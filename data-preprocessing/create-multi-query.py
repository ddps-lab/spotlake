import pickle
import pandas as pd

# Load dataset
data = pickle.load(open('./sps_spotinfo_df.pkl', 'rb'))
last_date = data['TimeStamp_spotinfo'].iloc[-1]
data = data[data['TimeStamp_spotinfo'] == last_date]
data = data[['InstanceType', 'Region', 'AvailabilityZoneId', 'Score']]

# define score sum, combination of scores for specific sum
score_sum = {
    3: [{1: 3}],
    4: [{1: 2, 2: 1}],
    5: [{1: 2, 3: 1}, {1: 1, 2: 2}],
    6: [{1: 1, 2: 1, 3: 1}, {2: 3}],
    7: [{1: 1, 3: 2}, {2: 2, 3: 1}],
    8: [{2: 1, 3: 2}],
    9: [{3: 3}]
}

# build az-score dataframe using dictionary
az_list = list(data['AvailabilityZoneId'].unique())
az_score_df_dict = {}
for az in az_list:
    print(az)
    az_df = data[data['AvailabilityZoneId'] == az]
    print(len(az_df))
    for score in range(3):
        az_score_df_dict[(az, score+1)] = az_df[az_df['Score'] == score+1]
        print(f"{score+1} : {len(az_df[az_df['Score'] == score+1])}")

# find query using iteration and sampling
def find_query(score_comb, target_query_num):
    query_list = []
    while True:
        for az in az_list:
            for comb in score_comb:
                query = []
                sample_df_list = []
                for score, num in comb.items():
                    cond_df = az_score_df_dict[(az, score)]
                    if len(cond_df) < num:
                        break
                    sample_df = cond_df.sample(num)
                    sample_instance = list(sample_df['InstanceType'].values)
                    sample_region = sample_df['Region'].values[0]
                    sample_az = sample_df['AvailabilityZoneId'].values[0]
                    query.extend(sample_instance)
                if len(query) != 3:
                    continue
                query.append(sample_region)
                query_str = '_'.join(query)
                if query_str not in query_set:
                    print(query)
                    query_set.append(query_str)
                    query_list.append(query)
                    if len(query_list) == target_query_num:
                        return query_list
                      
# query generation
target_query_num = 200
score_query_dict = {}
query_set = []
for target_score, score_comb in score_sum.items():
    query_list = find_query(score_comb, target_query_num)
    score_query_dict[target_score] = query_list
    
# check for duplicated query
all_result_list = [v for k, v in score_query_dict.items()]
all_result_list = ['_'.join(x) for sublist in all_result_list for x in sublist]
print(len(all_result_list))
print(len(set(all_result_list)))
