import pandas as pd

def compare(previous_df, current_df):
    previous_df = pd.DataFrame(eval(previous_df))
    previous_df['Workload'] = previous_df['InstanceType'] + ":" + previous_df['Region'] + ":" + previous_df['AZ']
    previous_df['Feature'] = previous_df['SPS'] + ":" + previous_df['IF'] + ":" + previous_df['SpotPrice'] + ":" + previous_df['Savings']
    current_df['Workload'] = current_df['InstanceType'] + ":" + current_df['Region'] + ":" + current_df['AZ']
    current_df['Feature'] = current_df['SPS'] + ":" + current_df['IF'] + ":" + current_df['Savings']
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
