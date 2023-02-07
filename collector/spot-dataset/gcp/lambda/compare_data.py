import pandas as pd
from utility import slack_msg_sender

# compare previous collected workload with current collected workload
# return changed workload
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
                slack_msg_sender.send_slack_message(f"{prev_workload}, {curr_workload} workload error")
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
                slack_msg_sender.send_slack_message(f"{prev_workload}, {curr_workload} workload error")
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
                slack_msg_sender.send_slack_message(f"{prev_workload}, {curr_workload} workload error")
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

