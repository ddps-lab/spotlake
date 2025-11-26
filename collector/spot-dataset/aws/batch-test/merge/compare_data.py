# ------ import module ------
import pandas as pd
import numpy as np

# ------ import user module ------
try:
    from slack_msg_sender import send_slack_message
except ImportError:
    print("Warning: slack_msg_sender not found. Slack notifications will be disabled.")
    def send_slack_message(msg):
        print(f"[SLACK] {msg}")

# compare previous collected workload with current collected workload
# return changed workload
def compare(previous_df, current_df, workload_cols, feature_cols):  
    previous_df.loc[:,"Workload"] = previous_df[workload_cols].apply(lambda row: ":".join(row.values.astype(str)), axis=1)
    previous_df.loc[:,"Feature"] = previous_df[feature_cols].apply(lambda row: ":".join(row.values.astype(str)), axis=1)
    current_df.loc[:,"Workload"] = current_df[workload_cols].apply(lambda row: ":".join(row.values.astype(str)), axis=1)
    current_df.loc[:,"Feature"] = current_df[feature_cols].apply(lambda row: ":".join(row.values.astype(str)), axis=1)

    current_indices = current_df[["Workload", "Feature"]].sort_values(by="Workload").index
    current_values = current_df[["Workload", "Feature"]].sort_values(by="Workload").values
    previous_indices = previous_df[["Workload", "Feature"]].sort_values(by="Workload").index
    previous_values = previous_df[["Workload", "Feature"]].sort_values(by="Workload").values
    
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
                send_slack_message(f"{prev_workload}, {curr_workload} workload error")
                print(f"{prev_workload}, {curr_workload} workload error")
                raise Exception("workload error")
            break
        elif prev_idx == len(previous_indices):
            curr_workload = current_values[curr_idx][0]
            curr_feature = current_values[curr_idx][1]
            if curr_workload not in previous_values[:,0]:
                changed_indices.append(current_indices[curr_idx])
                curr_idx += 1
                continue
            else:
                send_slack_message(f"{prev_workload}, {curr_workload} workload error")
                print(f"{prev_workload}, {curr_workload} workload error")
                raise Exception("workload error")
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
                send_slack_message(f"{prev_workload}, {curr_workload} workload error")
                print(f"{prev_workload}, {curr_workload} workload error")
                raise Exception("workload error")
        else:
            if prev_feature != curr_feature:
                changed_indices.append(current_indices[curr_idx])
            curr_idx += 1
            prev_idx += 1
    changed_df = current_df.loc[changed_indices].drop(["Workload", "Feature"], axis=1)
    removed_df = previous_df.loc[removed_indices].drop(["Workload", "Feature"], axis=1)
    
    for col in feature_cols:
        removed_df[col] = 0

    # removed_df have one more column, "Ceased"
    removed_df["Ceased"] = True

    return changed_df, removed_df

# ------ Compare the values of T3 and T2 ------
def compare_max_instance(previous_df, new_df, target_capacity):
    fallback_dict = {50:45, 45:40, 40:35, 35:30, 30:25, 25:20, 20:15, 15:10, 10:5, 5:1, 1:0}
    fallback_val = fallback_dict.get(target_capacity, 0)

    spotlake_df = new_df.copy()

    merged_df = pd.merge(
        spotlake_df,
        previous_df[["InstanceType", "AZ", "SPS", "T3", "T2"]],
        on=["InstanceType", "AZ"],
        how="left",
        suffixes=("", "_prev")
    )

    # Fix SPS when single node SPS
    if target_capacity == 1:
        merged_df["SPS"] = merged_df["SPS"].combine_first(merged_df["SPS_prev"])

    # Merge single node SPS with multi node SPS if (multi node SPS) > (single node SPS)
    merged_df.loc[(merged_df["SPS"] > merged_df["SPS_prev"]), "SPS_prev"] = merged_df["SPS"]

    # Calculate T3
    merged_df["T3"] = np.where(
        merged_df["SPS"] >= 3,
        np.maximum(merged_df["T3"], merged_df["T3_prev"]),
        np.minimum(fallback_val, merged_df["T3_prev"])
    )

    # Calculate T2
    merged_df["T2"] = np.where(
        merged_df["SPS"] >= 2,
        np.maximum(merged_df["T2"], merged_df["T2_prev"]),
        np.minimum(fallback_val, merged_df["T2_prev"])
    )

    if target_capacity == 1:
        # When SPS lower than condition, set T3 or T2 to 0
        merged_df.loc[merged_df["SPS"] <= 2, "T3"] = 0
        merged_df.loc[merged_df["SPS"] < 2, "T2"] = 0
    else:
        # When SPS lower than condition, set T3 or T2 to 0
        merged_df.loc[merged_df["SPS_prev"] <= 2, "T3"] = 0
        merged_df.loc[merged_df["SPS_prev"] < 2, "T2"] = 0
        # Fix SPS to Single node SPS
        merged_df["SPS"] = merged_df["SPS_prev"]

    # Convert to int
    for col in ["SPS", "T2", "T3"]:
        merged_df[col] = merged_df[col].astype("Int64")
    
    # Drop unnecessary columns
    merged_df.drop(columns=["T3_prev", "T2_prev", "SPS_prev"], inplace=True)

    return merged_df
