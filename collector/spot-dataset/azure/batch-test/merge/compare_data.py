import pandas as pd
import numpy as np

# compare previous collected workload with current collected workload
# return changed workload
def compare_sps(previous_df, current_df, workload_cols, feature_cols):
    previous_df = previous_df.copy().astype(object)
    current_df = current_df.copy().astype(object)

    fill_values = {
        'OndemandPrice': -1,
        'Savings': -1,
        'IF': -1,
        'DesiredCount': -1,
        'AvailabilityZone': 'N/A',
        'Score': 'N/A',
        'Time': 'N/A'
    }
    previous_df = previous_df.fillna(fill_values)
    current_df = current_df.fillna(fill_values)

    previous_df = previous_df.dropna(axis=0)
    current_df = current_df.dropna(axis=0)

    previous_df['Workload'] = previous_df[workload_cols].astype(str).agg(':'.join, axis=1)
    previous_df['Feature'] = previous_df[feature_cols].astype(str).agg(':'.join, axis=1)
    current_df['Workload'] = current_df[workload_cols].astype(str).agg(':'.join, axis=1)
    current_df['Feature'] = current_df[feature_cols].astype(str).agg(':'.join, axis=1)

    previous_df = previous_df.drop_duplicates(subset=['Workload'])
    current_df = current_df.drop_duplicates(subset=['Workload'])

    # current_df와 previous_df 를 merge 방법으로 비교
    merged_df = current_df.merge(
        previous_df[['Workload', 'Feature']],
        on='Workload',
        how='left',  # previous_df 기준으로 병합
        suffixes=('_curr', '_prev')
    )

    # 변경된 행 필터링
    changed_df = merged_df[
        # Workload가 새로 추가된 경우 (previous_df에 존재하지 않음)
        (merged_df['Feature_prev'].isna()) |
        # Feature 값이 변경된 경우 (Workload는 존재하지만 Feature 값이 다름)
        ((merged_df['Feature_prev'].notna()) & (merged_df['Feature_curr'] != merged_df['Feature_prev']))
        ]
    
    current_df = current_df.drop(columns=["Feature", "Workload"], errors="ignore")
    changed_df = changed_df[current_df.columns]

    return changed_df if not changed_df.empty else pd.DataFrame(columns=current_df.columns)

def compare_max_instance(previous_df, new_df, target_capacity):
    fallback_dict = {50:45, 45:40, 40:35, 35:30, 30:25, 25:20, 20:15, 15:10, 10:5, 5:1, 1:0}
    fallback_val = fallback_dict.get(target_capacity, 0)
    
    # Ensure T2/T3 columns exist in previous_df for legacy compatibility
    for col in ["T2", "T3"]:
        if col not in previous_df.columns:
            previous_df[col] = 0

    merged_df = pd.merge(
        new_df,
        previous_df[["InstanceType", "Region", "AvailabilityZone", "DesiredCount", "Score", "T3", "T2"]],
        on=["InstanceType", "Region", "AvailabilityZone", "DesiredCount"],
        how="left",
        suffixes=("", "_prev")
    )

    if target_capacity == 1:
        merged_df["Score"] = merged_df["Score"].combine_first(merged_df["Score_prev"])
    
    merged_df["Score"] = pd.to_numeric(merged_df["Score"], errors='coerce')
    merged_df["Score_prev"] = pd.to_numeric(merged_df["Score_prev"], errors='coerce')
    merged_df["T3"] = pd.to_numeric(merged_df["T3"], errors='coerce').fillna(0)
    merged_df["T3_prev"] = pd.to_numeric(merged_df["T3_prev"], errors='coerce').fillna(0)
    merged_df["T2"] = pd.to_numeric(merged_df["T2"], errors='coerce').fillna(0)
    merged_df["T2_prev"] = pd.to_numeric(merged_df["T2_prev"], errors='coerce').fillna(0)

    merged_df.loc[(merged_df["Score"] > merged_df["Score_prev"]), "Score_prev"] = merged_df["Score"]

    merged_df["T3"] = np.where(
        merged_df["Score"] >= 3,
        np.maximum(merged_df["T3"], merged_df["T3_prev"]),
        np.minimum(fallback_val, merged_df["T3_prev"])
    )

    merged_df["T2"] = np.where(
        merged_df["Score"] >= 2,
        np.maximum(merged_df["T2"], merged_df["T2_prev"]),
        np.minimum(fallback_val, merged_df["T2_prev"])
    )
    
    if target_capacity == 1:
        merged_df.loc[merged_df["Score"] <= 2, "T3"] = 0
        merged_df.loc[merged_df["Score"] < 2, "T2"] = 0
    else:
        merged_df.loc[merged_df["Score_prev"] <= 2, "T3"] = 0
        merged_df.loc[merged_df["Score_prev"] < 2, "T2"] = 0
        merged_df["Score"] = merged_df["Score_prev"].fillna(merged_df["Score"])

    for col in ["T2", "T3"]:
        merged_df[col] = merged_df[col].astype(int)
    
    merged_df["Score"] = pd.to_numeric(merged_df["Score"], errors='coerce').astype("Int64")

    merged_df.drop(columns=["T3_prev", "T2_prev", "Score_prev"], errors='ignore', inplace=True)

    return merged_df
