import pandas as pd
import numpy as np

# compare previous collected workload with current collected workload
# return changed workload
def compare_sps(previous_df, current_df, workload_cols, feature_cols):
    previous_df = previous_df.copy()
    current_df = current_df.copy()

    fill_values = {
        'OndemandPrice': -1,
        'Savings': -1,
        'IF': -1,
        'DesiredCount': -1,
        'AvailabilityZone': 'NaN',
        'Score': 'NaN',
        'SPS_Update_Time': 'NaN'
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

    return changed_df if not changed_df.empty else None