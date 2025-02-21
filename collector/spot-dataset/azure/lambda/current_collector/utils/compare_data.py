import pandas as pd
import numpy as np

# compare previous collected workload with current collected workload
# return changed workload
def compare(previous_df, current_df, workload_cols, feature_cols):
    previous_df = previous_df.copy()
    current_df = current_df.copy()

    previous_df['OndemandPrice'] = previous_df['OndemandPrice'].fillna(-1)
    current_df['OndemandPrice'] = current_df['OndemandPrice'].fillna(-1)
    previous_df['Savings'] = previous_df['Savings'].fillna(-1)
    current_df['Savings'] = current_df['Savings'].fillna(-1)
    previous_df['IF'] = previous_df['IF'].fillna(-1)
    current_df['IF'] = current_df['IF'].fillna(-1)

    previous_df = previous_df.dropna(axis=0)
    current_df = current_df.dropna(axis=0)

    previous_df.loc[:, 'Workload'] = previous_df[workload_cols].apply(lambda row: ':'.join(row.values.astype(str)), axis=1)
    previous_df.loc[:, 'Feature'] = previous_df[feature_cols].apply(lambda row: ':'.join(row.values.astype(str)), axis=1)
    current_df.loc[:, 'Workload'] = current_df[workload_cols].apply(lambda row: ':'.join(row.values.astype(str)), axis=1)
    current_df.loc[:, 'Feature'] = current_df[feature_cols].apply(lambda row: ':'.join(row.values.astype(str)), axis=1)

    previous_df = previous_df.drop_duplicates(['Workload'])
    current_df = current_df.drop_duplicates(['Workload'])

    current_indices = current_df[['Workload', 'Feature']].sort_values(by='Workload').index
    current_values = current_df[['Workload', 'Feature']].sort_values(by='Workload').values
    previous_indices = previous_df[['Workload', 'Feature']].sort_values(by='Workload').index
    previous_values = previous_df[['Workload', 'Feature']].sort_values(by='Workload').values

    changed_indices = []

    prev_idx = 0
    curr_idx = 0
    while True:
        if (curr_idx == len(current_indices)) and (prev_idx == len(previous_indices)):
            break
        elif curr_idx == len(current_indices):
            prev_workload = previous_values[prev_idx][0]
            if prev_workload not in current_values[:, 0]:
                prev_idx += 1
                continue
            else:
                print(f"{prev_workload}, {curr_workload} workload error")
                raise Exception('workload error')
            break
        elif prev_idx == len(previous_indices):
            curr_workload = current_values[curr_idx][0]
            curr_feature = current_values[curr_idx][1]
            if curr_workload not in previous_values[:, 0]:
                changed_indices.append(current_indices[curr_idx])
                curr_idx += 1
                continue
            else:
                print(f"{prev_workload}, {curr_workload} workload error")
                raise Exception('workload error')
            break

        prev_workload = previous_values[prev_idx][0]
        prev_feature = previous_values[prev_idx][1]
        curr_workload = current_values[curr_idx][0]
        curr_feature = current_values[curr_idx][1]

        if prev_workload != curr_workload:
            if curr_workload not in previous_values[:, 0]:
                changed_indices.append(current_indices[curr_idx])
                curr_idx += 1
            elif prev_workload not in current_values[:, 0]:
                prev_idx += 1
                continue
            else:
                print(f"{prev_workload}, {curr_workload} workload error")
                raise Exception('workload error')
        else:
            if prev_feature != curr_feature:
                changed_indices.append(current_indices[curr_idx])
            curr_idx += 1
            prev_idx += 1

    current_df = current_df.loc[changed_indices].drop(['Workload', 'Feature'], axis=1)

    return current_df


def compare_sps(previous_df, current_df, workload_cols, feature_cols):
    previous_df = previous_df.copy()
    current_df = current_df.copy()

    fill_values = {
        'OndemandPrice': -1,
        'Savings': -1,
        'IF': -1,
        'Score': -1,
        'AvailabilityZone': -1,
        'DesiredCount': -1,
        'SPS_Update_Time': -1
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