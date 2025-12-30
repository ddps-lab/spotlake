import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from collections import defaultdict
import sys
import os

# Add parent directory to path to import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sps_module import sps_shared_resources

SS_Resources = sps_shared_resources

def filter_invalid_parameter(regions_and_instance_types_df):
    regions_and_instance_types_del1_invalid_df = regions_and_instance_types_df[
        ~regions_and_instance_types_df['RegionCode'].isin(SS_Resources.invalid_regions_tmp) &
        ~regions_and_instance_types_df['InstanceType'].isin(SS_Resources.invalid_instance_types_tmp)
        ]

    return regions_and_instance_types_del1_invalid_df


def greedy_clustering_to_create_optimized_request_list(regions_and_instance_types_df):
    support_set, regions, instancetypes = load_support_data_from_df(regions_and_instance_types_df)

    queries = clustering_cover_remaining_greedily(support_set, regions, instancetypes)

    df_greedy_clustered = pd.DataFrame(
        [{'Regions': list(regions), 'InstanceTypes': list(instancetypes)} for regions, instancetypes in
         queries]
    )
    return df_greedy_clustered


def load_support_data_from_df(df):
    support_set = set()
    regions = set()
    instancetypes = set()

    for row in df.itertuples(index=False):
        region = row.RegionCode
        itype = row.InstanceType
        support_set.add((region, itype))
        regions.add(region)
        instancetypes.add(itype)

    return support_set, sorted(regions), sorted(instancetypes)


def clustering_cover_remaining_greedily(support_set, regions, instancetypes, n_region_clusters=2, n_inst_clusters=2):
    region_to_idx = {r: idx for idx, r in enumerate(regions)}
    inst_to_idx = {i: idx for idx, i in enumerate(instancetypes)}

    R_matrix = np.zeros((len(regions), len(instancetypes)), dtype=int)
    for (r, i) in support_set:
        R_matrix[region_to_idx[r], inst_to_idx[i]] = 1

    I_matrix = R_matrix.T

    region_clusters = KMeans(n_clusters=n_region_clusters, random_state=0).fit(R_matrix)
    inst_clusters = KMeans(n_clusters=n_inst_clusters, random_state=0).fit(I_matrix)

    region_cluster_map = defaultdict(list)
    for idx, c in enumerate(region_clusters.labels_):
        region_cluster_map[c].append(regions[idx])

    inst_cluster_map = defaultdict(list)
    for idx, c in enumerate(inst_clusters.labels_):
        inst_cluster_map[c].append(instancetypes[idx])

    remaining = set(support_set)
    queries = []

    for rc, rgroup in region_cluster_map.items():
        for ic, igroup in inst_cluster_map.items():
            subset = {(r, i) for (r, i) in remaining if r in rgroup and i in igroup}
            if not subset:
                continue

            region_freq = defaultdict(int)
            inst_freq = defaultdict(int)
            for (r, i) in subset:
                region_freq[r] += 1
                inst_freq[i] += 1

            sel_regions = sorted(rgroup, key=lambda x: region_freq[x], reverse=True)[:8]
            sel_insts = sorted(igroup, key=lambda x: inst_freq[x], reverse=True)[:5]

            covered = {(r, i) for (r, i) in subset if r in sel_regions and i in sel_insts}
            if covered:
                queries.append((set(sel_regions), set(sel_insts)))
                remaining -= covered

    if remaining:
        queries += _cover_remaining_greedily(remaining)

    return queries


def _cover_remaining_greedily(remaining):
    queries = []
    while remaining:
        r, i = remaining.pop()
        current_r = {r}
        current_i = {i}
        covered = {(r, i)}

        remaining -= covered

        while (len(current_r) < 8 or len(current_i) < 5):
            best_gain = 0
            best_choice = None
            best_type = None

            if len(current_r) < 8:
                candidate_regions = {rr for (rr, ii) in remaining if ii in current_i and rr not in current_r}
                for rr in candidate_regions:
                    gain = sum((rr, ii) in remaining for ii in current_i)
                    if gain > best_gain:
                        best_gain = gain
                        best_choice = rr
                        best_type = 'region'

            if len(current_i) < 5:
                candidate_insts = {ii for (rr, ii) in remaining if rr in current_r and ii not in current_i}
                for ii in candidate_insts:
                    gain = sum((rr, ii) in remaining for rr in current_r)
                    if gain > best_gain:
                        best_gain = gain
                        best_choice = ii
                        best_type = 'instance'

            if best_gain == 0:
                break

            if best_type == 'region':
                current_r.add(best_choice)
                newly_covered = {(best_choice, x) for x in current_i if (best_choice, x) in remaining}
            else:
                current_i.add(best_choice)
                newly_covered = {(x, best_choice) for x in current_r if (x, best_choice) in remaining}

            covered |= newly_covered
            remaining -= newly_covered

        queries.append((current_r, current_i))
    return queries


def grouping_to_create_optimized_request_list(dataframe):
    if dataframe.empty:
        return pd.DataFrame()

    def get_instance_type_batches(region_groups, region_instance_map):
        api_calls = []
        for group in region_groups:
            instance_union = set.union(*(region_instance_map.get(region, set()) for region in group))

            instance_list = list(instance_union)
            instance_batches = [instance_list[i:i + 5] for i in range(0, len(instance_list), 5)]

            for batch in instance_batches:
                api_calls.append({"Regions": group, "InstanceTypes": batch})
        return api_calls

    region_counts = dataframe['RegionCode'].value_counts().reset_index()
    region_counts.columns = ['RegionCode', 'Count']

    regions_less_than_100 = region_counts.loc[region_counts['Count'] < 100, 'RegionCode'].tolist()
    regions_100_or_more = region_counts.loc[region_counts['Count'] >= 100, 'RegionCode'].tolist()

    grouped_regions_less_than_100 = [regions_less_than_100[i:i + 8] for i in range(0, len(regions_less_than_100), 8)]
    grouped_regions_100_or_more = [regions_100_or_more[i:i + 8] for i in range(0, len(regions_100_or_more), 8)]

    region_instance_dict = dataframe.groupby("RegionCode")["InstanceType"].apply(lambda x: set(x.dropna())).to_dict()

    api_calls_less_than_100 = get_instance_type_batches(grouped_regions_less_than_100, region_instance_dict)
    api_calls_100_or_more = get_instance_type_batches(grouped_regions_100_or_more, region_instance_dict)

    res_final_api_calls_df = pd.DataFrame(api_calls_less_than_100 + api_calls_100_or_more)

    return res_final_api_calls_df
