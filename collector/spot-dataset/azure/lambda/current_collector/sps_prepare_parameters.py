import json
import numpy as np
import pandas as pd
import os
from dotenv import load_dotenv
from sklearn.cluster import KMeans
from collections import defaultdict

load_dotenv('./files_sps/.env')

INVALID_REGIONS_PATH_JSON = os.getenv('INVALID_REGIONS_PATH_JSON')
INVALID_INSTANCE_TYPES_PATH_JSON = os.getenv('INVALID_INSTANCE_TYPES_PATH_JSON')


def filter_invalid_parameter(regions_and_instance_types_df):
    """
    이 메서드는 무효한 regions_and_instance_types 데이터(JSON 파일)를 로드하고,
    요청한 dataframe중의 무효값을 제거합니다.
    """
    try:
        with open(INVALID_REGIONS_PATH_JSON, 'r') as file:
            invalid_region_list = json.load(file).get("invalid_regions", [])

        with open(INVALID_INSTANCE_TYPES_PATH_JSON, 'r') as file:
            invalid_instance_type_list = json.load(file).get("invalid_instance_types", [])

    except FileNotFoundError:
        print("Invalid_regions file or invalid_instance_types file not found.")
        invalid_region_list = []
        invalid_instance_type_list = []
    except json.JSONDecodeError as e:
        print(f"filter_invalid_parameter func. Failed to parse JSON: {e}")
        invalid_region_list = []
        invalid_instance_type_list = []
    except Exception as e:
        print(f"filter_invalid_parameter func. An unexpected error occurred: {e}")
        invalid_region_list = []
        invalid_instance_type_list = []

    regions_and_instance_types_del1_invalid_df = regions_and_instance_types_df[
        ~regions_and_instance_types_df['RegionCode'].isin(invalid_region_list) &
        ~regions_and_instance_types_df['InstanceType'].isin(invalid_instance_type_list)
        ]

    print(f"Length of regions_and_instance_types_df: {len(regions_and_instance_types_df)}")
    print(f"Length of regions_and_instance_types_del_invalid_df: {len(regions_and_instance_types_del1_invalid_df)}")

    return regions_and_instance_types_del1_invalid_df


def greedy_clustering_to_create_optimized_request_list(regions_and_instance_types_df):
    '''
    요청값인 dataframe을 greedy&clustering 방법으로 파라미터 pool을 최적화 만듭니다.
    '''
    support_set, regions, instancetypes = load_support_data_from_df(regions_and_instance_types_df)

    queries = clustering_cover_remaining_greedily(support_set, regions, instancetypes)

    df_greedy_clustered = pd.DataFrame(
        [{'Regions': list(regions), 'InstanceTypes': list(instancetypes)} for regions, instancetypes in
         queries]
    )
    return df_greedy_clustered


def load_support_data_from_df(df):
    """
    df에서 Region별 지원 InstanceType 정보를 읽어들여
    지원 관계를 (region, instancetype) 튜플의 집합으로 반환합니다.
    """
    support_set = set()
    regions = set()
    instancetypes = set()

    for _, row in df.iterrows():
        region = row['RegionCode']
        itype = row['InstanceType']
        support_set.add((region, itype))
        regions.add(region)
        instancetypes.add(itype)

    return support_set, sorted(regions), sorted(instancetypes)


def clustering_cover_remaining_greedily(support_set, regions, instancetypes, n_region_clusters=2, n_inst_clusters=2,
                                        max_regions=8, max_instancetypes=5):
    """
    클러스터링 기반 커버 알고리즘:
    1. Region과 InstanceType을 클러스터링.
    2. 각 region 클러스터와 instance 클러스터 조합에 대해 빈도 기반으로 인스턴스와 리전을 선택하여 커버.
    3. 남은 에지(remaining)는 이전엔 하나씩 쿼리를 만들었지만, 이제 간단한 그리디 로직을 적용하여 가능한 한 묶어서 커버.
    """
    region_to_idx = {r: idx for idx, r in enumerate(regions)}
    inst_to_idx = {i: idx for idx, i in enumerate(instancetypes)}

    # Region별 지원 패턴 행렬
    R_matrix = np.zeros((len(regions), len(instancetypes)), dtype=int)
    for (r, i) in support_set:
        R_matrix[region_to_idx[r], inst_to_idx[i]] = 1

    # InstanceType별 지원 패턴 (전치)
    I_matrix = R_matrix.T

    # 클러스터링
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

    # 각 클러스터 조합마다 커버 시도
    for rc, rgroup in region_cluster_map.items():
        for ic, igroup in inst_cluster_map.items():
            subset = {(r, i) for (r, i) in remaining if r in rgroup and i in igroup}
            if not subset:
                continue

            # 빈도 계산
            region_freq = defaultdict(int)
            inst_freq = defaultdict(int)
            for (r, i) in subset:
                region_freq[r] += 1
                inst_freq[i] += 1

            # 빈도 높은 순으로 정렬
            sel_regions = sorted(rgroup, key=lambda x: region_freq[x], reverse=True)[:max_regions]
            sel_insts = sorted(igroup, key=lambda x: inst_freq[x], reverse=True)[:max_instancetypes]

            # 실제 커버되는 에지
            covered = {(r, i) for (r, i) in subset if r in sel_regions and i in sel_insts}
            if covered:
                queries.append((set(sel_regions), set(sel_insts)))
                remaining -= covered

    # 남은 에지를 처리
    # 여기서 이전에는 남은 에지 각각 쿼리를 생성했으나,
    # 이제 간단한 그리디 로직으로 최대한 묶어서 커버
    if remaining:
        queries += _cover_remaining_greedily(remaining, max_regions, max_instancetypes)

    return queries


def _cover_remaining_greedily(remaining, max_regions=8, max_instancetypes=5):
    """
    남은 에지를 간단한 그리디 방식으로 커버하는 보조 함수.
    최대 8개 region, 5개 instanceType을 골라 한번에 많이 커버하는 쿼리를 반복적으로 생성.
    """
    queries = []
    while remaining:
        # 하나 꺼내서 시작
        r, i = remaining.pop()
        current_r = {r}
        current_i = {i}
        covered = {(r, i)}

        remaining -= covered

        # 확장
        # 단순히 region과 instance 중 추가 시 이득이 가장 큰 후보를 추가
        while (len(current_r) < max_regions or len(current_i) < max_instancetypes):
            best_gain = 0
            best_choice = None
            best_type = None  # 'region' or 'instance'

            # 추가 가능한 region 후보
            if len(current_r) < max_regions:
                # 현재 instances와 결합 가능한 region 후보 탐색
                candidate_regions = {rr for (rr, ii) in remaining if ii in current_i and rr not in current_r}
                for rr in candidate_regions:
                    # rr 추가 시 이득 계산
                    gain = sum((rr, ii) in remaining for ii in current_i)
                    if gain > best_gain:
                        best_gain = gain
                        best_choice = rr
                        best_type = 'region'

            # 추가 가능한 instance 후보
            if len(current_i) < max_instancetypes:
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


