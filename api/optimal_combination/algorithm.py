import pandas as pd

# Function to find the max_instance for each combination of InstanceType and AZ
def find_max_instance(df):
    result = []
    grouped = df.groupby(['InstanceType', 'AZ'])
    for (instance_type, az), group in grouped:
        filtered = group[group['SPS'] >= 3].sort_values(by='TargetCapacity', ascending=False)
        if not filtered.empty:
            max_target_capacity = filtered.iloc[0]['TargetCapacity']
        else:
            max_target_capacity = None
        result.append({'InstanceType': instance_type, 'AZ': az, 'Max_Instance': max_target_capacity})
    ret = pd.DataFrame(result)
    return ret



def find_efficient_combination_with_limits(core_target, sorted_instances):
    memo = {}  # 메모이제이션을 위한 딕셔너리입니다.
    def dp(remaining_cores, i=0):
        if remaining_cores <= 0:
            return 0, []  # 모든 코어 수를 만족한 경우
        elif i >= len(sorted_instances):
            return float('inf'), []  # 더 이상 처리할 인스턴스가 없는 경우
        elif (remaining_cores, i) in memo:
            return memo[(remaining_cores, i)]  # 이미 계산된 경우 메모이제이션 값을 반환

        # 옵션 1: 현재 인스턴스를 사용하지 않는 경우
        cost_without, combination_without = dp(remaining_cores, i + 1)

        # 옵션 2: 현재 인스턴스를 사용하는 경우, max_instance 고려
        instance = sorted_instances[i]
        max_instance_usage = min(instance['Max_Instance'], (remaining_cores + instance['FunctionPerInstance'] - 1) // instance['FunctionPerInstance'])  # max_instance 제한을 고려한 최대 사용 가능 수를 계산
        best_cost_with_inclusion = float('inf')
        best_combination_with_inclusion = []
        
        # 현재 인스턴스를 1개부터 max_instance_usage까지 사용하는 경우
        for usage in range(1, max_instance_usage + 1):
            remaining_cores_after_inclusion = remaining_cores - (instance['FunctionPerInstance'] * usage)
            cost_with_inclusion, combination_with_inclusion = dp(remaining_cores_after_inclusion, i + 1)  # 현재 인스턴스를 고려한 후 다음 인스턴스로 이동
            cost_with_inclusion += instance['SpotPrice'] * usage
            if cost_with_inclusion < best_cost_with_inclusion:
                best_cost_with_inclusion = cost_with_inclusion
                best_combination_with_inclusion = combination_with_inclusion + [(instance['InstanceType'], instance['AZ'])] * usage

        # 최소 비용을 갖는 옵션을 선택
        if cost_without <= best_cost_with_inclusion:
            memo[(remaining_cores, i)] = (cost_without, combination_without)
        else:
            memo[(remaining_cores, i)] = (best_cost_with_inclusion, best_combination_with_inclusion)
        return memo[(remaining_cores, i)]

    min_cost, combination = dp(core_target)
    
    count_combination = {}
    for instance in combination:
        if instance in count_combination:
            count_combination[instance] += 1
        else:
            count_combination[instance] = 1  
    
    return min_cost, count_combination



def select_instance(Memory_per_function, vCPU_per_function, required_functions, data):
    data['FunctionPerInstance'] = data.apply(lambda row: min(row['vCPU'] // vCPU_per_function, row['Memory'] // Memory_per_function), axis=1)
    data['PricePerFunction'] = data['SpotPrice'] / data['FunctionPerInstance']
    sorted_data = data.sort_values(by='PricePerFunction').reset_index(drop=True)

    total_functions = 0
    total_cost = 0
    instance_combination = {}

    for index, row in sorted_data.iterrows():
        if total_functions >= required_functions:
            break

        instances_needed = (required_functions - total_functions) // row['FunctionPerInstance']
        instance_to_use = min(instances_needed, row['Max_Instance'])


        if instance_to_use > 0:
            instance_combination[(row['InstanceType'], row['AZ'])] = instance_to_use
            total_cost += instance_to_use * row['SpotPrice']
            total_functions += instance_to_use * row['FunctionPerInstance']




    remain_function = required_functions - total_functions
    if remain_function:
        for key, value in instance_combination.items():
            sorted_data.loc[(sorted_data['InstanceType'] == key[0]) & (sorted_data['AZ'] == key[1]), 'Max_Instance'] -= value
        remain_core_cost, remain_core_combination = find_efficient_combination_with_limits(remain_function, sorted_data.to_dict('records'))

        for key, value in remain_core_combination.items():
                if key in instance_combination:
                    instance_combination[key] += value
                else:
                    instance_combination[key] = value        
        return total_cost+remain_core_cost, instance_combination
    else:
        return total_cost, instance_combination