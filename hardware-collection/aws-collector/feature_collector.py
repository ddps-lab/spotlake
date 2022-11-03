import pandas as pd
import json
import os
from get_info import get_processor_info, get_vcpus_info, get_disk_info, get_memory_info, get_network_info, get_gpu_info


with open('aws_instance_types.json', 'r') as f:
    aws_json = json.load(f)
df_raw = pd.DataFrame(aws_json)

target = [['on-demand', 'spot']]
df_raw = df_raw[df_raw['SupportedUsageClasses'].apply(lambda x: x in target)]

col_list = ['InstanceType', 'SupportedArchitectures', 'SustainedClockSpeed(Ghz)', 
                'DefaultVCpus', 'DefaultCores', 'DefaultThreadsPerCore',
                'DiskSize(GB)', 'DiskCount', 'DiskType',
                'MemorySize(MiB)', 'NetworkPerformance(Gbit)',
                'GpuName', 'GPUManufacturer', 'GPUCount', 'GPUMemorySize(MiB)']

feature_list = []

for idx, row in df_raw.iterrows():
    each_feature = []
    each_feature.append(row['InstanceType'])

    each_feature = get_processor_info(each_feature, row['ProcessorInfo'])
    each_feature = get_vcpus_info(each_feature, row['VCpuInfo'])

    if type(row['InstanceStorageInfo']) == dict:
        each_feature = get_disk_info(each_feature, row['InstanceStorageInfo']['Disks'][0])
    else:
        for i in range(0, 3):
            each_feature.append(None)

    each_feature = get_memory_info(each_feature, row['MemoryInfo'])

    each_feature = get_network_info(each_feature, row['NetworkInfo'])
    
    if type(row['GpuInfo']) == dict:
        each_feature = get_gpu_info(each_feature, row['GpuInfo']['Gpus'][0])
    else:
        for i in range(0, 4):
            each_feature.append(None)

    feature_list.append(each_feature)

df_aws_instances = pd.DataFrame(feature_list, columns=col_list)
df_aws_instances.to_pickle('./aws_hardware_feature.pkl')

file_path = './aws_instance_types.json'
if os.path.exists(file_path):
    os.remove(file_path)
