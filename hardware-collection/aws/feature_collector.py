import pandas as pd
import json
import sys
from get_info import get_processor_info, get_vcpus_info, get_disk_info, get_memory_info, get_network_info, get_gpu_info

FILE_NAME = sys.argv[1]
FILE_PATH = f'./{FILE_NAME}'

with open(FILE_PATH, 'r') as f:
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
    each_feature = get_disk_info(each_feature, row['InstanceStorageInfo'])
    each_feature = get_memory_info(each_feature, row['MemoryInfo'])
    each_feature = get_network_info(each_feature, row['NetworkInfo'])
    each_feature = get_gpu_info(each_feature, row['GpuInfo'])
    
    feature_list.append(each_feature)

df_aws_instances = pd.DataFrame(feature_list, columns=col_list)
df_aws_instances.to_pickle('./aws_hardware_feature.pkl')
