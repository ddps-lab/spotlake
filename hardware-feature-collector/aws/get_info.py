def get_processor_info(each_feature, processor_info):
    try:
        each_feature.append(processor_info['SupportedArchitectures'])
    except:
        each_feature.append(None)
    
    try:
        each_feature.append(processor_info['SustainedClockSpeedInGhz'])
    except:
        each_feature.append(None)
    
    return each_feature

def get_vcpus_info(each_feature, vcpu_info):
    try:
        each_feature.append(vcpu_info['DefaultVCpus'])
    except:
        each_feature.append(None)

    try:
        each_feature.append(vcpu_info['DefaultCores'])
    except:
        each_feature.append(None)

    try:
        each_feature.append(vcpu_info['DefaultThreadsPerCore'])
    except:
        each_feature.append(None)

    return each_feature

def get_disk_info(each_feature, disk_info):
    each_feature.append(disk_info['SizeInGB'])
    each_feature.append(disk_info['Count'])
    each_feature.append(disk_info['Type'])
    
    return each_feature

def get_memory_info(each_feature, memory_info):
    try:
        each_feature.append(memory_info['SizeInMiB'])
    except:
        each_feature.append(None)

    return each_feature


def get_network_info(each_feature, network_info):
    try:
        each_feature.append(network_info['NetworkPerformance'])
    except:
        each_feature.append(None)

    return each_feature

def get_gpu_info(each_feature, gpu_info):
    each_feature.append(gpu_info['Name'])
    each_feature.append(gpu_info['Manufacturer'])
    each_feature.append(gpu_info['Count'])
    each_feature.append(gpu_info['MemoryInfo']['SizeInMiB'])

    return each_feature