import json
import pandas as pd

# This code is referenced from "https://github.com/doitintl/gcpinstances.info/blob/master/scraper.py"

global available_region_lists
global df_instance_metadata

##### Need to changed -> load from S3 #####
with open('available_region.json', 'r') as f:
    available_region_lists = json.load(f)

df_instance_metadata = pd.read_json('instance_metadata.json')
##### ------------------------------- #####

def extract_price(machine_type, price_data, price_type):
    # get price from pricelist and put into output data (for N1 : f1-micro, g1-small)
    # input : machine type, filtered pricelist, price type (ondemand or preemptible)
    # output : None, but save price into final output data

    for region, price in price_data.items():
        if region in output[machine_type].keys():
            output[machine_type][region][price_type] = price


def calculate_price(cpu_data, ram_data, machine_series, price_type):
    # get regional price of each unit and calculate workload price
    # input : regional cpu & ram price of workload, machine series, price type (ondemand or preemptible)
    # output : None, but save price into final output data
    instance_spec = df_instance_metadata[df_instance_metadata['instance_type'].str.contains(machine_series)]
    for k, v in instance_spec.iterrows():
        cpu_quantity = v['guest_cpus']
        ram_quantity = v['memoryGB']
        instance_type = v['instance_type']
        for region, av_instance in available_region_lists.items():
            if instance_type not in av_instance:
                continue
            for cpu_region, cpu_price in cpu_data.items():
                for ram_region, ram_price in ram_data.items():
                    if cpu_region == ram_region and cpu_region == region :
                        output[instance_type][cpu_region][price_type] = cpu_quantity * \
                                                                    cpu_price + ram_quantity * ram_price


def get_price(pricelist, df_instance_metadata, available_region_lists):
    # put prices of workloads into output data
    # input : pricelist of compute engine unit, instace metadata, available region lists
    # output : dictionary data of calculated price

    # init output
    global output
    output = {}
    for instance_type in df_instance_metadata['instance_type']:
        output[instance_type] = {}
        for region in available_region_lists.keys():
            if instance_type in available_region_lists[region]:
                output[instance_type][region] = {}
                output[instance_type][region]['ondemand'] = -1
                output[instance_type][region]['preemptible'] = -1

    # make series : [types... ] dictionary for traversal
    instance_series = dict()
    for instance_type in df_instance_metadata['instance_type']:
        series = instance_type.split('-')[0]
        if series not in instance_series.keys():
            instance_series[series] = list()
        instance_series[series].append(instance_type)

    for series, types in instance_series.items():
        if series in ['f1', 'g1'] :  # shared cpu
            instance_type = types[0]

            # ondemand
            ondemand_data = pricelist[f'CP-COMPUTEENGINE-VMIMAGE-{instance_type.upper()}']
            extract_price(instance_type, ondemand_data, 'ondemand')

            # preemptible
            preemptible_data = pricelist[f'CP-COMPUTEENGINE-VMIMAGE-{instance_type.upper()}-PREEMPTIBLE']
            extract_price(instance_type, preemptible_data, 'preemptible')  
        
        else :
            # ondemand
            cpu_data = pricelist[f'CP-COMPUTEENGINE-{series.upper()}-PREDEFINED-VM-CORE']
            ram_data = pricelist[f'CP-COMPUTEENGINE-{series.upper()}-PREDEFINED-VM-RAM']
            calculate_price(cpu_data, ram_data, series, 'ondemand')
            
            # preemptible
            cpu_data = pricelist[f'CP-COMPUTEENGINE-{series.upper()}-PREDEFINED-VM-CORE-PREEMPTIBLE']
            ram_data = pricelist[f'CP-COMPUTEENGINE-{series.upper()}-PREDEFINED-VM-RAM-PREEMPTIBLE']
            calculate_price(cpu_data, ram_data, series, 'preemptible')

    ###### Need to change #####
    # A2
    # ondemand
    cpu_data = pricelist['CP-COMPUTEENGINE-A2-PREDEFINED-VM-CORE']
    ram_data = pricelist['CP-COMPUTEENGINE-A2-PREDEFINED-VM-RAM']
    gpu_data = pricelist['GPU_NVIDIA_TESLA_A100']
    ssd_ondemand_price = 0.04

    instance_spec = df_instance_metadata[df_instance_metadata['instance_type'].str.contains('a2')]
    for k, v in instance_spec.iterrows():
        instance_type = v['instance_type']
        cpu_quantity = v['guest_cpus']
        ram_quantity = v['memoryGB']
        gpu_quantity = v['guest_accerlator_count']

        # handle a2-ultragpu
        ssd_quantity = v['ssd']
        if pd.isna(ssd_quantity):
            ssd_quantity = 0
        else :
            gpu_data = pricelist['GPU_NVIDIA_TESLA_A100-80GB']

        for region, av_instance in available_region_lists.items():
            if instance_type not in av_instance:
                continue
            for cpu_region, cpu_price in cpu_data.items():
                for ram_region, ram_price in ram_data.items():
                    for gpu_region, gpu_price in gpu_data.items():
                        if cpu_region == ram_region and cpu_region == gpu_region and cpu_region == region:
                                output[instance_type][region]['ondemand'] = cpu_quantity * cpu_price + \
                                                                        ram_quantity * ram_price + gpu_quantity * gpu_price + ssd_quantity * ssd_ondemand_price

    # preemptible
    cpu_data = pricelist['CP-COMPUTEENGINE-A2-PREDEFINED-VM-CORE-PREEMPTIBLE']
    ram_data = pricelist['CP-COMPUTEENGINE-A2-PREDEFINED-VM-RAM-PREEMPTIBLE']
    gpu_data = pricelist['GPU_NVIDIA_TESLA_A100-PREEMPTIBLE']
    ssd_preemptible_price = 0.02

    instance_spec = df_instance_metadata[df_instance_metadata['instance_type'].str.contains('a2')]
    for k, v in instance_spec.iterrows():
            instance_type = v['instance_type']
            cpu_quantity = v['guest_cpus']
            ram_quantity = v['memoryGB']
            gpu_quantity = v['guest_accerlator_count']

            # handle a2-ultragpu
            ssd_quantity = v['ssd']
            if pd.isna(ssd_quantity):
                ssd_quantity = 0
            else :
                gpu_data = pricelist['GPU_NVIDIA_TESLA_A100-80GB-PREEMPTIBLE']

            for region, av_instance in available_region_lists.items():
                if instance_type not in av_instance:
                    continue
                for cpu_region, cpu_price in cpu_data.items():
                    for ram_region, ram_price in ram_data.items():
                        for gpu_region, gpu_price in gpu_data.items():
                            if cpu_region == ram_region and cpu_region == gpu_region and cpu_region == region:
                                output[instance_type][region]['preemptible'] = cpu_quantity * cpu_price + \
                                                                                ram_quantity * ram_price + gpu_quantity * gpu_price + ssd_quantity * ssd_preemptible_price


    # G2
    # ondemand
    cpu_data = pricelist['CP-COMPUTEENGINE-G2-PREDEFINED-VM-CORE']
    ram_data = pricelist['CP-COMPUTEENGINE-G2-PREDEFINED-VM-RAM']
    gpu_data = pricelist['GPU_NVIDIA_TESLA_L4']

    instance_spec = df_instance_metadata[df_instance_metadata['instance_type'].str.contains('g2')]
    for k, v in instance_spec.iterrows():
        instance_type = v['instance_type']
        cpu_quantity = v['guest_cpus']
        ram_quantity = v['memoryGB']
        gpu_quantity = v['guest_accerlator_count']

        for region, av_instance in available_region_lists.items():
            if instance_type not in av_instance:
                continue
            for cpu_region, cpu_price in cpu_data.items():
                for ram_region, ram_price in ram_data.items():
                    for gpu_region, gpu_price in gpu_data.items():
                        if cpu_region == ram_region and cpu_region == gpu_region and cpu_region == region:
                                output[instance_type][region]['ondemand'] = cpu_quantity * cpu_price + \
                                                                        ram_quantity * ram_price + gpu_quantity * gpu_price

    # preemptible
    cpu_data = pricelist['CP-COMPUTEENGINE-G2-PREDEFINED-VM-CORE-PREEMPTIBLE']
    ram_data = pricelist['CP-COMPUTEENGINE-G2-PREDEFINED-VM-RAM-PREEMPTIBLE']
    gpu_data = pricelist['GPU_NVIDIA_TESLA_L4-PREEMPTIBLE']

    instance_spec = df_instance_metadata[df_instance_metadata['instance_type'].str.contains('g2')]
    for k, v in instance_spec.iterrows():
            instance_type = v['instance_type']
            cpu_quantity = v['guest_cpus']
            ram_quantity = v['memoryGB']
            gpu_quantity = v['guest_accerlator_count']

            for region, av_instance in available_region_lists.items():
                if instance_type not in av_instance:
                    continue
                for cpu_region, cpu_price in cpu_data.items():
                    for ram_region, ram_price in ram_data.items():
                        for gpu_region, gpu_price in gpu_data.items():
                            if cpu_region == ram_region and cpu_region == gpu_region and cpu_region == region:
                                output[instance_type][region]['preemptible'] = cpu_quantity * cpu_price + \
                                                                                ram_quantity * ram_price + gpu_quantity * gpu_price
    ##### ------------------------------- #####

    return output


def preprocessing_price(df):
    # make list to struct final dataframe
    # input : dataframe 
    # output : list having Instance type, Region, Ondemand price, Preemptible price

    new_list = []
    for machine_type, info in df.items():
        for region, price in info.items():
            try:
                ondemand = price['ondemand']
                preemptible = price['preemptible']
            except TypeError :
                continue

            if ondemand != -1 and preemptible != -1:
                ondemand = round(ondemand, 4)
                preemptible = round(preemptible, 4)

            new_list.append(
                [machine_type, region, ondemand, preemptible])
    
    return new_list
