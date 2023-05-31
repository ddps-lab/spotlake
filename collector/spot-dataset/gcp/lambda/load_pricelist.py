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
df_instance_metadata['guest_accerlator_count'] = df_instance_metadata['guest_accerlator_count'].fillna(0)
df_instance_metadata['ssd'] = df_instance_metadata['ssd'].fillna(0)

def extract_price(machine_type, price_data, price_type):
    # get price from pricelist and put into output data (for N1 : f1-micro, g1-small)
    # input : machine type, filtered pricelist, price type (ondemand or preemptible)
    # output : None, but save price into final output data

    for region, price in price_data.items():
        if region in output[machine_type].keys():
            output[machine_type][region][price_type] = price


def calculate_price(cpu_data, ram_data, gpu_data, instance_type, price_type):
    # get regional price of each unit and calculate workload price
    # input : regional cpu, ram, gpu price of workload, instance_type, price type (ondemand or preemptible)
    # output : None, but save price into final output data

    ssd_price = 0.04 if price_type == 'ondemand' else 0.02
    instance_spec = df_instance_metadata[df_instance_metadata['instance_type'] == instance_type]
    for k, v in instance_spec.iterrows():
        instance_type = v['instance_type']
        cpu_quantity = v['guest_cpus']
        ram_quantity = v['memoryGB']
        gpu_quantity = v['guest_accerlator_count']
        ssd_quantity = v['ssd']

        for region, av_instance in available_region_lists.items():
            if instance_type not in av_instance:
                continue
            for cpu_region, cpu_price in cpu_data.items():
                for ram_region, ram_price in ram_data.items():
                    if cpu_region == ram_region and cpu_region == region:
                        price = cpu_quantity * cpu_price + ram_quantity * ram_price
                    
                        if gpu_data != None:
                            for gpu_region, gpu_price in gpu_data.items():
                                if gpu_region == region :
                                    output[instance_type][cpu_region][price_type] = price + gpu_quantity * gpu_price + ssd_quantity * ssd_price
                        else :
                            output[instance_type][cpu_region][price_type] = price


def get_price(pricelist, df_instance_metadata, available_region_lists):
    # put prices of workloads into output data
    # input : pricelist of compute engine unit, instace metadata, available region lists
    # output : dictionary data of calculated price

    global output
    output = {}
    for instance_type in df_instance_metadata['instance_type']:
        output[instance_type] = {}
        for region in available_region_lists.keys():
            if instance_type in available_region_lists[region]:
                output[instance_type][region] = {}
                output[instance_type][region]['ondemand'] = -1
                output[instance_type][region]['preemptible'] = -1


    for instance_type in df_instance_metadata['instance_type']:
        series = instance_type.split('-')[0]

        if series in ['f1', 'g1'] :  # shared cpu
            ondemand_data = pricelist[f'CP-COMPUTEENGINE-VMIMAGE-{instance_type.upper()}']
            extract_price(instance_type, ondemand_data, 'ondemand')

            # preemptible
            preemptible_data = pricelist[f'CP-COMPUTEENGINE-VMIMAGE-{instance_type.upper()}-PREEMPTIBLE']
            extract_price(instance_type, preemptible_data, 'preemptible')  
        
        else :
            # get gpu data
            gpu_data = None
            gpu_data_preemptible = None
            accelerator = df_instance_metadata[df_instance_metadata['instance_type'] == instance_type]['guest_accerlator_type'].values[0]

            if pd.isna(accelerator) == False:
                accelerator = accelerator.upper().replace('-', '_')
                if 'TESLA' not in accelerator :
                    accelerator = accelerator.replace('NVIDIA', 'NVIDIA_TESLA', 1)
                    
                if '80GB' in accelerator :
                    accelerator =accelerator.replace ('_80GB', '-80GB', 1)

                gpu_data = pricelist[f'GPU_{accelerator}']
                gpu_data_preemptible = pricelist[f'GPU_{accelerator}-PREEMPTIBLE']

            try:
                # ondemand
                cpu_data = pricelist[f'CP-COMPUTEENGINE-{series.upper()}-PREDEFINED-VM-CORE']
                ram_data = pricelist[f'CP-COMPUTEENGINE-{series.upper()}-PREDEFINED-VM-RAM']
                calculate_price(cpu_data, ram_data, gpu_data, instance_type, 'ondemand')
                    
                # preemptible
                cpu_data = pricelist[f'CP-COMPUTEENGINE-{series.upper()}-PREDEFINED-VM-CORE-PREEMPTIBLE']
                ram_data = pricelist[f'CP-COMPUTEENGINE-{series.upper()}-PREDEFINED-VM-RAM-PREEMPTIBLE']
                calculate_price(cpu_data, ram_data, gpu_data_preemptible, instance_type, 'preemptible')

            except KeyError:
                # C3 series doesn't exsist in pricelist.json
                # M3 series doesn't support spot (preemptible)
                pass

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
