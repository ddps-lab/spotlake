from gcp_metadata import region_list, machine_type_list, n1, e2, n2, n2d, t2d, c2, c2d, m1, a2

# This code is referenced from "https://github.com/doitintl/gcpinstances.info/blob/master/scraper.py"


def extract_price(machine_type, price_data, price_type):
    # get price from pricelist and put into output data (for N1 : f1-micro, g1-small)
    # input : machine type, filtered pricelist, price type (ondemand or preemptible)
    # output : None, but save price into final output data

    for region, price in price_data.items():
        if region in region_list:
            if price == 0:
                output[machine_type][region][price_type] = -1
            else:
                output[machine_type][region][price_type] = price


def calculate_price(cpu_data, ram_data, machine_series, price_type):
    # get regional price of each unit and calculate workload price
    # input : regional cpu & ram price of workload, machine series, price type (ondemand or preemptible)
    # output : None, but save price into final output data

    for machine_type, feature in machine_series.items():
        cpu_quantity = feature['cpu']
        ram_quantity = feature['ram']
        for cpu_region, cpu_price in cpu_data.items():
            for ram_region, ram_price in ram_data.items():
                if cpu_region == ram_region and cpu_region in region_list:
                    output[machine_type][cpu_region][price_type] = cpu_quantity * \
                        cpu_price + ram_quantity*ram_price


def get_price(pricelist):
    # put prices of workloads into output data
    # input : pricelist of compute engine unit
    # output : dictionary data of calculated price

    global output
    output = {}
    for machine_type in machine_type_list:
        output[machine_type] = {}
        for region in region_list:
            output[machine_type][region] = {}
            output[machine_type][region]['ondemand'] = -1
            output[machine_type][region]['preemptible'] = -1

    # N1 : f1-micro
    # ondemand
    f1_data_odm = pricelist['CP-COMPUTEENGINE-VMIMAGE-F1-MICRO']
    extract_price('f1-micro', f1_data_odm, 'ondemand')

    # preemptible
    f1_data_prmt = pricelist['CP-COMPUTEENGINE-VMIMAGE-F1-MICRO-PREEMPTIBLE']
    extract_price('f1-micro', f1_data_prmt, 'preemptible')

    # N1 : g1-small
    # ondemand
    g1_data_odm = pricelist['CP-COMPUTEENGINE-VMIMAGE-G1-SMALL']
    extract_price('g1-small', g1_data_odm, 'ondemand')

    # preemptible
    g1_data_prmt = pricelist['CP-COMPUTEENGINE-VMIMAGE-G1-SMALL-PREEMPTIBLE']
    extract_price('g1-small', g1_data_prmt, 'preemptible')

    # N1
    # ondemand
    cpu_data = pricelist['CP-COMPUTEENGINE-N1-PREDEFINED-VM-CORE']
    ram_data = pricelist['CP-COMPUTEENGINE-N1-PREDEFINED-VM-RAM']
    calculate_price(cpu_data, ram_data, n1, 'ondemand')
    # preemptible
    cpu_data = pricelist['CP-COMPUTEENGINE-N1-PREDEFINED-VM-CORE-PREEMPTIBLE']
    ram_data = pricelist['CP-COMPUTEENGINE-N1-PREDEFINED-VM-RAM-PREEMPTIBLE']
    calculate_price(cpu_data, ram_data, n1, 'preemptible')

    # E2
    # ondemand
    cpu_data = pricelist['CP-COMPUTEENGINE-E2-PREDEFINED-VM-CORE']
    ram_data = pricelist['CP-COMPUTEENGINE-E2-PREDEFINED-VM-RAM']
    calculate_price(cpu_data, ram_data, e2, 'ondemand')
    # preemptible
    cpu_data = pricelist['CP-COMPUTEENGINE-E2-PREDEFINED-VM-CORE-PREEMPTIBLE']
    ram_data = pricelist['CP-COMPUTEENGINE-E2-PREDEFINED-VM-RAM-PREEMPTIBLE']
    calculate_price(cpu_data, ram_data, e2, 'preemptible')

    # N2
    # ondemand
    cpu_data = pricelist['CP-COMPUTEENGINE-N2-PREDEFINED-VM-CORE']
    ram_data = pricelist['CP-COMPUTEENGINE-N2-PREDEFINED-VM-RAM']
    calculate_price(cpu_data, ram_data, n2, 'ondemand')
    # preemptible
    cpu_data = pricelist['CP-COMPUTEENGINE-N2-PREDEFINED-VM-CORE-PREEMPTIBLE']
    ram_data = pricelist['CP-COMPUTEENGINE-N2-PREDEFINED-VM-RAM-PREEMPTIBLE']
    calculate_price(cpu_data, ram_data, n2, 'preemptible')

    # N2D
    # ondemand
    cpu_data = pricelist['CP-COMPUTEENGINE-N2D-PREDEFINED-VM-CORE']
    ram_data = pricelist['CP-COMPUTEENGINE-N2D-PREDEFINED-VM-RAM']
    calculate_price(cpu_data, ram_data, n2d, 'ondemand')

    # preemptible
    cpu_data = pricelist['CP-COMPUTEENGINE-N2D-PREDEFINED-VM-CORE-PREEMPTIBLE']
    ram_data = pricelist['CP-COMPUTEENGINE-N2D-PREDEFINED-VM-RAM-PREEMPTIBLE']
    calculate_price(cpu_data, ram_data, n2d, 'preemptible')

    # T2D
    # ondemand
    cpu_data = pricelist['CP-COMPUTEENGINE-T2D-PREDEFINED-VM-CORE']
    ram_data = pricelist['CP-COMPUTEENGINE-T2D-PREDEFINED-VM-RAM']
    calculate_price(cpu_data, ram_data, t2d, 'ondemand')

    # preemptible
    cpu_data = pricelist['CP-COMPUTEENGINE-T2D-PREDEFINED-VM-CORE-PREEMPTIBLE']
    ram_data = pricelist['CP-COMPUTEENGINE-T2D-PREDEFINED-VM-RAM-PREEMPTIBLE']
    calculate_price(cpu_data, ram_data, t2d, 'preemptible')

    # C2
    # ondemand
    cpu_data = pricelist['CP-COMPUTEENGINE-C2-PREDEFINED-VM-CORE']
    ram_data = pricelist['CP-COMPUTEENGINE-C2-PREDEFINED-VM-RAM']
    calculate_price(cpu_data, ram_data, c2, 'ondemand')

    # preemptible
    cpu_data = pricelist['CP-COMPUTEENGINE-C2-PREDEFINED-VM-CORE-PREEMPTIBLE']
    ram_data = pricelist['CP-COMPUTEENGINE-C2-PREDEFINED-VM-RAM-PREEMPTIBLE']
    calculate_price(cpu_data, ram_data, c2, 'preemptible')

    # C2D
    # ondemand
    cpu_data = pricelist['CP-COMPUTEENGINE-C2D-PREDEFINED-VM-CORE']
    ram_data = pricelist['CP-COMPUTEENGINE-C2D-PREDEFINED-VM-RAM']
    calculate_price(cpu_data, ram_data, c2d, 'ondemand')

    # preemptible
    cpu_data = pricelist['CP-COMPUTEENGINE-C2D-PREDEFINED-VM-CORE-PREEMPTIBLE']
    ram_data = pricelist['CP-COMPUTEENGINE-C2D-PREDEFINED-VM-RAM-PREEMPTIBLE']
    calculate_price(cpu_data, ram_data, c2d, 'preemptible')

    # M1
    # ondemand
    cpu_data = pricelist['CP-COMPUTEENGINE-M1-PREDEFINED-VM-CORE']
    ram_data = pricelist['CP-COMPUTEENGINE-M1-PREDEFINED-VM-RAM']
    calculate_price(cpu_data, ram_data, m1, 'ondemand')

    # preemptible
    cpu_data = pricelist['CP-COMPUTEENGINE-M1-PREDEFINED-VM-CORE-PREEMPTIBLE']
    ram_data = pricelist['CP-COMPUTEENGINE-M1-PREDEFINED-VM-RAM-PREEMPTIBLE']
    calculate_price(cpu_data, ram_data, m1, 'preemptible')

    # A2
    # ondemand
    cpu_data = pricelist['CP-COMPUTEENGINE-A2-PREDEFINED-VM-CORE']
    ram_data = pricelist['CP-COMPUTEENGINE-A2-PREDEFINED-VM-RAM']
    gpu_data = pricelist['GPU_NVIDIA_TESLA_A100']
    for machine_type, feature in a2.items():
        cpu_quantity = feature['cpu']
        ram_quantity = feature['ram']
        gpu_quantity = feature['gpu']
        for cpu_region, cpu_price in cpu_data.items():
            for ram_region, ram_price in ram_data.items():
                for gpu_region, gpu_price in gpu_data.items():
                    if cpu_region == ram_region and cpu_region == gpu_region and cpu_region in region_list:
                        output[machine_type][cpu_region]['ondemand'] = cpu_quantity * \
                            cpu_price + ram_quantity*ram_price + gpu_quantity*gpu_price
    # preemptible
    cpu_data = pricelist['CP-COMPUTEENGINE-A2-PREDEFINED-VM-CORE-PREEMPTIBLE']
    ram_data = pricelist['CP-COMPUTEENGINE-A2-PREDEFINED-VM-RAM-PREEMPTIBLE']
    gpu_data = pricelist['GPU_NVIDIA_TESLA_A100-PREEMPTIBLE']
    for machine_type, feature in a2.items():
        cpu_quantity = feature['cpu']
        ram_quantity = feature['ram']
        gpu_quantity = feature['gpu']
        for cpu_region, cpu_price in cpu_data.items():
            for ram_region, ram_price in ram_data.items():
                for gpu_region, gpu_price in gpu_data.items():
                    if cpu_region == ram_region and cpu_region == gpu_region and cpu_region in region_list:
                        if output[machine_type][cpu_region]['ondemand'] != -1:
                            output[machine_type][cpu_region]['preemptible'] = cpu_quantity * \
                                cpu_price + ram_quantity*ram_price + gpu_quantity*gpu_price
    return output


def preprocessing_price(df):
    # make list to struct final dataframe
    # input : dataframe 
    # output : list having Instance type, Region, Ondemand price, Preemptible price
    
    new_list = []
    for machine_type, info in df.items():
        for region, price in info.items():
            ondemand = price['ondemand']
            preemptible = price['preemptible']

            if ondemand != -1 and preemptible != -1:
                ondemand = round(ondemand, 4)
                preemptible = round(preemptible, 4)

            new_list.append(
                 [machine_type, region, ondemand, preemptible])
    return new_list
