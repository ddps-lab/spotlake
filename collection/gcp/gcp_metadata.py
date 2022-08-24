# to form dataframe and find price of workloads
# https://cloud.google.com/compute/docs/general-purpose-machines
machine_type_list = [
    'n1-standard-1', 'n1-standard-2', 'n1-standard-4', 'n1-standard-8', 'n1-standard-16', 'n1-standard-32', 'n1-standard-64', 'n1-standard-96',
    'n1-highmem-2', 'n1-highmem-4', 'n1-highmem-8', 'n1-highmem-16', 'n1-highmem-32', 'n1-highmem-64', 'n1-highmem-96',
    'n1-highcpu-2', 'n1-highcpu-4', 'n1-highcpu-8', 'n1-highcpu-16', 'n1-highcpu-32', 'n1-highcpu-64', 'n1-highcpu-96',
    'f1-micro', 'g1-small',
    'e2-standard-2', 'e2-standard-4', 'e2-standard-8', 'e2-standard-16', 'e2-standard-32',
    'e2-highmem-2', 'e2-highmem-4', 'e2-highmem-8', 'e2-highmem-16',
    'e2-highcpu-2', 'e2-highcpu-4', 'e2-highcpu-8', 'e2-highcpu-16', 'e2-highcpu-32',
    'e2-micro', 'e2-small', 'e2-medium',
    'n2-standard-2', 'n2-standard-4', 'n2-standard-8', 'n2-standard-16', 'n2-standard-32', 'n2-standard-48', 'n2-standard-64', 'n2-standard-80', 'n2-standard-96', 'n2-standard-128',
    'n2-highmem-2', 'n2-highmem-4', 'n2-highmem-8', 'n2-highmem-16', 'n2-highmem-32', 'n2-highmem-48', 'n2-highmem-64', 'n2-highmem-80', 'n2-highmem-96', 'n2-highmem-128',
    'n2-highcpu-2', 'n2-highcpu-4', 'n2-highcpu-8', 'n2-highcpu-16', 'n2-highcpu-32', 'n2-highcpu-48', 'n2-highcpu-64', 'n2-highcpu-80', 'n2-highcpu-96',
    'n2d-standard-2', 'n2d-standard-4', 'n2d-standard-8', 'n2d-standard-16', 'n2d-standard-32', 'n2d-standard-48', 'n2d-standard-64', 'n2d-standard-80', 'n2d-standard-96', 'n2d-standard-128', 'n2d-standard-224',
    'n2d-highmem-2', 'n2d-highmem-4', 'n2d-highmem-8', 'n2d-highmem-16', 'n2d-highmem-32', 'n2d-highmem-48', 'n2d-highmem-64', 'n2d-highmem-80', 'n2d-highmem-96',
    'n2d-highcpu-2', 'n2d-highcpu-4', 'n2d-highcpu-8', 'n2d-highcpu-16', 'n2d-highcpu-32', 'n2d-highcpu-48', 'n2d-highcpu-64', 'n2d-highcpu-80', 'n2d-highcpu-96', 'n2d-highcpu-128', 'n2d-highcpu-224',
    't2d-standard-1', 't2d-standard-2', 't2d-standard-4', 't2d-standard-8', 't2d-standard-16', 't2d-standard-32', 't2d-standard-48', 't2d-standard-60',
    'c2-standard-4', 'c2-standard-8', 'c2-standard-16', 'c2-standard-30', 'c2-standard-60',
    'c2d-standard-2', 'c2d-standard-4', 'c2d-standard-8', 'c2d-standard-16', 'c2d-standard-32', 'c2d-standard-56', 'c2d-standard-112',
    'c2d-highcpu-2', 'c2d-highcpu-4', 'c2d-highcpu-8', 'c2d-highcpu-16', 'c2d-highcpu-32', 'c2d-highcpu-56', 'c2d-highcpu-112',
    'c2d-highmem-2', 'c2d-highmem-4', 'c2d-highmem-8', 'c2d-highmem-16', 'c2d-highmem-32', 'c2d-highmem-56', 'c2d-highmem-112',
    'm1-ultramem-40', 'm1-ultramem-80', 'm1-ultramem-160', 'm1-megamem-96',
    'a2-highgpu-1g', 'a2-highgpu-2g', 'a2-highgpu-4g', 'a2-highgpu-8g', 'a2-megagpu-16g'
]

# https://cloud.google.com/compute/docs/regions-zones
region_list = [
    'us-central1', 'us-east1', 'us-east4', 'us-east5', 'us-west4', 'us-west1', 'us-west2', 'us-west3', 'us-south1',
    'europe-central2', 'europe-west1', 'europe-west2', 'europe-west3', 'europe-west4',
    'europe-west6', 'europe-west8', 'europe-west9', 'europe-north1', 'europe-southwest1',
    'northamerica-northeast1', 'northamerica-northeast2', 'asia-east1', 'asia-east2',
    'asia-northeast1', 'asia-northeast2', 'asia-northeast3', 'asia-southeast1',
    'australia-southeast1', 'australia-southeast2', 'southamerica-east1', 'asia-south1',
    'asia-southeast2', 'asia-south2', 'southamerica-west1'
]

# to parse regional price from vm instance pricing
# https://cloud.google.com/compute/vm-instance-pricing
region_mapping = {
    'io': 'us-central1', 'ore': 'us-west1', 'la': 'us-west2',
    'slc': 'us-west3', 'lv': 'us-west4', 'sc': 'us-east1',
    'nv': 'us-east4', 'col': 'us-east5', 'dl': 'us-south1',
    'mtreal': 'northamerica-northeast1', 'tor': 'northamerica-northeast2',
    'spaulo': 'southamerica-east1', 'sant': 'southamerica-west1', 'fi': 'europe-north1',
    'eu': 'europe-west1', 'lon': 'europe-west2', 'ffurt': 'europe-west3',
    'nether': 'europe-west4', 'zur': 'europe-west6', 'ml': 'europe-west8',
    'par': 'europe-west9', 'wsaw': 'europe-central2', 'mrid': 'europe-southwest1',
    'mbai': 'asia-south1', 'del': 'asia-south2', 'sg': 'asia-southeast1',
    'jk': 'asia-southeast2', 'syd': 'australia-southeast1', 'mel': 'australia-southeast2',
    'hk': 'asia-east2', 'tw': 'asia-east1', 'ja': 'asia-northeast1',
    'osa': 'asia-northeast2', 'kr': 'asia-northeast3'
}

# to calculate price from pricelist
# https://cloud.google.com/compute/docs/general-purpose-machines
n1 = {
    'n1-standard-1': {'cpu': 1, 'ram': 3.75},
    'n1-standard-2': {'cpu': 2, 'ram': 7.5},
    'n1-standard-4': {'cpu': 4, 'ram': 15},
    'n1-standard-8': {'cpu': 8, 'ram': 30},
    'n1-standard-16': {'cpu': 16, 'ram': 60},
    'n1-standard-32': {'cpu': 32, 'ram': 120},
    'n1-standard-64': {'cpu': 64, 'ram': 240},
    'n1-standard-96': {'cpu': 96, 'ram': 360},
    'n1-highmem-2': {'cpu': 2, 'ram': 13},
    'n1-highmem-4': {'cpu': 4, 'ram': 26},
    'n1-highmem-8': {'cpu': 8, 'ram': 52},
    'n1-highmem-16': {'cpu': 16, 'ram': 104},
    'n1-highmem-32': {'cpu': 32, 'ram': 208},
    'n1-highmem-64': {'cpu': 64, 'ram': 416},
    'n1-highmem-96': {'cpu': 96, 'ram': 624},
    'n1-highcpu-2': {'cpu': 2, 'ram': 1.8},
    'n1-highcpu-4': {'cpu': 4, 'ram': 3.6},
    'n1-highcpu-8': {'cpu': 8, 'ram': 7.2},
    'n1-highcpu-16': {'cpu': 16, 'ram': 14.4},
    'n1-highcpu-32': {'cpu': 32, 'ram': 28.8},
    'n1-highcpu-64': {'cpu': 64, 'ram': 57.6},
    'n1-highcpu-96': {'cpu': 96, 'ram': 86.4}
}

e2 = {
    'e2-micro': {'cpu': 0.25, 'ram': 1},
    'e2-small': {'cpu': 0.5, 'ram': 2},
    'e2-medium': {'cpu': 1, 'ram': 4},
    'e2-standard-2': {'cpu': 2, 'ram': 8},
    'e2-standard-4': {'cpu': 4, 'ram': 16},
    'e2-standard-8': {'cpu': 8, 'ram': 32},
    'e2-standard-16': {'cpu': 16, 'ram': 64},
    'e2-standard-32': {'cpu': 32, 'ram': 128},
    'e2-highmem-2': {'cpu': 2, 'ram': 16},
    'e2-highmem-4': {'cpu': 4, 'ram': 32},
    'e2-highmem-8': {'cpu': 8, 'ram': 64},
    'e2-highmem-16': {'cpu': 16, 'ram': 128},
    'e2-highcpu-2': {'cpu': 2, 'ram': 2},
    'e2-highcpu-4': {'cpu': 4, 'ram': 4},
    'e2-highcpu-8': {'cpu': 8, 'ram': 8},
    'e2-highcpu-16': {'cpu': 16, 'ram': 16},
    'e2-highcpu-32': {'cpu': 32, 'ram': 32}
}

n2 = {
    'n2-standard-2': {'cpu': 2, 'ram': 8},
    'n2-standard-4': {'cpu': 4, 'ram': 16},
    'n2-standard-8': {'cpu': 8, 'ram': 32},
    'n2-standard-16': {'cpu': 16, 'ram': 64},
    'n2-standard-32': {'cpu': 32, 'ram': 128},
    'n2-standard-48': {'cpu': 48, 'ram': 192},
    'n2-standard-64': {'cpu': 64, 'ram': 256},
    'n2-standard-80': {'cpu': 80, 'ram': 320},
    'n2-standard-96': {'cpu': 96, 'ram': 384},
    'n2-standard-128': {'cpu': 128, 'ram': 512},
    'n2-highmem-2': {'cpu': 2, 'ram': 16},
    'n2-highmem-4': {'cpu': 4, 'ram': 32},
    'n2-highmem-8': {'cpu': 8, 'ram': 64},
    'n2-highmem-16': {'cpu': 16, 'ram': 128},
    'n2-highmem-32': {'cpu': 32, 'ram': 256},
    'n2-highmem-48': {'cpu': 48, 'ram': 384},
    'n2-highmem-64': {'cpu': 64, 'ram': 512},
    'n2-highmem-80': {'cpu': 80, 'ram': 640},
    'n2-highmem-96': {'cpu': 96, 'ram': 768},
    'n2-highmem-128': {'cpu': 128, 'ram': 864},
    'n2-highcpu-2': {'cpu': 2, 'ram': 2},
    'n2-highcpu-4': {'cpu': 4, 'ram': 4},
    'n2-highcpu-8': {'cpu': 8, 'ram': 8},
    'n2-highcpu-16': {'cpu': 16, 'ram': 16},
    'n2-highcpu-32': {'cpu': 32, 'ram': 32},
    'n2-highcpu-48': {'cpu': 48, 'ram': 48},
    'n2-highcpu-64': {'cpu': 64, 'ram': 64},
    'n2-highcpu-80': {'cpu': 80, 'ram': 80},
    'n2-highcpu-96': {'cpu': 96, 'ram': 96}
}

n2d = {
    'n2d-standard-2': {'cpu': 2, 'ram': 8},
    'n2d-standard-4': {'cpu': 4, 'ram': 16},
    'n2d-standard-8': {'cpu': 8, 'ram': 32},
    'n2d-standard-16': {'cpu': 16, 'ram': 64},
    'n2d-standard-32': {'cpu': 32, 'ram': 128},
    'n2d-standard-48': {'cpu': 48, 'ram': 192},
    'n2d-standard-64': {'cpu': 64, 'ram': 256},
    'n2d-standard-80': {'cpu': 80, 'ram': 320},
    'n2d-standard-96': {'cpu': 96, 'ram': 384},
    'n2d-standard-128': {'cpu': 128, 'ram': 512},
    'n2d-standard-224': {'cpu': 224, 'ram': 896},
    'n2d-highmem-2': {'cpu': 2, 'ram': 16},
    'n2d-highmem-4': {'cpu': 4, 'ram': 32},
    'n2d-highmem-8': {'cpu': 8, 'ram': 64},
    'n2d-highmem-16': {'cpu': 16, 'ram': 128},
    'n2d-highmem-32': {'cpu': 32, 'ram': 256},
    'n2d-highmem-48': {'cpu': 48, 'ram': 384},
    'n2d-highmem-64': {'cpu': 64, 'ram': 512},
    'n2d-highmem-80': {'cpu': 80, 'ram': 640},
    'n2d-highmem-96': {'cpu': 96, 'ram': 768},
    'n2d-highcpu-2': {'cpu': 2, 'ram': 2},
    'n2d-highcpu-4': {'cpu': 4, 'ram': 4},
    'n2d-highcpu-8': {'cpu': 8, 'ram': 8},
    'n2d-highcpu-16': {'cpu': 16, 'ram': 16},
    'n2d-highcpu-32': {'cpu': 32, 'ram': 32},
    'n2d-highcpu-48': {'cpu': 48, 'ram': 48},
    'n2d-highcpu-64': {'cpu': 64, 'ram': 64},
    'n2d-highcpu-80': {'cpu': 80, 'ram': 80},
    'n2d-highcpu-96': {'cpu': 96, 'ram': 96},
    'n2d-highcpu-128': {'cpu': 128, 'ram': 128},
    'n2d-highcpu-224': {'cpu': 224, 'ram': 224}
}

t2d = {
    't2d-standard-1': {'cpu': 1, 'ram': 4},
    't2d-standard-2': {'cpu': 2, 'ram': 8},
    't2d-standard-4': {'cpu': 4, 'ram': 16},
    't2d-standard-8': {'cpu': 8, 'ram': 32},
    't2d-standard-16': {'cpu': 16, 'ram': 64},
    't2d-standard-32': {'cpu': 32, 'ram': 128},
    't2d-standard-48': {'cpu': 48, 'ram': 192},
    't2d-standard-60': {'cpu': 60, 'ram': 240}
}

c2 = {
    'c2-standard-4': {'cpu': 4, 'ram': 16},
    'c2-standard-8': {'cpu': 8, 'ram': 32},
    'c2-standard-16': {'cpu': 16, 'ram': 64},
    'c2-standard-30': {'cpu': 30, 'ram': 120},
    'c2-standard-60': {'cpu': 60, 'ram': 240}
}

c2d = {
    'c2d-standard-2': {'cpu': 2, 'ram': 8},
    'c2d-standard-4': {'cpu': 4, 'ram': 16},
    'c2d-standard-8': {'cpu': 8, 'ram': 32},
    'c2d-standard-16': {'cpu': 16, 'ram': 64},
    'c2d-standard-32': {'cpu': 32, 'ram': 128},
    'c2d-standard-56': {'cpu': 56, 'ram': 224},
    'c2d-standard-112': {'cpu': 112, 'ram': 448},
    'c2d-highmem-2': {'cpu': 2, 'ram': 16},
    'c2d-highmem-4': {'cpu': 4, 'ram': 32},
    'c2d-highmem-8': {'cpu': 8, 'ram': 64},
    'c2d-highmem-16': {'cpu': 16, 'ram': 128},
    'c2d-highmem-32': {'cpu': 32, 'ram': 256},
    'c2d-highmem-56': {'cpu': 56, 'ram': 448},
    'c2d-highmem-112': {'cpu': 112, 'ram': 896},
    'c2d-highcpu-2': {'cpu': 2, 'ram': 4},
    'c2d-highcpu-4': {'cpu': 4, 'ram': 8},
    'c2d-highcpu-8': {'cpu': 8, 'ram': 16},
    'c2d-highcpu-16': {'cpu': 16, 'ram': 32},
    'c2d-highcpu-32': {'cpu': 32, 'ram': 64},
    'c2d-highcpu-56': {'cpu': 56, 'ram': 112},
    'c2d-highcpu-112': {'cpu': 112, 'ram': 224}
}

m1 = {
    'm1-ultramem-40': {'cpu': 40.0, 'ram': 961.0},
    'm1-ultramem-80': {'cpu': 80.0, 'ram': 1922.0},
    'm1-ultramem-160': {'cpu': 160.0, 'ram': 3844.0},
    'm1-megamem-96': {'cpu': 96.0, 'ram': 1433.6}
}

a2 = {
    'a2-highgpu-1g': {'cpu': 12, 'ram': 85, 'gpu': 1},
    'a2-highgpu-2g': {'cpu': 24, 'ram': 170, 'gpu': 2},
    'a2-highgpu-4g': {'cpu': 48, 'ram': 340, 'gpu': 4},
    'a2-highgpu-8g': {'cpu': 96, 'ram': 680, 'gpu': 8},
    'a2-megagpu-16g': {'cpu': 96, 'ram': 1360, 'gpu': 16}
}
