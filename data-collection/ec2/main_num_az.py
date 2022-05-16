import time
import boto3
import os
import pickle
from datetime import datetime
from ec2_package import *
from collections import Counter


def collect_num_az():
    start = time.time()
    
    session = boto3.session.Session(profile_name='jaeil')
    
    regions = get_regions(session)
    # print(f'region list: {regions}')
    print(f'total {len(regions)} regions')
    
    counter = Counter()
    for idx, region in enumerate(regions):
        print(f'{idx + 1}/{len(regions)} {region}')
    
        for it in get_it_az(session, region):
            counter += {(region, it): 1}
    
    result = dict()
    for key, cnt in counter.items():
        region, it = key
        if it in result:
            result[it].append((region, cnt))
        else:
            result[it] = [(region, cnt)]
    
    print(result)
    
    pickle.dump(result, open('/home/ec2-user/WorkloadCreator/base.pickle', 'wb'))
    
    print('end process, the running time is', f'{time.time() - start}')

# from pprint import pprint
#
# with open('2022-03-23 16:59:30.bin', 'rb') as file:
#     data = pickle.load(file)
# pprint(data)
# print(len(data))
