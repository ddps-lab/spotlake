import json
import pandas as pd
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

from upload_data import upload_metadata

LOCAL_PATH = 'tmp/'

def trim_region(before_trim):
    region = before_trim
    if before_trim != 'global':
        region = before_trim.split('/')[1]
        region = region.split('-')[0] + '-' + region.split('-')[1]
    return region


def get_aggregated_list():
    # get data from google api : https://cloud.google.com/compute/docs/reference/rest/v1/machineTypes/aggregatedList
    # input : none
    # output : dataframe (rawdata)
    credentials = GoogleCredentials.get_application_default()
    service = discovery.build('compute', 'v1', credentials=credentials)
    project = 'gcp-hw-feature-collector'
    request = service.machineTypes().aggregatedList(project=project, includeAllScopes=True)

    # get aggregated list from api
    rawdata = []
    while request is not None:
        response = request.execute()

        for name, machine_types_scoped_list in response['items'].items():
            item = (name, machine_types_scoped_list)
            rawdata.append(item)

        request = service.machineTypes().aggregatedList_next(previous_request=request, previous_response=response)
    
    df_raw = pd.DataFrame(rawdata, columns=['scope', 'content'])

    return df_raw


def parsing_data_from_aggragated_list(df):
    # parse available region list & instance metadata from api rawdata
    # input : dataframe (rawdata)
    # output : dict (available region lists), dataframe (instance spec)
    available_region_lists = {}
    instance_metadata = []

    for idx, value in df.iterrows():
        # trim region
        region = trim_region(value['scope'])

        # check content is 'WARNING' or 'MACHINETYPES'
        data = value['content'].get('machineTypes')
        if data != None:
            for instance in data:
                # add instance type in available region
                if region not in available_region_lists:
                    available_region_lists[region] = []
                available_region_lists[region].append(instance['name'])

                # add instance spec in instance metadata
                spec = {'instance_type' : instance['name'],
                    'guest_cpus' : instance['guestCpus'],
                    'memoryGB' : round((instance['memoryMb'] / 1024), 2)      # convert Mebibyte to Gibigyte
                    }
                if 'accelerators' in instance:
                    spec['guest_accelerator_type'] = instance['accelerators'][0]['guestAcceleratorType']
                    spec['guest_accelerator_count'] = instance['accelerators'][0]['guestAcceleratorCount']
                    if 'ultragpu' in instance['name']:
                        spec['ssd'] = instance['accelerators'][0]['guestAcceleratorCount']
                
                instance_metadata.append(spec)
    
    df_instance_metadata = pd.DataFrame(instance_metadata)
    df_instance_metadata.drop_duplicates(subset=['instance_type'], keep='first', inplace=True, ignore_index=True )
    df_instance_metadata['guest_accelerator_type'] = df_instance_metadata['guest_accelerator_type'].fillna(0)
    df_instance_metadata['ssd'] = df_instance_metadata['ssd'].fillna(0)
    
    df_instance_metadata.to_json(f'{LOCAL_PATH}/instance_metadata.json')    
    with open(f'{LOCAL_PATH}/available_region_lists.json', 'w') as f:
        f.write(json.dumps(available_region_lists))     
    
    upload_metadata('available_region_lists')
    upload_metadata('instance_metadata')
