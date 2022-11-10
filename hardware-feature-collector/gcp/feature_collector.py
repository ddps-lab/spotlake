import pandas as pd
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

credentials = GoogleCredentials.get_application_default()
service = discovery.build('compute', 'v1', credentials=credentials)

PROJECT_ID = 'gcp-hw-feature-collector'

meta_data = []
request = service.machineTypes().aggregatedList(project=PROJECT_ID, includeAllScopes=True)
while request is not None:
    response = request.execute()

    for name, machine_types_scoped_list in response['items'].items():
        if machine_types_scoped_list.get('machineTypes') != None:
            item = (name, machine_types_scoped_list)
            meta_data.append(item)
    
    request = service.machineTypes().aggregatedList_next(previous_request=request, previous_response=response)

feature_list = {}
for data in meta_data:
    region = data[0]
    instance_types_list = data[1]['machineTypes']

    for instance in instance_types_list:
        instance_type = instance['name']

        feature_list[instance_type] = { 'InstanceType' : instance_type,
                                        'guestCpus' : instance['guestCpus'],
                                        'memory(Mb)' : instance['memoryMb'],
                                        'maximumPersistentDisks' : instance['maximumPersistentDisks'],
                                        'maximumPersistentDisksSize(Gb)' : instance['maximumPersistentDisksSizeGb']}
        try:
            feature_list[instance_type]['GuestAcceleratorType'] = instance['accelerators'][0]['guestAcceleratorType']
        except:
            feature_list[instance_type]['GuestAcceleratorType'] = None
        
        try:
            feature_list[instance_type]['GuestAcceleratorCount'] = instance['accelerators'][0]['guestAcceleratorCount']
        except:
            feature_list[instance_type]['GuestAcceleratorCount'] = None

df_gcp_instances = pd.DataFrame(feature_list.values())
df_gcp_instances.to_pickle('./gcp_hardware_feature.pkl')
