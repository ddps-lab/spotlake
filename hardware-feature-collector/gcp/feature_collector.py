import json
import pandas as pd
import boto3
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

SAVE_FILENAME = '/tmp/gcp_hardware_feature.csv'
BUCKET_NAME = 'hardware-feature'
PROJECT_ID = 'gcp-hw-feature-collector'

def get_authentication():
    credentials = GoogleCredentials.get_application_default()
    service = discovery.build('compute', 'v1', credentials=credentials)
    
    return service


def get_hardware_feature(service):
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
                                            'memory(Mb)' : instance['memoryMb']}
            try:
                feature_list[instance_type]['GuestAcceleratorType'] = instance['accelerators'][0]['guestAcceleratorType']
            except:
                feature_list[instance_type]['GuestAcceleratorType'] = None
            
            try:
                feature_list[instance_type]['GuestAcceleratorCount'] = instance['accelerators'][0]['guestAcceleratorCount']
            except:
                feature_list[instance_type]['GuestAcceleratorCount'] = None
    
    df_gcp_instances = pd.DataFrame(feature_list.values())
    df_gcp_instances.to_csv(SAVE_FILENAME, index=False)

    session = boto3.Session()
    s3 = session.client('s3')
    with open(SAVE_FILENAME, 'rb') as f:
        s3.upload_fileobj(f, BUCKET_NAME, 'gcp/hardware_feature.csv')


def lambda_handler(event, context):
    service = get_authentication()
    get_hardware_feature(service)

    return {
        'statusCode': 200,
        'body': json.dumps('')
    }
