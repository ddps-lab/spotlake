import json
import urllib.parse
import boto3
import pandas as pd
import gzip
import dbinfo
import pymysql
from io import BytesIO
import time

connection = pymysql.connect(
    host = dbinfo.db_host,
    user = dbinfo.db_username,
    passwd = dbinfo.db_password,
    db = dbinfo.db_name,
    port = dbinfo.db_port
)

s3 = boto3.client('s3')


def lambda_handler(event, context):
    try:
        start = time.time()
        _gz, _json = False, False
        
        cursor = connection.cursor() # establish DB Connection
    
        # Get the object from the event and show its content type
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8') # get name of the file including extension
        
        # distinguish the file extension
        if key.endswith(".csv.gz"): _gz = True;
        elif key.endswith(".json"): _json = True;
        
        response = s3.get_object(Bucket=bucket, Key=key)
        
        # extract the latest data of response body
        if _gz:
            with gzip.GzipFile(fileobj=BytesIO(response['Body'].read())) as gz:
                # Load the content into a pandas DataFrame
                latest_data = pd.read_csv(gz)
        if _json:
            latest_data = pd.DataFrame(json.load(response['Body']))
        
        # Print the DataFrame to verify
        #print(latest_data.head())
        
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

    # According to the structure of datasets, their queries might be different. In terms of that, we separated these tasks.        
    if _gz: sql = gzQuery(latest_data)
    if _json: sql = jsonQuery(latest_data)

        
    # Execute the sql query.
    cursor.execute(sql)
    connection.commit()
    return response['ContentType']

# SPS5 ~ 50
def gzQuery(latest_data):
    capacity = latest_data[['TargetCapacity'][0]][1]
    sps_name = 'SPS' + str(capacity)
    latest_data = latest_data[['InstanceType', 'Region', 'AZ', 'SPS']].rename(columns={'SPS' : sps_name})
    
    # sql query constructor    
    values = list(zip(
        latest_data['InstanceType'], latest_data['Region'], latest_data['AZ'], latest_data[sps_name]
    ))
    #print(values)
    
    sql = f"""
        INSERT INTO `spotlake`.`sps` (`InstanceType`, `Region`, `AZ`, `{sps_name}`) VALUES
        """
        
    values_str = ', '.join([f"('{v[0]}', '{v[1]}', '{v[2]}', {v[3]})" for v in values])
    sql = sql + values_str + f"""
                ON DUPLICATE KEY UPDATE `InstanceType` = VALUES(`InstanceType`), `Region` = VALUES(`Region`), `AZ` = VALUES(`AZ`), `{sps_name}` = VALUES(`{sps_name}`);
                """
                
    return sql
    
    
# SPS1
def jsonQuery(latest_data):
    capacity = 1
    sps_name = "SPS1"
    latest_data = latest_data[['InstanceType', 'Region', 'AZ', 'SPS', 'SpotPrice']].rename(columns={'SPS' : sps_name})
    
     # sql query constructor    
    values = list(zip(
        latest_data['InstanceType'], latest_data['Region'], latest_data['AZ'], latest_data[sps_name], latest_data['SpotPrice']
    ))
    #print(values)
    
    sql = f"""
        INSERT INTO `spotlake`.`sps` (`InstanceType`, `Region`, `AZ`, `{sps_name}`, `SpotPrice`) VALUES
        """
        
    values_str = ', '.join([f"('{v[0]}', '{v[1]}', '{v[2]}', {v[3]}, {v[4]})" for v in values])
    sql = sql + values_str + f"""
                ON DUPLICATE KEY UPDATE `InstanceType` = VALUES(`InstanceType`), `Region` = VALUES(`Region`), `AZ` = VALUES(`AZ`), `{sps_name}` = VALUES(`{sps_name}`), `SpotPrice` = VALUES(`SpotPrice`);
                """
                
    return sql