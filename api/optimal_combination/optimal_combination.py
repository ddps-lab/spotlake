import boto3
import json
import pandas as pd
import time
import algorithm
import pymysql
import dbinfo
import variables
from datetime import datetime, timedelta, timezone

connection = pymysql.connect(
    host = dbinfo.db_host,
    user = dbinfo.db_username,
    passwd = dbinfo.db_password,
    db = dbinfo.db_name,
    port = dbinfo.db_port
)

def lambda_handler(event, context):
    start = time.time()
    
    # query parameter preprocessing
    get_body = event['queryStringParameters']
    instance_types = eval(get_body.get("InstanceTypes", "['m5']"))
    regions = eval(get_body.get("Regions", "['us-west-2']"))
    function_num = int(get_body.get("FunctionNum", '10'))
    mpf = int(get_body.get("MemPerFunction", '2'))
    cpf = int(get_body.get("CorePerFunction", '1'))
    
    # connection establishment
    cursor = connection.cursor()
    
    # fetch all
    sql = "SELECT * FROM `spotlake`.`sps`;"
    cursor.execute(sql)
    rows = cursor.fetchall()

    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(rows, columns=columns)
    
    # coremark, hw_info, Family merge task
    coremark = pd.DataFrame(variables.coremark)
    hw_info = pd.DataFrame(variables.hw_info)
    
    
    df = pd.merge(df, coremark, on=['InstanceType'])
    df = pd.merge(df, hw_info, on=['InstanceType'])
    df = df[df['Memory'] >= mpf]
    df = df[df['vCPU'] >= cpf]
    
    df['Family'] = df['InstanceType'].str.split('.').str[0] 
    df = df.loc[df['Family'].isin(instance_types) & df['Region'].isin(regions)]
    
    # change the structure for algorithm.py
    sps_columns = [col for col in df.columns if col.startswith('SPS')]
    
    df_melted = df.melt(id_vars=['InstanceType', 'Region', 'AZ', 'SpotPrice', 'CoreMark', 'vCPU', 'Memory', 'Family'], value_vars=sps_columns, var_name='TargetCapacity', value_name='SPS')
    df_melted['TargetCapacity'] = df_melted['TargetCapacity'].str.replace('SPS', '')
    
    # exclude the non-numeric value of sps
    df_melted = df_melted[pd.to_numeric(df_melted['SPS'], errors='coerce').notna()].astype({'SPS': int})
    #print("[All Data Preprocessing] time: ", time.time() - start)
    
    
    # new_algorithm task
    max_instance_df = algorithm.find_max_instance(df_melted).dropna()
    max_instance_df['Max_Instance'] = max_instance_df['Max_Instance'].astype(int)
    latest_data = pd.merge(df_melted, max_instance_df)
    min_cost, combination = algorithm.select_instance(mpf, cpf, function_num, latest_data)

    count_instance_with_limits = {}
    for key, value in combination.items():
        count_instance_with_limits[str(key)] = value
    count_instance_with_limits['TotalPrice'] = min_cost
    # TODO implement
    
    return {
        'statusCode': 200,
        'body': json.dumps([count_instance_with_limits])
    }
    
    
