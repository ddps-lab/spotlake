import boto3
import time
import requests
import io
import pickle
import json
from datetime import datetime, timedelta


bucket_name = 'spotlake'
s3_path_date = (datetime.today().date() + timedelta(days=-1)).strftime('%Y/%m/%d')
s3 = boto3.client('s3')
athena_handler = boto3.client('athena',region_name='us-west-2')


def create_athena_table():
    athena_handler.start_query_execution(
        QueryString=f"""
             CREATE EXTERNAL TABLE IF NOT EXISTS aws (
                instancetype string,
                az string,
                spotprice double,
                region string,
                sps int,
                ondemandprice double,
                interruptfrequency double,
                savings int
              ) 
              ROW FORMAT DELIMITED
              FIELDS TERMINATED BY ','
              LINES TERMINATED BY '\n'
              LOCATION 's3://spotlake/rawdata/aws/{s3_path_date}'
              TBLPROPERTIES ("skip.header.line.count"="1"); 
             """,
        ResultConfiguration={'OutputLocation': f's3://spotlake-count-log/create-athena-table/{s3_path_date}/'}
    )
    print("create table")



def query_athena(query_string):
    query_start = athena_handler.start_query_execution(
        QueryString=query_string,
        ResultConfiguration={'OutputLocation': f's3://spotlake-count-log/query_athena/{s3_path_date}/'}
    )

    while True:
        query_status = athena_handler.get_query_execution(
            QueryExecutionId=query_start['QueryExecutionId'])
        query_execution_status = query_status['QueryExecution']['Status'][
            'State']

        if query_execution_status == 'SUCCEEDED':
            result = athena_handler.get_query_results(
                QueryExecutionId=query_start['QueryExecutionId'])
            break

        print("sleep")
        time.sleep(1)

    print(result)
    return result["ResultSet"]['Rows'][1]['Data'][0]['VarCharValue']


def get_workload_num():
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=f'monitoring/{s3_path_date}/workloads.pkl')
        datas = pickle.load(io.BytesIO(obj["Body"].read()))
        azs = 0
        for instance, query in datas.items():
            for ra in query:
                azs += ra[1]
        return azs
    except Exception as e:
        print(e)


def send_message(sps, interruptfrequency, spot, ondemand, num_of_workloads):
    url = '' #slackurl
    result_msg = f"""
    <{datetime.today().date()} spotlake_workload_monitoring>\n
    - The number of ingested records.\n
        - SPS : {sps}
        - IF  : {int(interruptfrequency)*144}
        - spot: {spot}
        - ondemand : {ondemand}
    - The number of records that must be ingested. : {num_of_workloads*144}"""
    data = {'text': result_msg}
    resp = requests.post(url=url, json=data)
    return data


def lambda_handler(event, context):
    create_athena_table()

    sps = query_athena('SELECT count(*)FROM "default"."aws" where sps != -1;')
    interruptfrequency = query_athena('SELECT COUNT(DISTINCT(instancetype, region)) FROM "default"."aws" WHERE interruptfrequency != -1')
    spotprice = query_athena('SELECT count(*)FROM "default"."aws" where spotprice != -1;')
    ondemandprice = query_athena('SELECT count(*)FROM "default"."aws" where ondemandprice != -1;')
    num_of_workload = get_workload_num()

    message = send_message(sps, interruptfrequency, spotprice, ondemandprice, num_of_workload)

    return {
        'statusCode': 200,
        'body': json.dumps(message)
    }


