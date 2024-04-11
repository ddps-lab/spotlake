import boto3
import sys
import time
import botocore
import json
from datetime import datetime, timedelta, timezone

from slack_msg_sender import send_slack_message

S3_PATH_DATE = (datetime.now(timezone.utc).date() + timedelta(days=-1)).strftime("%Y/%m/%d")

S3_LOG_BUCKET = "sps-athena-log"
TABLE_NAME = "sps_server_monitor"

def query_athena(athena_client, query_string, result_configuration):
    print(f"---- will query ----\n{query_string}")
    try:
        query_start = athena_client.start_query_execution(
            QueryString=query_string,
            ResultConfiguration=result_configuration
        )
    except Exception as e:
        message = f"""
            {S3_PATH_DATE}\n
            --- Exception while start athena query ---\n
            {query_string}\n
            --------------------------------------------
            {e}
        """
        raise Exception(send_slack_message(message))
    
    retries = 0
    max_retries = 3
    result = None
    while retries <= max_retries:
        query_status = athena_client.get_query_execution(QueryExecutionId=query_start['QueryExecutionId'])
        query_execution_status = query_status['QueryExecution']['Status']['State']

        if query_execution_status == 'SUCCEEDED':
            result = athena_client.get_query_results(QueryExecutionId=query_start['QueryExecutionId'])
            break
        elif query_execution_status in ['CANCELLED', 'FAILED']:
            message = f"""
                {S3_PATH_DATE}\n
                --- Exception while get athena query ---\n
                {query_string}\n
                ----------------------------------------
                {e}
            """
            raise Exception(send_slack_message(message))

        print(f"sleep {2**retries} seconds for waiting query")
        time.sleep(2**retries)
        retries += 1
    
    if result == None:
        message = f"""
            {S3_PATH_DATE}\n
            --- trying 3 times over athena query using exeponential backoff ---\n
            {query_string}\n
            -------------------------------------------------------------------
            {e}
        """
        raise Exception(send_slack_message(message))

    return result

def create_athena_table(athena_client, data_bucket_name):
    # drop table if exist
    drop_athena_table(athena_client)
    # create table
    query_string = f"""
                CREATE EXTERNAL TABLE IF NOT EXISTS {TABLE_NAME}(
                    instancetype string,
                    region string,
                    az string,
                    SPS int,
                    TargetCapacity int
                )
                ROW FORMAT DELIMITED
                FIELDS TERMINATED BY ','
                LINES TERMINATED BY '\n'
                LOCATION 's3://{data_bucket_name}/aws/{S3_PATH_DATE}'
                TBLPROPERTIES ("skip.header.line.count"="1");
            """
    result_configuation = {'OutputLocation': f"s3://{S3_LOG_BUCKET}/create-athena-table/{S3_PATH_DATE}/"}
    try:
        result = query_athena(athena_client, query_string, result_configuation)
    except Exception as e:
        send_slack_message(f"exception at aws sps server monitoring create_athena_table()\n{query_string}\n--- error msg ---\n{e}")
        raise e
    print(f"create table {TABLE_NAME}")

def drop_athena_table(athena_client):
    query_string = f"DROP TABLE IF EXISTS {TABLE_NAME}"
    result_configuration = {'OutputLocation': f"s3://{S3_LOG_BUCKET}/drop-athena-table/{S3_PATH_DATE}/"}
    try:
        result = query_athena(athena_client, query_string, result_configuration)
    except Exception as e:
        print(f"The error message : {e}")
        raise e
    
def make_message(athena_client):
    target_capacities = [5, 10, 15, 20, 25, 30, 35, 40, 45, 50]
    result_configuration = {'OutputLocation': f's3://{S3_LOG_BUCKET}/check-table/{S3_PATH_DATE}/'}
    message = f"<{datetime.today().date()} sps server monitoring - AWS>"
    message += f"\n```- the number of collected sps data per Target Capacity"

    for target_capacity in target_capacities:
        query_string = f'SELECT COUNT(*) FROM "default"."{TABLE_NAME}" WHERE SPS != -1 AND TargetCapacity = {target_capacity};'
        result = query_athena(athena_client, query_string, result_configuration)
        num_rows = result['ResultSet']['Rows'][1]['Data'][0]['VarCharValue']
        message += f"\n\tTarget Capacity {target_capacity:2} : {num_rows}"

    # Total Number of Row which Collected yesterday data
    query_string = f'SELECT COUNT(*) FROM "default"."{TABLE_NAME}" WHERE SPS != -1;'
    result_sps = query_athena(athena_client, query_string, result_configuration)
    num_rows = result_sps['ResultSet']['Rows'][1]['Data'][0]['VarCharValue']
    message += f"\nTotal number of row collected sps data : {num_rows}```"

    return message

def lambda_handler(event, context):
    athena_client = boto3.client('athena', region_name='us-west-2')
    create_athena_table(athena_client, 'sps-query-data')
    result_message = make_message(athena_client)
    print(result_message)
    message_json = send_slack_message(result_message)
    drop_athena_table(athena_client)

    return {
        'statusCode': 200,
        'body': json.dumps(message_json)
    }

if __name__ == "__main__":
    import os
    session = boto3.session.Session(
        aws_access_key_id = os.environ.get("AWS_ACCESS_KEY"),
        aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    )
    athena_client = session.client('athena', region_name='us-west-2')
    create_athena_table(athena_client, 'sps-query-data')
    result_message = make_message(athena_client)
    print(f"---- raw ----\n{result_message}")
    message_json = send_slack_message(result_message)
    drop_athena_table(athena_client)
    print(message_json)