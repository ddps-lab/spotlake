import sys
import time
import boto3
import botocore
from datetime import datetime, timedelta, timezone

sys.path.append("/home/ubuntu/spotlake/utility")
from slack_msg_sender import send_slack_message

S3_PATH_DATE = (datetime.now(timezone.utc).date() + timedelta(days=-1)).strftime("%Y/%m/%d")
S3_LOG_BUCKET = "sps-athena-log"
TABLE_NAME = "sps_server_monitor"

def query_athena(athena_client, query_string, result_configuration):
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

        print(f"sleep {2**retries} seconds")
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
                    az int,
                    1 int,
                    5 int,
                    10 int,
                    15 int,
                    20 int,
                    25 int,
                    30 int,
                    35 int,
                    40 int,
                    45 int,
                    50 int
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
        raise e
    print(f"create table {TABLE_NAME}")

def drop_athena_table(athena_client):
    query_string = f"DROP TABLE IF EXIST {TABLE_NAME}"
    result_configuration = {'OutputLocation': f"s3://{S3_LOG_BUCKET}/drop-athena-table/{S3_PATH_DATE}/"}
    try:
        result = query_athena(athena_client, query_string, result_configuration)
    except Exception as e:
        raise e
    
def make_message(athena_client):
    sps_query_string = f"SELECT COUNT(*) FROM 'default'.'{TABLE_NAME}'"
    result_configuration = {'OutputLocation': f's3://{S3_LOG_BUCKET}/check-table/{S3_PATH_DATE}/'}
    result_sps = query_athena(athena_client, sps_query_string, result_configuration)
    pass
