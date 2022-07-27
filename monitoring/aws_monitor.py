from datetime import datetime, timedelta
import boto3
import requests
import json

session = boto3.Session()
query_client = session.client('timestream-query')
client = query_client
daily_count = 0
result_msg = ""
start_date = (datetime.today().date() + timedelta(days=-1)).strftime('%Y-%m-%d %H:%M:%S')
end_date = (datetime.today().date() + timedelta(days=0)).strftime('%Y-%m-%d %H:%M:%S')

#generate query_string and get the count of yesterday data from Timestream.
def run_query():
    try:
        query_string =  f"""SELECT count(*) FROM "spotrank-timestream"."spot-table" WHERE time between timestamp '{start_date}' and timestamp '{end_date}' """
        response = client.query(QueryString=query_string)
        daily_count = response['Rows'][0]['Data'][0]['ScalarValue']
        print(daily_count)
        send_message(daily_count)
    except Exception as err:
        print("Exception while running query:", err)

#send data count message to slack
def send_message(count):
    global result_msg
    url=''#slackurl
    result_msg = "%s ingestion data count : %s"%(datetime.today().date(), count)
    data = {'text':result_msg}
    resp = requests.post(url=url, json=data)
    return resp

def lambda_handler(event, context):
    global result_msg
    run_query()
    return {
        'statusCode': 200,
        'body': json.dumps(result_msg)
    }
