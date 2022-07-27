from datetime import datetime, timedelta
import boto3
import requests
import json

session = boto3.Session()
query_client = session.client('timestream-query')
client = query_client
today = datetime.today() + timedelta(days=-1)
daily_count = 0

def run_query():
    try:
        query_string = today_sql()
        response = client.query(QueryString=query_string)
        daily_count = response['Rows'][0]['Data'][0]['ScalarValue']
        send_message(daily_count)
    except Exception as err:
        print("Exception while running query:", err)


def today_sql():
    return f"""SELECT count(*) FROM "spotrank-timestream"."spot-table" WHERE time between timestamp 
'%s-%s-%s 00:00:00' and timestamp '%s-%s-%s 23:59:59' """%(today.year,today.month,today.day,today.year,today.month,today.day)
def send_message(count):
    url=''#slackurl
    msg = "%s ingestion data count : %s"%(str(today.date()), count)
    data = {'text':msg}
    resp = requests.post(url=url, json=data)
    return resp


def lambda_handler(event, context):
    run_query()
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
