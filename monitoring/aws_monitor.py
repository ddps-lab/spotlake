from datetime import datetime, timedelta
import boto3
import requests
import json
import pickle
import io

session = boto3.Session()
query_client = session.client('timestream-query')
client = query_client
daily_count = 0
result_msg = ""
start_date = (datetime.today().date() + timedelta(days=-1)).strftime('%Y-%m-%d %H:%M:%S')
end_date = (datetime.today().date() + timedelta(days=0)).strftime('%Y-%m-%d %H:%M:%S')
s3 = boto3.client('s3')
bucket_name = '' #bucket_name


# generate query_string and get the count of yesterday data from Timestream.
def run_query():
    try:
        query_string = f"""SELECT count(*) FROM "spotrank-timestream"."spot-table" WHERE time between timestamp '{start_date}' and timestamp '{end_date}' """
        response = client.query(QueryString=query_string)
        daily_count = response['Rows'][0]['Data'][0]['ScalarValue']
        num_of_azs = get_azs()
        send_message(daily_count, num_of_azs)
    except Exception as err:
        print("Exception while running query:", err)


# send data count message to slack
def send_message(count, num_of_azs):
    global result_msg
    url = ''  # slackurl
    result_msg = """<%s monitoring> \n- number of data ingested : %s\n- number of data must be ingested : %d""" % (datetime.today().date(), count, num_of_azs * 144)
    data = {'text': result_msg}
    resp = requests.post(url=url, json=data)
    return resp

# get number of all Region-InstanceType-AvailabilityZone from s3 pkl file
def get_azs():
    try:
        obj = s3.get_object(Bucket=bucket_name, Key='base.pickle')
        datas = pickle.load(io.BytesIO(obj["Body"].read()))
        azs = 0
        for instance, query in datas.items():
            for ra in query:
                azs += ra[1]
        return azs
    except Exception as e:
        print(e)


def lambda_handler(event, context):
    global result_msg
    run_query()
    return {
        'statusCode': 200,
        'body': json.dumps(result_msg)
    }