import subprocess
import datetime
import boto3
import json
import io

BUCKET_NAME = ''
COMMAND = './spotinfo --output csv --region all'
s3 = boto3.resource('s3')
s3_bucket = s3.Bucket(BUCKET_NAME)

def get_spotinfo(command):
  process = subprocess.Popen(command.split(' '),
                             stdout=subprocess.PIPE)
  stdout, stderr = process.communicate()
  csv_string = stdout.decode('utf-8')
  return csv_string

def handler(event, context):
  now = datetime.datetime.now()
  now_time = now.strftime('%H:%M:%S')

  spotinfo_string = get_spotinfo(COMMAND)
  filename = f'spotinfo-{now_time}.txt'

  # save data to S3
  s3_path = f'{now.year}/{now.month}/{now.day}/{filename}'
  s3_bucket.put_object(Key=s3_path, Body=spotinfo_string)  
  
  return {
    'statusCode': 200,
    'body': json.dumps(f'{s3_path} successfully saved to {BUCKET_NAME} bucket!')
  }
