import json
import boto3
import requests
import time

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('AzureAuth')


def get_item(key):
    global table
    return table.get_item(Key={'id': key})['Item']['data']


def put_item(id, data):
    global table
    table.put_item(Item={'id': id, 'data': data})


def lambda_handler(event, context):
    now = int(time.time())

    expire = get_item('expire')
    if expire - 300 > now:
        access_token = get_item('access_token')
        return access_token

    realm = get_item('realm')
    client_id = get_item('client_id')
    refresh_token = get_item('refresh_token')

    data = requests.post(f'https://login.microsoftonline.com/{realm}/oauth2/v2.0/token', data={'client_id': client_id, 'grant_type': 'refresh_token', 'client_info': '1',
                         'claims': '{"access_token": {"xms_cc": {"values": ["CP1"]}}}', 'refresh_token': refresh_token, 'scope': 'https://management.core.windows.net//.default offline_access openid profile'}).json()

    access_token = data['access_token']
    refresh_token = data['refresh_token']
    expires_in = data['expires_in']

    put_item('access_token', access_token)
    put_item('refresh_token', refresh_token)
    put_item('expire', now + expires_in)

    return access_token
