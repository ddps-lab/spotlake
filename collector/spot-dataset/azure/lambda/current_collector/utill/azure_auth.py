import json
import requests
import time
from utill.dynamodb import DynamoDB


def get_token():
    db = DynamoDB("AzureAuth")

    now = int(time.time())

    expire = db.get_item('expire')
    if expire - 300 > now:
        access_token = db.get_item('access_token')
        return access_token

    realm = db.get_item('realm')
    client_id = db.get_item('client_id')
    refresh_token = db.get_item('refresh_token')

    data = requests.post(f'https://login.microsoftonline.com/{realm}/oauth2/v2.0/token', data={'client_id': client_id, 'grant_type': 'refresh_token', 'client_info': '1',
                         'claims': '{"access_token": {"xms_cc": {"values": ["CP1"]}}}', 'refresh_token': refresh_token, 'scope': 'https://management.core.windows.net//.default offline_access openid profile'}).json()

    access_token = data['access_token']
    refresh_token = data['refresh_token']
    expires_in = data['expires_in']

    db.put_item('access_token', access_token)
    db.put_item('refresh_token', refresh_token)
    db.put_item('expire', now + expires_in)

    return access_token
