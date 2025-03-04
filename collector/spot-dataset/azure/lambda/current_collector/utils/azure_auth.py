import os
import requests
import time
from azure.identity import ClientSecretCredential
from utils.pub_service import DB_AzureAuth
from azure.core.exceptions import ClientAuthenticationError

def get_token():
    db = DB_AzureAuth

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


def get_sps_token_and_subscriptions():
    tenant_id = os.environ.get('TENANT_ID')
    client_id = os.environ.get('CLIENT_ID')
    client_secret = os.environ.get('CLIENT_SECRET')
    subscriptions = os.environ.get('SUBSCRIPTIONS')

    if not all([tenant_id, client_id, client_secret, subscriptions]):
        raise ValueError("Missing required environment variables: TENANT_ID, CLIENT_ID, CLIENT_SECRET, or SUBSCRIPTIONS")

    subscriptions = [sub.strip() for sub in subscriptions.split(",") if sub.strip()]

    try:
        credential = ClientSecretCredential(tenant_id, client_id, client_secret)
        sps_token = credential.get_token("https://management.azure.com/.default").token
    except ClientAuthenticationError as e:
        raise ValueError(f"Failed to authenticate with Azure: {e}")

    return sps_token, subscriptions