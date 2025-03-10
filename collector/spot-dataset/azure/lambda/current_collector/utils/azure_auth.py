import os
import requests
import time
from azure.identity import ClientSecretCredential
from utils.pub_service import DB_AzureAuth
from azure.core.exceptions import ClientAuthenticationError

def get_token():
    now = int(time.time())

    expire = DB_AzureAuth.get_item('expire')
    if expire - 300 > now:
        access_token = DB_AzureAuth.get_item('access_token')
        return access_token

    realm = DB_AzureAuth.get_item('realm')
    client_id = DB_AzureAuth.get_item('client_id')
    refresh_token = DB_AzureAuth.get_item('refresh_token')

    data = requests.post(f'https://login.microsoftonline.com/{realm}/oauth2/v2.0/token', data={'client_id': client_id, 'grant_type': 'refresh_token', 'client_info': '1',
                         'claims': '{"access_token": {"xms_cc": {"values": ["CP1"]}}}', 'refresh_token': refresh_token, 'scope': 'https://management.core.windows.net//.default offline_access openid profile'}).json()

    access_token = data['access_token']
    refresh_token = data['refresh_token']
    expires_in = data['expires_in']

    DB_AzureAuth.put_item('access_token', access_token)
    DB_AzureAuth.put_item('refresh_token', refresh_token)
    DB_AzureAuth.put_item('expire', now + expires_in)

    return access_token


def get_sps_token_and_subscriptions():
    auth_data = DB_AzureAuth.get_item('login_auth')
    tenant_id = auth_data['tenant_id']
    client_id = auth_data['client_id']
    client_secret = auth_data['client_secret']
    subscriptions = auth_data['subscriptions'].split(",")


    if not all([tenant_id, client_id, client_secret, subscriptions]):
        raise ValueError("Missing required environment variables: TENANT_ID, CLIENT_ID, CLIENT_SECRET, or SUBSCRIPTIONS")

    try:
        credential = ClientSecretCredential(tenant_id, client_id, client_secret)
        sps_token = credential.get_token("https://management.azure.com/.default").token
    except ClientAuthenticationError as e:
        raise ValueError(f"Failed to authenticate with Azure: {e}")

    return sps_token, subscriptions