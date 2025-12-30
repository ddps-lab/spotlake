import requests
import os
import inspect
import boto3

session = boto3.Session()
ssm_client = session.client('ssm', region_name='us-west-2')

class SsmHandler:
    def __init__(self):
        self.client = ssm_client

    def get_parameter(self, name, with_decryption=False):
        try:
            response = self.client.get_parameter(Name=name, WithDecryption=with_decryption)
            return response['Parameter']['Value']
        except Exception as e:
            print(f"Error retrieving parameter {name}: {e}")
            return None

SSM = SsmHandler()

def send_slack_message(msg):
    url_key = 'error_notification_slack_webhook_url'
    url = SSM.get_parameter(url_key, with_decryption=False)

    if not url:
        print(f"Slack webhook URL not found in SSM. Message: {msg}")
        return

    stack = inspect.stack()
    try:
        module_name = stack[1][1]
        line_no = stack[1][2]
        function_name = stack[1][3]
        message = f"File \"{module_name}\", line {line_no}, in {function_name} :\n{msg}"
    except IndexError:
        message = f"Unknown Source:\n{msg}"

    slack_data = {
        "text": message
    }
    try:
        requests.post(url, json=slack_data)
    except Exception as e:
        print(f"Failed to send slack message: {e}")
