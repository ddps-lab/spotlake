#lambda의 경우 requests 모듈은 layer로 준비필요
import requests
import inspect
import boto3
import os


def send_slack_message(msg):
    url = get_webhook_url()

    module_name = inspect.stack()[1][1]
    line_no = inspect.stack()[1][2]
    function_name = inspect.stack()[1][3]

    message = f"File \"{module_name}\", line {line_no}, in {function_name} :\n{msg}"

    slack_data = {
        "text": message
    }

    requests.post(url, json=slack_data)


def get_webhook_url():
    try:
        ssm = boto3.client('ssm')
        parameter = ssm.get_parameter(Name="error-notification-slack-webhook-url", WithDecryption=False)
        url = parameter['Parameter']['Value']
    except:
        url = os.environ.get('error-notification-slack-webhook-url')

    return url
