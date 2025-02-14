import os
import boto3
import requests
import inspect

def send_slack_block(msg):
    url = get_webhook_url()
    print(f"Slack Webhook URL : {url}")

    module_name = inspect.stack()[1][1]
    line_no = inspect.stack()[1][2]
    function_name = inspect.stack()[1][3]

    
    if(type(msg) == str):
        message = f"File \"{module_name}\", line {line_no}, in {function_name} :\n{msg}"
        slack_data = {"text": message}
    else:
        slack_data = msg
        
    post = requests.post(url, json=slack_data)
    print(f"slack request : {post.request}")
    print(f"Slack Response : {post.text}")

def get_webhook_url():
    try:
        ssm = boto3.client('ssm', region_name='us-west-2')
        parameter = ssm.get_parameter(Name="error_notification_slack_webhook_url", WithDecryption=False)
        url = parameter['Parameter']['Value']
    except Exception as e:
        print(f"Error retrieving Slack webhook URL: {e}")
        url = os.environ.get('error_notification_slack_webhook_url')
        url = "https://hooks.slack.com/services/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"

    return url
