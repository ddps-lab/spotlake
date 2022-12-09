import boto3
import json


def get_token():
    lambda_client = boto3.client('lambda')
    token = lambda_client.invoke(
        FunctionName='AzureAuth', InvocationType='RequestResponse')["Payload"].read()
    return json.loads(token.decode("utf-8"))
