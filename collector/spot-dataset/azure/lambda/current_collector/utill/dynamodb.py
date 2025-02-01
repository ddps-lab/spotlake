import boto3

class DynamoDB:
    def __init__(self, table):
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        self.table = dynamodb.Table(table)

    def get_item(self, key):
        return self.table.get_item(Key={'id': key})['Item']['data']

    def put_item(self, id, data):
        self.table.put_item(Item={'id': id, 'data': data})