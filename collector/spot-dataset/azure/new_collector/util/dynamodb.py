import boto3


class DynamoDB:
    def __init__(self, table):
        dynamodb = boto3.resource('dynamodb')
        self.table = dynamodb.Table(table)

    def get_item(self, key):
        return self.table.get_item(Key={'id': key})['Item']['data']

    def get_all_items(self):
        datas = {}
        for i in self.table.scan()["Items"]:
            datas[i["id"]] = i["data"]

        return datas

    def put_item(self, id, data):
        self.table.put_item(Item={'id': id, 'data': data})
