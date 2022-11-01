import boto3
import json


class S3:
    def __init__(self, name):
        s3 = boto3.resource("s3")
        self.bucket = s3.Bucket(name)

    def get_json(self, key):
        return json.loads(self.bucket.Object(key=key).get()["Body"].read().decode("utf-8"))

    def put_json(self, key, data):
        self.bucket.Object(key=key).put(Body=json.dumps(data))
