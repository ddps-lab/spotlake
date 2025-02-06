import boto3
import io
import json
import pickle
import requests
import inspect
import os
from const_config import AzureCollector, Storage

STORAGE_CONST = Storage()
AZURE_CONST = AzureCollector()

session = boto3.Session()
dynamodb = session.resource('dynamodb', region_name='us-east-1')
s3_client = session.client('s3', region_name='us-west-2')
s3_resource = session.resource('s3', region_name='us-west-2')
ssm_client = session.client('ssm', region_name='us-west-2')

class DynamoDB:
    def __init__(self, table):
        self.table = dynamodb.Table(table)

    def get_item(self, key):
        return self.table.get_item(Key={'id': key})['Item']['data']

    def put_item(self, id, data):
        self.table.put_item(Item={'id': id, 'data': data})

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

class S3Handler:
    def __init__(self):
        self.client = s3_client
        self.resource = s3_resource

    def upload_file(self, data, file_name, file_type="json", set_public_read = False):
        try:
            if file_type not in ["json", "pkl", "df_to_csv.gz"]:
                raise ValueError("Unsupported file type. Use 'json' or 'pkl'.")

            if file_type == "json":
                if not isinstance(data, (dict, list)):
                    raise ValueError("JSON file must be a dictionary or a list")
                file = io.BytesIO(json.dumps(data, indent=4).encode("utf-8"))

            elif file_type == "pkl":
                if data is None:
                    raise ValueError("Data cannot be None for PKL file")
                file = io.BytesIO()
                pickle.dump(data, file)
                file.seek(0)

            elif file_type == "df_to_csv.gz":
                if data is None:
                    raise ValueError("Data cannot be None for csv.gz file")
                file = io.BytesIO()
                data.to_csv(file, index=False, compression="gzip")
                file.seek(0)

            file_path = f"{AZURE_CONST.SPS_FILE_PATH}/{file_name}"
            self.client.upload_fileobj(file, STORAGE_CONST.BUCKET_NAME, file_path)

            if set_public_read:
                object_acl = self.resource.ObjectAcl(STORAGE_CONST.BUCKET_NAME, file_path)
                object_acl.put(ACL='public-read')

            print(f"[S3]: Succeed to upload. Filename: [{file_name}]")

        except ValueError as ve:
            print(f"Validation error for {file_name}: {ve}")
        except Exception as e:
            print(f"Upload failed for {file_name}: {e}")

    def read_file(self, file_name, file_type="json"):
        try:
            file_path = f"{AZURE_CONST.SPS_FILE_PATH}/{file_name}"
            response = self.client.get_object(Bucket=STORAGE_CONST.BUCKET_NAME, Key=file_path)
            file = io.BytesIO(response['Body'].read())

            if file_type == "json":
                return json.load(file)

            elif file_type == "pkl":
                return pickle.load(file)

            else:
                raise ValueError("Unsupported file type. Use 'json' or 'pkl'.")

        except json.JSONDecodeError:
            print(f"Warning: {file_name} is not a valid JSON file.")
            return None
        except Exception as e:
            print(f"Error reading {file_name} from S3: {e}")
            return None

db_AzureAuth = DynamoDB("AzureAuth")
SSM = SsmHandler()
S3 = S3Handler()

def send_slack_message(msg):
    url_key = 'error_notification_slack_webhook_url'
    url = SSM.get_parameter(url_key, with_decryption=False)

    if not url:
        url = os.environ.get(url_key)

    stack = inspect.stack()
    module_name = stack[1][1]
    line_no = stack[1][2]
    function_name = stack[1][3]

    message = f"File \"{module_name}\", line {line_no}, in {function_name} :\n{msg}"
    slack_data = {
        "text": message
    }
    requests.post(url, json=slack_data)