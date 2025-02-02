import boto3
import io
import json
import pickle
from const_config import AzureCollector, Storage

STORAGE_CONST = Storage()
AZURE_CONST = AzureCollector()

session = boto3.Session()
dynamodb = session.resource('dynamodb', region_name='us-east-1')
s3_client = session.client('s3', region_name='us-west-2')
s3_resource = session.resource('s3', region_name='us-west-2')

class DynamoDB:
    def __init__(self, table):
        self.table = dynamodb.Table(table)

    def get_item(self, key):
        return self.table.get_item(Key={'id': key})['Item']['data']

    def put_item(self, id, data):
        self.table.put_item(Item={'id': id, 'data': data})


class S3Handler:
    def __init__(self):
        self.client = s3_client
        self.resource = s3_resource

    def upload_file(self, data, file_name, file_type="json", initialization=None):
        try:
            if file_type not in ["json", "pkl"]:
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

            file_path = f"{AZURE_CONST.SPS_FILE_PATH}/{file_name}"
            self.client.upload_fileobj(file, STORAGE_CONST.BUCKET_NAME, file_path)

            object_acl = self.resource.ObjectAcl(STORAGE_CONST.BUCKET_NAME, file_path)
            object_acl.put(ACL='public-read')

            if initialization:
                print(f"[S3]: Succeed to initialize. Filename: [{file_name}]")
            else:
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
