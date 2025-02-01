import io
import json
import boto3
import pickle
from threading import RLock
from const_config import AzureCollector, Storage

STORAGE_CONST = Storage()
AZURE_CONST = AzureCollector()

bad_request_retry_count = 0
found_invalid_instance_type_retry_count = 0
found_invalid_region_retry_count = 0
time_out_retry_count = 0
too_many_requests_count = 0
last_location_index = {}
available_locations = None
lock = RLock()
location_lock = RLock()

sps_token = None
invalid_regions_tmp = None
invalid_instance_types_tmp = None
locations_call_history_tmp = None
locations_over_limit_tmp = None
subscriptions = None

session = boto3.Session()
s3_client = session.client('s3')
s3_resource = session.resource('s3')

# Upload file to S3
def upload_file_to_s3(data, file_name, file_type="json", initialization=None):
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

        s3_client.upload_fileobj(file, STORAGE_CONST.BUCKET_NAME, AZURE_CONST.SPS_FILE_PATH + '/' + file_name)
        object_acl = s3_resource.ObjectAcl(STORAGE_CONST.BUCKET_NAME, AZURE_CONST.SPS_FILE_PATH + '/' + file_name)
        object_acl.put(ACL='public-read')

        if initialization:
            print(f"[S3]: Succeed to initialize. Filename: [{file_name}]")
        else:
            print(f"[S3]: Succeed to upload. Filename: [{file_name}]")

    except ValueError as ve:
        print(f"Validation error for {file_name}: {ve}")
    except Exception as e:
        print(f"Upload failed for {file_name}: {e}")


def read_file_from_s3(file_name, file_type="json"):
    try:
        response = s3_client.get_object(Bucket=STORAGE_CONST.BUCKET_NAME,Key=AZURE_CONST.SPS_FILE_PATH + '/' + file_name)
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
        print(f"Read failed for {file_name}: {e}")
        return None