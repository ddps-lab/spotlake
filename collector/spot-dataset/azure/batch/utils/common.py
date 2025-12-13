import boto3
import io
import json
import pickle
import logging
import pandas as pd
import yaml
from utils.constants import STORAGE_CONST

# Setup Clients
session = boto3.Session()
s3_client = session.client('s3', region_name='us-west-2')
s3_resource = session.resource('s3', region_name='us-west-2')
cw_client = session.client('logs', region_name='us-west-2')
timestream_write_client = session.client('timestream-write', region_name='us-west-2')

class S3Handler:
    def __init__(self):
        self.client = s3_client
        self.resource = s3_resource

    def upload_file(self, data, file_path, file_type="json", set_public_read=False):
        try:
            if file_type not in ['json', 'pkl', 'pkl.gz', 'df_to_csv.gz', 'yaml']:
                raise ValueError("Unsupported file type")

            if file_type == "json":
                class PandasJSONEncoder(json.JSONEncoder):
                    def default(self, obj):
                        if pd.isna(obj):
                            return None
                        return super().default(obj)
                file = io.BytesIO(json.dumps(data, cls=PandasJSONEncoder).encode("utf-8"))

            elif file_type == "pkl":
                file = io.BytesIO()
                pickle.dump(data, file)
                file.seek(0)

            elif file_type == "pkl.gz":
                file = io.BytesIO()
                data.to_pickle(file, compression="gzip")
                file.seek(0)
            
            elif file_type == "yaml":
                file = io.BytesIO(yaml.safe_dump(data).encode("utf-8"))

            self.client.upload_fileobj(file, STORAGE_CONST.BUCKET_NAME, file_path)

            if set_public_read:
                object_acl = self.resource.ObjectAcl(STORAGE_CONST.BUCKET_NAME, file_path)
                object_acl.put(ACL='public-read')

            print(f"[S3]: Succeed to upload. Filename: [{file_path}]")

        except Exception as e:
            print(f"Upload failed for {file_path}: {e}")

    def read_file(self, file_path, file_type="json"):
        try:
            response = self.client.get_object(Bucket=STORAGE_CONST.BUCKET_NAME, Key=file_path)
            file = io.BytesIO(response['Body'].read())

            if file_type == "json":
                return json.load(file)
            elif file_type == "pkl":
                return pd.read_pickle(file)
            elif file_type == "pkl.gz":
                return pd.read_pickle(file, compression="gzip")
            elif file_type == "yaml":
                return yaml.safe_load(file)
            else:
                raise ValueError("Unsupported file type")

        except Exception as e:
            print(f"Error reading {file_path} from S3: {e}")
            return None

class CWHandler:
    def __init__(self):
        self.client = cw_client

    def put_log_events(self, log_events, log_group_name, log_stream_name):
        try:
            self.client.put_log_events(
                logGroupName=log_group_name,
                logStreamName=log_stream_name,
                logEvents=log_events
            )
        except Exception as e:
            print(f"Error CWHandler put_log_events: {e}")
            # Don't raise, just log error to avoid stopping the entire process for logs
            pass

class TimestreamHandler:
    def __init__(self):
        self.client = timestream_write_client

    def write_records(self, records, common_attrs, database_name, table_name):
        try:
            self.client.write_records(
                Records=records,
                CommonAttributes=common_attrs,
                DatabaseName=database_name,
                TableName=table_name
            )
        except self.client.exceptions.RejectedRecordsException as err:
             # Raise to handle recursive retry in caller
             raise err
        except Exception as e:
            print(f"Error TimestreamHandler write_record: {e}")
            raise e

class LoggerConfig(logging.Logger):
    def __init__(self, level=logging.INFO, format_str='[%(levelname)s]: %(message)s'):
        super().__init__(__name__, level)
        if self.hasHandlers():
            self.handlers.clear()
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(format_str))
        self.addHandler(handler)

S3 = S3Handler()
Logger = LoggerConfig()
CW = CWHandler()
TimestreamWrite = TimestreamHandler()
