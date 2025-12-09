import boto3
import os
import sys
import json
from datetime import datetime, timezone

sys.path.append("/home/ubuntu/spotlake/utility")
from slack_msg_sender import send_slack_message

def calculate_execution_ms(start_time, end_time):
    return int((end_time - start_time) * 1000)

def upload_data_to_s3(s3_client, saved_filename, s3_dir_name, s3_obj_name, bucket_name):
    with open(saved_filename, 'rb') as f:
        s3_client.upload_fileobj(f, bucket_name, f"aws/{s3_dir_name}/{s3_obj_name}")
    
    os.remove(saved_filename)

def upload_log_event(log_client, log_group_name, log_stream_name, log_event_key, log_event_value):
    message = json.dumps({log_event_key : log_event_value})
    timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)
    try:
        response = log_client.put_log_events(
            logGroupName=log_group_name,
            logStreamName=log_stream_name,
            logEvents=[
                {
                    'timestamp': timestamp,
                    'message': message
                },
            ],
        )
    except Exception as e:
        raise e
    return response