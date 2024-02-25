import boto3
import os
import sys
from datetime import datetime

sys.path.append("/home/ubuntu/spotlake/utility")
from slack_msg_sender import send_slack_message

def calculate_execution_ms(start_time, end_time):
    return int((end_time - start_time) * 1000)

def upload_data_to_s3(saved_filename, s3_dir_name, s3_obj_name, bucket_name):
    session = boto3.Session()
    s3 = session.client('s3')

    with open(saved_filename, 'rb') as f:
        s3.upload_fileobj(f, bucket_name, f"aws/{s3_dir_name}/{s3_obj_name}")
    
    os.remove(saved_filename)
    
def upload_log_event(client, log_group_name, log_stream_name, log_event_key, log_event_value):
    message = {log_event_key : log_event_value}
    timestamp = int(datetime.utcnow().timestamp() * 1000)
    try:
        response = client.put_log_events(
            logGroupName=log_group_name,
            logStreamName=log_stream_name,
            logEvents=[
                {
                    'timestamp': timestamp,
                    'message': str(message)
                },
            ],
        )
    except Exception as e:
        raise e
    return response

def log_execution_time(client, start_time, end_time, log_group_name, log_stream_name, log_event_key):
    execution_time = calculate_execution_ms(start_time, end_time)
    try:
        response = upload_log_event(client, log_group_name, log_stream_name, log_event_key, execution_time)
    except Exception as e:
        raise e
    return response