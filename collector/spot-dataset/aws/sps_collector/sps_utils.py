import boto3
import os
import sys
from datetime import datetime

sys.path.append("/home/ubuntu/spotlake/utility")
from slack_msg_sender import send_slack_message

def calculate_execution_ms(start_time, end_time):
    return int((end_time - start_time) * 1000)

def upload_data_to_s3(s3_client, saved_filename, s3_dir_name, s3_obj_name, bucket_name):
    with open(saved_filename, 'rb') as f:
        s3_client.upload_fileobj(f, bucket_name, f"aws/{s3_dir_name}/{s3_obj_name}")
    
    os.remove(saved_filename)
