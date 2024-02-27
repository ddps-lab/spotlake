import boto3
import os
import sys
from datetime import datetime

sys.path.append("/home/ubuntu/spotlake/utility")
from slack_msg_sender import send_slack_message

# LOG_GROUP_NAME = "SPS-Collected-Data"
# LOG_STREAM_NAME_AMOUNT_DATA = "amount-data"
# LOG_STREAM_NAME_EXECUTION_TIME = "execution-time"
# LOG_STREAM_NAME_EXTRA = "extra-log"

def calculate_execution_ms(start_time, end_time):
    return int((end_time - start_time) * 1000)

def upload_data_to_s3(s3_client, saved_filename, s3_dir_name, s3_obj_name, bucket_name):
    with open(saved_filename, 'rb') as f:
        s3_client.upload_fileobj(f, bucket_name, f"aws/{s3_dir_name}/{s3_obj_name}")
    
    os.remove(saved_filename)
    
# def upload_log_event(log_client, log_group_name, log_stream_name, message):
#     timestamp = int(datetime.utcnow().timestamp() * 1000)
#     try:
#         response = log_client.put_log_events(
#             logGroupName=log_group_name,
#             logStreamName=log_stream_name,
#             logEvents=[
#                 {
#                     'timestamp': timestamp,
#                     'message': str(message)
#                 },
#             ],
#         )
#     except Exception as e:
#         raise e
#     return response

# def log_execution_time(client, start_time, end_time, message_key):
#     execution_time = calculate_execution_ms(start_time, end_time)
#     try:
#         response = upload_log_event(client, LOG_GROUP_NAME, LOG_STREAM_NAME_EXECUTION_TIME, f"{message_key} {execution_time}")
#     except Exception as e:
#         raise e
#     return response

# def log_amount(client, amount_data):
#     message = f"DATA_ROWS {amount_data}"
#     try:
#         response = upload_log_event(client, LOG_GROUP_NAME, LOG_STREAM_NAME_AMOUNT_DATA, message)
#     except Exception as e:
#         raise e
#     return response