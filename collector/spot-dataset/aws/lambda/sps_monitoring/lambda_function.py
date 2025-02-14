from datetime import datetime, timedelta, timezone
import gzip
import json
import os
import pickle
import pandas as pd
import boto3

from send_slack_block import send_slack_block


def main():
    # ------ Set Constants ------
    TARGET_DATE = datetime.now(timezone.utc) - timedelta(days=1)
    BUCKET_NAME = "spotlake"
    BUCKET_FILE_PATH = "rawdata/aws"
    S3_DIR_NAME = TARGET_DATE.strftime('%Y/%m/%d')
    SPS_FILE_PREFIX = f"{BUCKET_FILE_PATH}/sps/{S3_DIR_NAME}"
    
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=SPS_FILE_PREFIX)
    
    sps_df = pd.DataFrame()
    if 'Contents' in response:
        for obj in response['Contents']:
            print(obj['Key'])
            if obj['Key'].endswith('.pkl.gz'):
                s3_obj = s3.get_object(Bucket=BUCKET_NAME, Key=obj['Key'])
                unzip_obj = gzip.decompress(s3_obj['Body'].read())
                sps_df = pd.concat([sps_df, pd.DataFrame(pickle.loads(unzip_obj))], ignore_index=True)
    else:
        raise Exception("No SPS files found")

    target_capacities = [1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50]
    count_target_capacities = { capacity: 0 for capacity in target_capacities }

    if not sps_df.empty:
        grouped = sps_df.groupby('TargetCapacity').size()
        for capacity in target_capacities:
            count = grouped.get(capacity, 0)
            count_target_capacities[capacity] = count
    total_number = sps_df[sps_df['SPS'] > 0].shape[0]

    slack_message = generate_slack_message(count_target_capacities, total_number)
    send_slack_block(slack_message)

def generate_slack_message(count_target_capacities, total_number):
    target_capacities_message_blocks = []
    for capacity, count in count_target_capacities.items():
        target_capacities_message_blocks.append({
          "type": "rich_text_section",
          "elements": [
            {
              "type": "text",
              "text": f"Target Capacity {capacity:02d}: ",
              "style": {"bold": True}
            },
            {
              "type": "text",
              "text": f"{count}"
            }
          ]
        })

    message = {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{datetime.today().date()} spotlake_sps_monitoring - AWS",
                    "emoji": True
                }
            },
            {
                "type": "divider"
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Collected SPS data per Target Capacity*"
                }
            },
            {
                "type": "rich_text",
                "elements": [{
					"type": "rich_text_list",
					"style": "bullet",
					"indent": 0,
                    "elements": target_capacities_message_blocks
                }]
            },
            {
                "type": "divider"
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Total number of SPS data collected*\n{total_number}"
                }
            }
        ]
    }

    return message

    

def lambda_handler(event, context):
    try:
        main()
    except Exception as e:
        send_slack_block(f"Error in sps_monitoring: {str(e)}")
        raise

if __name__ == '__main__':
    main()