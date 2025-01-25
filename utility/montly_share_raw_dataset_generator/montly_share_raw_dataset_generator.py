import boto3
import botocore
import requests
import traceback
from datetime import datetime, timedelta
import os
import shutil
import zipfile


S3_SPOTLAKE_BUCKET = "spotlake"
S3_SHARE_RAW_DATASET_BUCKET = "share-raw-dataset"
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

session = boto3.session.Session(region_name="us-west-2")
s3_client = session.client("s3")
s3_resource = session.resource("s3")

today_datetime = datetime.today()
end_datetime = today_datetime.replace(day=1) - timedelta(days=1)
start_datetime = end_datetime.replace(day=1)

vendors = ["aws", "azure", "gcp"]
is_vendors_complete = {vendor: False for vendor in vendors}


def send_slack_complete_msg(month_name):
    message = {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "emoji": True,
                    "text": "✅ Monthly SpotLake Raw Dataset has been successfully generated!",
                },
            },
            {
                "type": "context",
                "elements": [
                    {
                        "text": f"*{month_name}* Dataset @ s3://{S3_SHARE_RAW_DATASET_BUCKET}",
                        "type": "mrkdwn",
                    }
                ],
            },
            {"type": "divider"},
        ]
    }

    vendor_msg = {"type": "section", "fields": []}

    for vendor, complete in is_vendors_complete.items():
        if complete:
            vendor_msg["fields"].append(
                {"type": "mrkdwn", "text": f"`{vendor.upper()}` Dataset:\n🆗"}
            )

    message["blocks"].append(vendor_msg)

    requests.post(SLACK_WEBHOOK_URL, json=message)

def send_slack_err_msg(err_msg):
    message = {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "emoji": True,
                    "text": "⚠ Monthly SpotLake Raw Dataset Generator Error",
                },
            },
            {
                "type": "context",
                "elements": [
                    {
                        "text": f"*{month_name}* Dataset @ s3://{S3_SHARE_RAW_DATASET_BUCKET}",
                        "type": "mrkdwn",
                    }
                ],
            },
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"{err_msg}"
                }
		    },
        ]
    }

    requests.post(SLACK_WEBHOOK_URL, json=message)



try:
    for vendor in vendors:
        print(f"Download {vendor}...")
        current_datetime = start_datetime
        current_month = current_datetime.month

        while current_datetime <= end_datetime:
            month_name = (
                f"{current_datetime.year}-{str(current_datetime.month).zfill(2)}"
            )
            s3_key = f"{vendor}/{current_datetime.year}/{vendor}-{month_name}.zip"

            try:
                s3_head_obj = s3_client.head_object(Bucket=S3_SHARE_RAW_DATASET_BUCKET, Key=s3_key)
                print(f"{s3_key} already exists in the bucket")
                break
            except botocore.exceptions.ClientError as e:
                error_code = e.response["Error"]["Code"]
                # if monthly share-raw-dataset don't exists
                if error_code == "404":
                    # generate download folder
                    tmp_folder_path = f"./{vendor}-{month_name}"
                    if os.path.exists(tmp_folder_path):
                        shutil.rmtree(tmp_folder_path)
                    os.mkdir(tmp_folder_path)
                    print(f"{tmp_folder_path}.zip Download...")
                    # generate empty zipfile
                    zipf = zipfile.ZipFile(f"{tmp_folder_path}.zip", "w")
                    while (
                        current_datetime.month == current_month
                        and current_datetime <= end_datetime
                    ):
                        bucket = s3_resource.Bucket(S3_SPOTLAKE_BUCKET)
                        s3_spotlake_folder = f"rawdata/{vendor}/{datetime.strftime(current_datetime, '%Y/%m/%d')}"
                        os.mkdir(f"{tmp_folder_path}/{str(current_datetime.day).zfill(2)}")

                        for obj in bucket.objects.filter(Prefix=s3_spotlake_folder):
                            # s3 download from spotlake raw data to local
                            filename = f"{tmp_folder_path}/{str(current_datetime.day).zfill(2)}/{obj.key.split('/')[-1]}"
                            try:
                                bucket.download_file(obj.key, filename)
                                print(filename)
                            except:
                                print(f"[{filename} doesn't exist!]")
                            zipf.write(filename)
                        current_datetime = current_datetime + timedelta(days=1)
                    zipf.close()

                    shutil.rmtree(tmp_folder_path)
                    current_month = current_datetime.month
                    s3_client.upload_file(f"{tmp_folder_path}.zip", S3_SHARE_RAW_DATASET_BUCKET, s3_key)
                    os.remove(f"{tmp_folder_path}.zip")
                    is_vendors_complete[vendor] = True
                    print(f"Complete!")
                else:
                    raise
    # all complete
    send_slack_complete_msg(month_name)
except:
    err_traceback = traceback.format_exc()
    print(err_traceback)
    send_slack_err_msg(err_traceback)
