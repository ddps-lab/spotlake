import json
import os
import urllib3

http = urllib3.PoolManager()

def lambda_handler(event, context):
    """
    Handle AWS Batch job failure events and send Slack notifications.
    Triggered by EventBridge when Batch job status changes to FAILED.
    """
    detail = event['detail']
    
    job_name = detail['jobName']
    job_id = detail['jobId']
    status = detail['status']
    status_reason = detail.get('statusReason', 'Unknown')
    container = detail.get('container', {})
    exit_code = container.get('exitCode', 'N/A')
    reason = container.get('reason', 'No reason provided')
    
    # Determine failure type based on exit code and reason
    failure_type = "Unknown Error"
    emoji = "🚨"
    
    if exit_code == 137 or "OutOfMemoryError" in str(status_reason) or "OutOfMemoryError" in str(reason):
        failure_type = "OOM (Out of Memory)"
        emoji = "💥"
    elif "timeout" in str(status_reason).lower() or "timeout" in str(reason).lower():
        failure_type = "Timeout"
        emoji = "⏰"
    elif exit_code and exit_code != 0 and exit_code != 'N/A':
        failure_type = f"Exit Code {exit_code}"
        emoji = "❌"
    
    # Extract collector type from job name (aws/azure, sps/if/price/merge)
    collector_type = "AWS" if "aws" in job_name.lower() else "Azure"
    
    # Build Slack message
    message = {
        "text": f"{emoji} *Batch Job Failed: {job_name}*",
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*{emoji} {collector_type} Batch Collector Failed*\n\n"
                            f"*Job Name:* `{job_name}`\n"
                            f"*Job ID:* `{job_id}`\n"
                            f"*Failure Type:* {failure_type}\n"
                            f"*Status:* {status}\n"
                            f"*Status Reason:* {status_reason}\n"
                            f"*Container Reason:* {reason}\n"
                            f"*Exit Code:* {exit_code}"
                }
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "View Job Details"},
                        "url": f"https://console.aws.amazon.com/batch/home?region=us-west-2#jobs/detail/{job_id}"
                    },
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "View CloudWatch Logs"},
                        "url": f"https://console.aws.amazon.com/cloudwatch/home?region=us-west-2#logsV2:log-groups/log-group/$252Faws$252Fbatch$252Fjob"
                    }
                ]
            }
        ]
    }
    
    # Send to Slack
    slack_webhook_url = os.environ['SLACK_WEBHOOK_URL']
    
    try:
        encoded_msg = json.dumps(message).encode('utf-8')
        resp = http.request(
            'POST',
            slack_webhook_url,
            body=encoded_msg,
            headers={'Content-Type': 'application/json'}
        )
        
        print(f"Slack notification sent. Status: {resp.status}")
        
        return {
            'statusCode': 200,
            'body': json.dumps(f'Notification sent for job {job_name}')
        }
    except Exception as e:
        print(f"Failed to send Slack notification: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
