#!/bin/bash
export PYTHONPATH=/home/ubuntu/.local/lib/python3.11/site-packages
export AWS_CONFIG_FILE=/home/ubuntu/.aws/config
export AWS_SHARED_CREDENTIALS_FILE=/home/ubuntu/.aws/credentials

current_date=$(date -u '+%Y-%m-%dT%H:%M')
echo "Cron Job Executed at $current_date" > /home/ubuntu/cron_test.log

python3 /home/ubuntu/collect_sps.py --timestamp "$current_date" > /home/ubuntu/cron_output.log 2>&1
