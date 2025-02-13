#!/bin/bash
current_date=$(date -u '+%Y-%m-%dT%H:%M')
echo "Cron Job Executed at $current_date" > /home/ubuntu/cron_test.log

python3 /home/ubuntu/spotlake/collector/spot-dataset/aws/collector/server/sps/collect_sps.py --timestamp "$current_date" > /home/ubuntu/cron_output.log 2>&1