date=$(date '+%Y-%m-%dT%H:%M')

python3 /home/ubuntu/spot-score/collection/aws/ec2_collector/aws_collect.py --timestamp $date &
# python3 /home/ubuntu/spot-score/collection/azure/collector/azure_collect.py --timestamp $date &
# python3 /home/ubuntu/spot-score/collection/gcp/collector/gcp_collect.py --timestamp $date &
