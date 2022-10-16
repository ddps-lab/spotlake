date=$(date '+%Y-%m-%dT%H:%M')

python3 /home/ubuntu/spotlake/collection/aws/ec2_collector/aws_collect.py --timestamp $date &
# python3 /home/ubuntu/spotlake/collection/azure/collector/azure_collect.py --timestamp $date &
# python3 /home/ubuntu/spotlake/collection/gcp/collector/gcp_collect.py --timestamp $date &
