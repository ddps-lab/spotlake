date=$(date '+%Y-%m-%d %H:%M')

python3 ./aws/ec2_collector/aws_collect.py --timestamp $date &
# python3 ./azure/collector/azure_collect_server.py --timestamp $date &
# python3 ./gcp/collector/gcp_collect.py --timestamp $date &
