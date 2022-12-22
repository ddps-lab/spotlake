date=$(date -d "+1 days" '+%Y-%m-%dT%H:%M')

python3 /home/ubuntu/spotlake/collector/spot-dataset/aws/ec2_collector/workload_binpacking.py --timestamp $date &

