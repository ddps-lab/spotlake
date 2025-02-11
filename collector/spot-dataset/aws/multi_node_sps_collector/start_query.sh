date=$(date -u '+%Y-%m-%dT%H:%M')

python3 /home/ubuntu/spotlake/collector/spot-dataset/aws/multi_node_sps_collector/main.py --timestamp $date
