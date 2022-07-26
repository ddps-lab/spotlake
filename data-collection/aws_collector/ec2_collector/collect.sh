date=$(date '+%Y-%m-%d %H:%M')

python3 aws_collect.py --timestamp $date &
# python3 azure_collect.py --timestamp $date &
# python3 gcp_collect.py --timestamp $date &