#!/bin/sh

FILE_NAME="aws_instance_types.json"
aws ec2 describe-instance-types --query InstanceTypes --output json > $FILE_NAME
python3 ./feature_collector.py $FILE_NAME
