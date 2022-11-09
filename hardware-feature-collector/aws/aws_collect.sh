#!/bin/sh

aws ec2 describe-instance-types --query InstanceTypes --output json > aws_instance_types.json
python3 ./feature_collector.py      #/tf/Hyeonyoung/aws_feature_collector/aws_feature_collector.py