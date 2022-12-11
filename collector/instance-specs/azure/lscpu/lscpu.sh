#!/bin/bash

while read instance_type
do
	AZURE_SUBSCRIPTION_ID=********-****-****-****-************ AWS_ACCESS_KEY_ID=******************** AWS_SECRET_ACCESS_KEY=**************************************** AWS_REGION_NAME=us-west-2 python3 lscpu.py --zone=eastus --instance_type=$instance_type
done < list.txt