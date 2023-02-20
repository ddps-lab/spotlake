#!/bin/bash

MACHINE_TYPE=`curl -s "http://metadata.google.internal/computeMetadata/v1/instance/name" -H "Metadata-Flavor: Google"`

# lscpu. Have to make google storage bucket 'gcp-cpuinfo'
CPU_PLATFORM=`curl -s "http://metadata.google.internal/computeMetadata/v1/instance/cpu-platform" -H "Metadata-Flavor: Google"`
sudo lscpu > /home/kmubigdatagcp/${MACHINE_TYPE}.txt
echo CPU Platform : ${CPU_PLATFORM} >> /home/kmubigdatagcp/${MACHINE_TYPE}.txt
gcloud storage cp /home/kmubigdatagcp/${MACHINE_TYPE}.txt gs://gcp-cpuinfo/

# dmidecode. Have to make google storage bucket 'gcp-meminfo'
sudo dmidecode -t memory > /home/kmubigdatagcp/${MACHINE_TYPE}.txt
gcloud storage cp /home/kmubigdatagcp/${MACHINE_TYPE}.txt gs://gcp-meminfo/

gcloud compute instances delete ${MACHINE_TYPE} --zone=us-central1-a --quiet
