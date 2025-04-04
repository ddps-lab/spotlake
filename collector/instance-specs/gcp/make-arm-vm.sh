#!/bin/bash

i=1
while read line || [ -n "$line" ] ; do

    machine_type=${line}

    gcloud compute instances create ${machine_type} \
    --image=ubuntu-2204-jammy-arm64-v20221206 \
    --image-project=ubuntu-os-cloud \
    --machine-type=${machine_type} \
    --zone=us-central1-a \
    --metadata-from-file=startup-script="./info-collector.sh" \
    --service-account feature-collector@gcp-hw-feature-collector.iam.gserviceaccount.com \
    --scopes=storage-full,compute-rw

    ((i+=1))
done < arm_machinetypes.txt
