#!/bin/sh

aws s3 sync s3://tmp-gcp/rawdata /home/ubuntu/gcp_rawdata
python3 gcp_preprocess_rawdata.py
aws s3 sync s3://spotlake/rawdata/gcp /home/ubuntu/gcp_newrawdata
python3 gcp_write_timestream.py