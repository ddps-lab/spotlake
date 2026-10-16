"""Render a self-contained CloudFormation template; no container/artifact bucket required."""
import json
import sys
from pathlib import Path

build = Path(sys.argv[1])
build.mkdir(parents=True, exist_ok=True)
source = Path(__file__).with_name("lambda_function.py").read_text(encoding="utf-8")
src_bucket = "titans-spotlake-data"
src_prefixes = ["aws/", "parquet_warm/aws/", "parquet_cp_hot/aws/"]
dst_bucket = "spotlake-public-daily"
dst_prefix = "spotvista/aws"
function_name = "spotvista-daily-exporter"
# duckdb + numpy layer published in this account (see README "Dependencies layer").
# The AWS SDK for pandas managed layer cannot be used: its pyarrow lacks zstd, which
# TITANS parquet needs. Pinned; bump deliberately.
layer = {"Fn::Sub": "arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:layer:spotvista-exporter-deps:1"}

template = {
    "AWSTemplateFormatVersion": "2010-09-09",
    "Description": "SpotVista availability scores from TITANS change points",
    "Resources": {
        "LogGroup": {"Type": "AWS::Logs::LogGroup", "Properties": {
            "LogGroupName": "/aws/lambda/" + function_name, "RetentionInDays": 30}},
        "Role": {"Type": "AWS::IAM::Role", "Properties": {
            "AssumeRolePolicyDocument": {"Version": "2012-10-17", "Statement": [{
                "Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]},
            "Policies": [{"PolicyName": "ReadTitansWriteSpotvista", "PolicyDocument": {
                "Version": "2012-10-17", "Statement": [
                    {"Effect": "Allow", "Action": "s3:GetObject",
                     "Resource": [f"arn:aws:s3:::{src_bucket}/{p}*" for p in src_prefixes]},
                    {"Effect": "Allow", "Action": "s3:ListBucket", "Resource": f"arn:aws:s3:::{src_bucket}",
                     "Condition": {"StringLike": {"s3:prefix": [f"{p}*" for p in src_prefixes]}}},
                    {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject"],
                     "Resource": f"arn:aws:s3:::{dst_bucket}/{dst_prefix}/*"},
                    {"Effect": "Allow", "Action": "s3:ListBucket", "Resource": f"arn:aws:s3:::{dst_bucket}",
                     "Condition": {"StringLike": {"s3:prefix": f"{dst_prefix}/*"}}},
                    {"Effect": "Allow", "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
                     "Resource": {"Fn::GetAtt": ["LogGroup", "Arn"]}},
                ]}}]}},
        "Function": {"Type": "AWS::Lambda::Function", "Properties": {
            "FunctionName": function_name, "Runtime": "python3.13", "Handler": "index.lambda_handler",
            "Architectures": ["arm64"], "MemorySize": 6144, "Timeout": 900, "Layers": [layer],
            # /tmp holds the month's parquet files (largest ~60 MB) and DuckDB spill space.
            "EphemeralStorage": {"Size": 2048},
            "ReservedConcurrentExecutions": 1, "Role": {"Fn::GetAtt": ["Role", "Arn"]},
            "Environment": {"Variables": {"SRC_BUCKET": src_bucket, "DST_BUCKET": dst_bucket,
                                           "DST_PREFIX": dst_prefix}},
            "Code": {"ZipFile": source}}},
        "RulePermission": {"Type": "AWS::Lambda::Permission", "Properties": {
            "FunctionName": {"Ref": "Function"}, "Action": "lambda:InvokeFunction",
            "Principal": "events.amazonaws.com", "SourceArn": {"Fn::GetAtt": ["DailyRule", "Arn"]},
            "SourceAccount": {"Ref": "AWS::AccountId"}}},
        # 01:00 UTC: TITANS has merged yesterday's 23:50 tick, and last month's
        # cold file (written ~00:14 on the 1st) exists.
        "DailyRule": {"Type": "AWS::Events::Rule", "Properties": {
            "Name": function_name, "Description": "SpotVista availability scores: every day 01:00 UTC",
            "ScheduleExpression": "cron(0 1 * * ? *)", "State": "ENABLED",
            "Targets": [{"Id": "Exporter", "Arn": {"Fn::GetAtt": ["Function", "Arn"]}, "Input": "{}",
                         "RetryPolicy": {"MaximumRetryAttempts": 2, "MaximumEventAgeInSeconds": 21600}}]}},
        "AsyncRetries": {"Type": "AWS::Lambda::EventInvokeConfig", "Properties": {
            "FunctionName": {"Ref": "Function"}, "Qualifier": "$LATEST", "MaximumRetryAttempts": 2,
            "MaximumEventAgeInSeconds": 21600}},
    },
    "Outputs": {
        "FunctionName": {"Value": {"Ref": "Function"}},
        "IndexUrl": {"Value": f"https://d2krkjqajp4l0e.cloudfront.net/{dst_prefix}/index.json"},
    },
}
encoded = json.dumps(template, ensure_ascii=False, indent=2)
if len(encoded.encode()) > 51_200:
    raise ValueError("Template exceeds CloudFormation's direct-upload limit")
(build / "template.json").write_text(encoded, encoding="utf-8")
print(f"Rendered {len(encoded.encode())} byte CloudFormation template")
