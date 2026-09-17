"""Render a self-contained CloudFormation template; no container/artifact bucket required."""
import json
import sys
from pathlib import Path

build = Path(sys.argv[1])
source = Path(__file__).with_name("lambda_function.py").read_text()
lab = json.loads((build / "lab-papers.json").read_text())
bucket = "spotlake-public-daily"
key = "citations/citations.json"
function_name = "spotlake-monthly-citations"
template = {
    "AWSTemplateFormatVersion": "2010-09-09",
    "Description": "Monthly SpotLake citation snapshot from Semantic Scholar and Crossref",
    "Resources": {
        "LogGroup": {"Type": "AWS::Logs::LogGroup", "Properties": {
            "LogGroupName": "/aws/lambda/" + function_name, "RetentionInDays": 30}},
        "Role": {"Type": "AWS::IAM::Role", "Properties": {
            "AssumeRolePolicyDocument": {"Version": "2012-10-17", "Statement": [{
                "Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]},
            "Policies": [{"PolicyName": "CitationSnapshotOnly", "PolicyDocument": {
                "Version": "2012-10-17", "Statement": [
                    {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject"],
                     "Resource": f"arn:aws:s3:::{bucket}/{key}"},
                    {"Effect": "Allow", "Action": ["logs:CreateLogStream", "logs:PutLogEvents"],
                     "Resource": {"Fn::GetAtt": ["LogGroup", "Arn"]}},
                ]}}]}},
        "Function": {"Type": "AWS::Lambda::Function", "Properties": {
            "FunctionName": function_name, "Runtime": "python3.13", "Handler": "index.lambda_handler",
            "Architectures": ["arm64"], "MemorySize": 256, "Timeout": 300,
            "ReservedConcurrentExecutions": 1, "Role": {"Fn::GetAtt": ["Role", "Arn"]},
            "Environment": {"Variables": {"CITATIONS_BUCKET": bucket, "CITATIONS_KEY": key,
                                           "LAB_PAPERS_JSON": json.dumps(lab, ensure_ascii=False)}},
            "Code": {"ZipFile": source}}},
        "SchedulerRole": {"Type": "AWS::IAM::Role", "Properties": {
            "AssumeRolePolicyDocument": {"Version": "2012-10-17", "Statement": [{
                "Effect": "Allow", "Principal": {"Service": "scheduler.amazonaws.com"},
                "Action": "sts:AssumeRole", "Condition": {"StringEquals": {
                    "aws:SourceAccount": {"Ref": "AWS::AccountId"},
                    "aws:SourceArn": {"Fn::Sub": "arn:${AWS::Partition}:scheduler:${AWS::Region}:${AWS::AccountId}:schedule-group/default"}}}}]},
            "Policies": [{"PolicyName": "InvokeCitationUpdaterOnly", "PolicyDocument": {
                "Version": "2012-10-17", "Statement": [{"Effect": "Allow",
                    "Action": "lambda:InvokeFunction", "Resource": {"Fn::GetAtt": ["Function", "Arn"]}}]}}]}},
        "MonthlySchedule": {"Type": "AWS::Scheduler::Schedule", "Properties": {
            "Name": "spotlake-monthly-citations-cron", "GroupName": "default",
            "Description": "First day of every month, 04:00 UTC / 13:00 Asia-Seoul, aligned with Drive upload",
            "ScheduleExpression": "cron(0 4 1 * ? *)", "ScheduleExpressionTimezone": "UTC",
            "FlexibleTimeWindow": {"Mode": "OFF"}, "State": "ENABLED",
            "Target": {"Arn": {"Fn::GetAtt": ["Function", "Arn"]},
                       "RoleArn": {"Fn::GetAtt": ["SchedulerRole", "Arn"]}, "Input": "{}",
                       "RetryPolicy": {"MaximumRetryAttempts": 2, "MaximumEventAgeInSeconds": 21600}}}},
        "AsyncRetries": {"Type": "AWS::Lambda::EventInvokeConfig", "Properties": {
            "FunctionName": {"Ref": "Function"}, "Qualifier": "$LATEST", "MaximumRetryAttempts": 2,
            "MaximumEventAgeInSeconds": 21600}},
    },
    "Outputs": {
        "FunctionName": {"Value": {"Ref": "Function"}},
        "ScheduleName": {"Value": {"Ref": "MonthlySchedule"}},
        "SnapshotUrl": {"Value": "https://d2krkjqajp4l0e.cloudfront.net/" + key},
    },
}
encoded = json.dumps(template, ensure_ascii=False, indent=2)
if len(encoded.encode()) > 51_200:
    raise ValueError("Template exceeds CloudFormation's direct-upload limit")
if len(json.dumps(template["Resources"]["Function"]["Properties"]["Environment"]["Variables"]).encode()) > 4000:
    raise ValueError("Lambda environment is too large; move configuration into the package")
(build / "template.json").write_text(encoded)
print(f"Rendered {len(encoded.encode())} byte CloudFormation template")
