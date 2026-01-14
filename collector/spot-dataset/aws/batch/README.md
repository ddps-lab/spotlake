# AWS Batch Spot Dataset Collector

[English](README.md) | [Korean](README_kr.md)

This directory configures a system that collects, merges, and stores Spot Instance related data (Spot Price, Spot Placement Score, Spot Instance Frequency, etc.) using AWS Batch.

## System Architecture and Data Flow

Data collection and processing occurs in the following order:

1.  **Workload Generation**
    *   **Trigger**: Daily at 23:55 UTC (EventBridge Schedule)
    *   **Operation**: `generate_workload.py` runs to optimize (Bin Packing) the region and instance type combinations to be collected the next day, generating workload files and storing them in S3.
2.  **Data Collection**
    *   **Trigger**: Every 10 minutes (EventBridge Schedule)
    *   **SPS Collection**: `collect_sps.py` reads the generated workloads, calls the AWS SPS API, and stores results in S3 (`rawdata/aws/sps/`).
    *   **Spot IF Collection**: `collect_if.py` executes `spotinfo` to collect interruption frequency data and stores it in S3 (`rawdata/aws/spot_if/`).
    *   **Spot Price Collection**: `collect_price.py` queries Spot Prices for all regions and stores them in S3 (`rawdata/aws/spot_price/`).
3.  **Data Merge and Upload**
    *   **Trigger**: Automatically triggered by EventBridge Rule when SPS data files are uploaded to S3.
    *   **Operation**: `merge_data.py` runs to merge SPS, IF, Price, and On-Demand Price data into one. It compares with data from previous time points to extract only changed portions, then uploads them to Amazon Timestream and updates the latest state in S3.

## S3-Based System Integration and State Management

This system utilizes Amazon S3 not just as a data store, but as **State Management** and **Configuration Store**.

### 1. S3 Data Lake Structure
All collected raw data is systematically stored in S3.
*   `rawdata/aws/sps/{YYYY}/{MM}/{DD}/{HH}-{MM}_sps_{capacity}.pkl.gz`: SPS collection results
*   `rawdata/aws/spot_if/{YYYY}/{MM}/{DD}/{HH}-{MM}_spot_if.pkl.gz`: Spot IF collection results
*   `rawdata/aws/spot_price/{YYYY}/{MM}/{DD}/{HH}-{MM}_spot_price.pkl.gz`: Spot Price collection results
*   `rawdata/aws/workloads/{YYYY}/{MM}/{DD}/binpacked_workloads.pkl.gz`: Generated daily workloads

### 2. State Management
The AWS SPS API has strict query limits, so multiple AWS accounts (Credentials) must be rotated. `collect_sps.py` tracks the current state through metadata files stored in S3.

*   **Metadata File**: `rawdata/aws/localfile/sps_metadata.yaml`
*   **Managed Items**:
    *   `credential_index`: Index of the currently used AWS credential. Moves to the next index when query quota is exceeded.
    *   `target_capacity_index`: Currently collecting Target Capacity (e.g., 1, 5, 10, ... 50). Changes sequentially every 10-minute cycle.
    *   `workload_date`: Checks if the workload file date has changed to reset indices.

### 3. Configuration and Resources
*   **Credentials**: Multiple AWS account credentials stored in `credential/credential_3699.csv` are read from S3 and used for SPS queries.
*   **Latest State Maintenance**: The most recently merged data is stored in `latest_data/latest_aws.json` to be used as the baseline for calculating changes (Delta) during the next merge.

## System Implementation Details

This project's Terraform code creates AWS Batch resources, but treats some core infrastructure (VPC, S3, Timestream) as external dependencies.

### 1. VPC and Network (External)
*   **Configuration**: AWS Batch Compute Environment is placed within pre-existing VPC and Subnets.
*   **Variables**: `vpc_id`, `subnet_ids`, `security_group_ids` are injected through `variables.tf`.
*   **Requirements**: The assigned Security Group must allow Outbound (443) access for the ECS Agent to communicate with AWS services (Batch, S3, ECR, CloudWatch).

### 2. Amazon S3 (External)
*   **Configuration**: S3 buckets used as data layers and state stores are not created by Terraform; existing bucket names are received as variables (`s3_bucket`).
*   **Permissions**: The Batch Job Role (`batch_job_policy_spotlake`) is granted read/write permissions (`s3:GetObject`, `s3:PutObject`, `s3:ListBucket`) for the bucket.

### 3. Amazon Timestream (External)
*   **Configuration**: The final storage for time-series data (Spot Price, SPS, etc.).
*   **Permissions**: The Batch Job Role has permissions to write records to Timestream (`timestream:WriteRecords`). Table creation is managed outside this code scope.

### 4. ECS and AWS Batch (Managed)
*   **Compute Environment**: Defined as `aws_batch_compute_environment` resource in `infrastructure/main.tf`.
*   **Job Queue**: Defined as `aws_batch_job_queue` resource, connected to the Compute Environment to manage job priorities.
*   **Job Definitions**:
    *   `spotlake-collection-job`: Executes `run_collection.sh` script to perform SPS, IF, Price collection in parallel, then runs Merge job upon completion.
    *   `spotlake-workload-job`: Generates workloads daily.

## Infrastructure Configuration (Terraform)

The Terraform code in the `infrastructure/` directory creates the following AWS resources.

### AWS Batch Compute Environment and Job Queue
A single Compute Environment (`spotlake-compute-env`) and Job Queue (`spotlake-job-queue`) are used to efficiently manage resources.

*   **Type**: SPOT
*   **Allocation Strategy**: `SPOT_CAPACITY_OPTIMIZED`
*   **Instance Types**: `optimal`
*   **Max vCPUs**: 128

### IAM Roles
*   **`aws_batch_service_role_spotlake`**: Role for AWS Batch service to manage AWS resources.
*   **`ecs_task_execution_role_spotlake`**: Role for ECS agent to pull images and send logs.
*   **`batch_job_role_spotlake`**: Role used by the application running inside the actual container. Has permissions for S3 read/write, Timestream write, EC2 information query (SPS, Price, etc.).

### Job Definitions
Each job uses the same Docker image but has different `command` and resource allocations.

| Job Name | Script | vCPU | Memory | Trigger |
| :--- | :--- | :--- | :--- | :--- |
| `spotlake-collection-job` | `run_collection.sh` | 4.0 | 4096 MiB | Every 10 minutes |
| `spotlake-workload-job` | `generate_workload.py` | 2.0 | 4096 MiB | Daily at 23:55 UTC |

## Terraform Variables and Resources

This project's Terraform code builds the AWS Batch environment utilizing existing infrastructure (VPC, S3, etc.).

### 1. Input Variables

Variables received through the deployment script (`deploy_infra.sh`) are as follows.

| Variable Name (Flag) | Description | Example |
| :--- | :--- | :--- |
| `vpc_id` (`-v`) | ID of the existing VPC where AWS Batch Compute Environment will be created. | `vpc-12345678` |
| `subnet_ids` (`-s`) | List of subnet IDs where Batch Jobs will run. Input in JSON array format. | `["subnet-123", "subnet-456"]` |
| `security_group_ids` (`-g`) | List of security group IDs to apply to the Batch Compute Environment. Must allow outbound internet access. | `["sg-12345678"]` |
| `image_uri` (`-i`) | Docker image URI to use in Batch Job Definition. (ECR, etc.) | `1234.dkr.ecr.../repo:tag` |
| `aws_region` (`-r`) | AWS region to create resources in. (Default: `us-west-2`) | `us-east-1` |
| `s3_bucket` (`-b`) | Name of existing S3 bucket to use for data storage and state management. (Default: `spotlake`) | `my-spotlake-bucket` |
| `aws_profile` (`-p`) | AWS CLI profile to use when running scripts. If not specified, follows environment variable (`AWS_PROFILE`) or default settings (`default`). | `my-profile` |

### 2. Created Resources vs Existing Resources

The distinction between resources **newly created by Terraform** and **resources that must already exist** is as follows.

#### Newly Created Resources (Managed by Terraform)
*   **AWS Batch Compute Environment**: `spotlake-compute-env`
*   **AWS Batch Job Queue**: `spotlake-job-queue`
*   **AWS Batch Job Definitions**: `spotlake-collection-job`, `spotlake-workload-job`
*   **IAM Roles and Policies**:
    *   `aws_batch_service_role_spotlake`: Batch service role.
    *   `ecs_task_execution_role_spotlake`: ECS task execution role.
    *   `batch_job_role_spotlake`: Role for accessing S3, Timestream, etc. from inside the container.
    *   `ecs_instance_role_spotlake`: EC2 instance profile role.
*   **EventBridge Schedules and Rules**: Rules for periodic job execution and S3 event triggers.

#### Existing Resources (Must Already Exist)
*   **VPC and Subnets**: Network environment must be pre-configured.
*   **Security Group**: Security group with appropriate Outbound rules (port 443, etc.) is required.
*   **S3 Bucket**: Bucket for data storage must be pre-created, and its name is passed as a variable.
*   **ECR Repository**: Registry where Docker images will be pushed.
*   **Timestream Database and Table**: Tables for storing time-series data must be pre-created. (Batch Job only has write permissions)

## Docker Image

*   **Base Image**: `python:3.9-slim`
*   **Installed Packages**: `boto3`, `pandas`, `numpy`, `pyyaml`, `ortools`, `requests`, and `spotinfo` binary.
*   **Build and Deploy**: Build the image and push to ECR (`spotlake-batch`) using the `scripts/build_and_push.sh` script.
*   **Execution**: A single image is used for all Batch Jobs, with each Job Definition specifying the Python script to execute to differentiate behavior.

## Directory Details

### `if/`
*   **`collect_if.py`**: Internally executes the `spotinfo` CLI tool to parse data.

### `infrastructure/`
*   **`main.tf`**: Defines Batch environment, Job Queue, Job Definition, IAM Role.
*   **`events.tf`**: Defines EventBridge Scheduler (Cron) and CloudWatch Event Rule (S3 trigger).

### `merge/`
*   **`merge_data.py`**: Core logic for data merging. Uses `compare_data.py` for change detection.

### `price/`
*   **`collect_price.py`**: Calls `describe_spot_price_history` API using `boto3`.

### `sps/`
*   **`collect_sps.py`**: Uses `get_spot_placement_scores` API to query scores for large numbers of region/instance combinations.

### `workload/`
*   **`generate_workload.py`**: Uses Google OR-Tools Bin Packing algorithm to create workload allocations that improve API query efficiency.

## Deployment and Operations Guide

### 1. Prerequisites
*   AWS CLI installed and credentials configured (`aws configure`)
*   Docker running
*   Terraform installed
*   `jq` installed (may be required for script execution)

### 2. System Deployment

**Step 1: Build and Push Docker Image**
Execute the `scripts/build_and_push.sh` script to build the image and upload it to ECR.
```bash
# Run from project root directory
./collector/spot-dataset/aws/batch/scripts/build_and_push.sh [-r <aws_region>] [-p <aws_profile>]

# Example (using defaults: us-west-2, default profile)
./collector/spot-dataset/aws/batch/scripts/build_and_push.sh

# Example (using specific region and profile)
./collector/spot-dataset/aws/batch/scripts/build_and_push.sh -r us-east-1 -p my-profile
```
Copy the `Image URI` output upon success.

**Step 2: Deploy Infrastructure**
The `scripts/deploy_infra.sh` script uses Terraform to build the AWS Batch environment. Specify required arguments when running.

```bash
# Deployment script execution example
./collector/spot-dataset/aws/batch/scripts/deploy_infra.sh \
    -v "vpc-xxxxxxx" \
    -s '["subnet-xxxxxxx", "subnet-yyyyyyy"]' \
    -g '["sg-xxxxxxx"]' \
    -i "123456789012.dkr.ecr.us-west-2.amazonaws.com/spotlake-batch:latest" \
    -p "my-aws-profile" # (Optional)
```

### 3. Manual Execution
Use AWS CLI to run jobs immediately outside of EventBridge schedules.

```bash
# Manual execution of integrated collection job
aws batch submit-job \
    --job-name manual-collection \
    --job-queue spotlake-job-queue \
    --job-definition spotlake-collection-job

# Manual execution of workload generation job
aws batch submit-job \
    --job-name manual-workload-generation \
    --job-queue spotlake-job-queue \
    --job-definition spotlake-workload-job
```

### 4. System Removal
Use the Terraform destroy command to remove deployed resources.

```bash
cd collector/spot-dataset/aws/batch/infrastructure
terraform destroy
```
*Note: S3 buckets and Timestream tables are not managed by Terraform and will not be deleted.*
