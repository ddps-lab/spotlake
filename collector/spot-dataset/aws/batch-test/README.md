# AWS Batch Spot Dataset Collector (TEST ENVIRONMENT)

[English](README.md) | [Korean](README_kr.md)

> [!WARNING]
> **This directory is a test environment.**
> While the code is similar to the Production environment (`collector/spot-dataset/aws/batch`), the data storage is separated.

This directory is an environment for testing Spot Instance related data collection using AWS Batch.
It reads Production data to perform logic, but stores results in **Test S3 bucket** and **Test Timestream table**.

## Data Flow and Storage Configuration

This test environment follows a **Read-Only Production / Write-Only Test** strategy.

### 1. Read (Source)
The following data is read from **Production S3 (`spotlake`)**:
*   **Workloads**: `rawdata/aws/workloads/...` (Uses actual generated workloads)
*   **Credentials**: `credential/credential_3699.csv` (Uses actual credentials)
*   **Spot IF**: `rawdata/aws/spot_if/...` (Uses data collected from Production)
*   **Spot Price**: `rawdata/aws/spot_price/...` (Uses data collected from Production)
*   **On-Demand Price**: `rawdata/aws/ondemand_price/...` (Uses data collected from Production)

### 2. Write (Destination)
The following data is stored in **Test S3 (`spotlake-test`)** and **Test Timestream**:
*   **SPS Data**: `rawdata/aws/sps/...` (Newly collected SPS data)
*   **Merged Data**: `latest_data/latest_aws.json` and `rawdata/aws/merged/...` (Merged results)
*   **Metadata**: `rawdata/aws/localfile/sps_metadata.yaml` (Independent state management for test environment)
*   **Timestream**: Database `spotlake-test`, Table `aws-test`

## Key Changes (vs Production)

### `sps/collect_sps.py`
*   `READ_BUCKET_NAME`: `spotlake` (Production)
*   `WRITE_BUCKET_NAME`: `spotlake-test` (Test)
*   SPS collection results and metadata (indices, etc.) are stored in `spotlake-test`.

### `merge/merge_data.py` and `upload_data.py`
*   `READ_BUCKET_NAME`: `spotlake` (Production)
*   `WRITE_BUCKET_NAME`: `spotlake-test` (Test)
*   Merges Production's Price/IF data with Test's SPS data.
*   Timestream upload target: `spotlake-test` DB / `aws-test` Table.

### `price/collect_price.py` and `if/collect_if.py`
*   `BUCKET_NAME`: `spotlake-test`
*   When running for test purposes, collected data is stored in `spotlake-test`.

## Deployment and Execution

### Docker Image Build
You need to build the image based on the `batch-test` directory.

**Linux/Mac**
```bash
./collector/spot-dataset/aws/batch-test/scripts/build_and_push.sh
```

**Windows (PowerShell)**
```powershell
./collector/spot-dataset/aws/batch-test/scripts/build_and_push.ps1
```

### Infrastructure Deployment
You can deploy test infrastructure separately, or change only the Job Definition in an existing Batch environment for testing.
This test environment has `_test` or `-test` suffixes on all resources (Role, Queue, Compute Environment, etc.) to avoid name conflicts with Production.
Also, like Production, it uses an **integrated Compute Environment and Job Queue** to perform collection and merge operations through `run_collection.sh`.

**Linux/Mac**
```bash
./collector/spot-dataset/aws/batch-test/scripts/deploy_infra.sh \
    -v "vpc-xxxxxxx" \
    -s '["subnet-xxxxxxx", "subnet-yyyyyyy"]' \
    -g '["sg-xxxxxxx"]' \
    -i "123456789012.dkr.ecr.us-west-2.amazonaws.com/spotlake-batch-test:latest"
```

**Windows (PowerShell)**
```powershell
./collector/spot-dataset/aws/batch-test/scripts/deploy_infra.ps1 `
    -VpcId "vpc-xxxxxxx" `
    -SubnetIds '["subnet-xxxxxxx", "subnet-yyyyyyy"]' `
    -SecurityGroupIds '["sg-xxxxxxx"]' `
    -ImageUri "123456789012.dkr.ecr.us-west-2.amazonaws.com/spotlake-batch-test:latest"
```
