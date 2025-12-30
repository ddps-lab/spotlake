# Azure SpotLake Collector (Batch Implementation)

This directory contains the Azure Spot Instance data collector implementation for AWS Batch (conceptually Azure Batch, but utilizing similar containerized architecture). It is designed to collect Spot Placement Scores (SPS), Spot Prices, and Instance Family (IF) data, merge them, and upload the results to data stores (Timestream, S3).

## 📂 Directory Structure

```graphql
azure/batch/
├── if/
│   └── collect_if.py           # Collects Instance Family data via Azure Resource Graph
├── price/
│   └── collect_price.py        # Collects Spot/On-Demand Prices via Azure Retail Prices API
├── sps/
│   ├── collect_sps.py          # Main entry point for SPS collection (Trigger & Metadata Mgmt)
│   └── load_sps.py             # Core logic for querying SPS API & greedy clustering
├── merge/
│   ├── merge_data.py           # Merges collected data (SPS + Price + IF), calculates T2/T3
│   ├── compare_data.py         # Logic for detecting changes vs previous state
│   └── upload_data.py          # Handles uploads to TimeStream, S3, CloudWatch
├── sps_module/                   # Legacy logic ported for SPS parameter preparation
│   ├── sps_location_manager.py
│   ├── sps_prepare_parameters.py
│   └── sps_shared_resources.py
├── utils/
│   ├── common.py               # Shared handlers (S3, Logger, Timestream, CloudWatch)
│   ├── constants.py            # Centralized constants & S3 paths
│   ├── azure_auth.py           # Azure authentication & credential management
│   └── slack_msg_sender.py     # Slack notification utility
├── infrastructure/               # Terraform IaC
│   ├── main.tf                 # Defines Batch Compute Env, Job Queue, Job Def, IAM Roles
│   ├── variables.tf            # Input variables (VPC, Subnet, Image URI, etc.)
│   └── events.tf               # EventBridge Scheduler definition (CRON trigger)
├── scripts/
│   ├── run_collection.sh       # Orchestration script for parallel execution
│   ├── build_and_push.sh       # Builds Docker image and pushes to ECR
│   ├── deploy_infra.sh         # Automates Terraform Init & Apply
│   ├── destroy_infra.sh        # Automates Terraform Destroy
│   └── delete_image.sh         # Deletes image from ECR
└── Dockerfile                  # Container definition for the collector
```

## 🔄 Data Collection Workflow

The collection process is orchestrated by `scripts/run_collection.sh` and runs in two main phases: **Parallel Collection** and **Merge & Upload**.

```mermaid
graph TD
    Start[Trigger / Cron] --> Orchestrator[run_collection.sh]
    
    subgraph "Phase 1: Parallel Collection"
        Orchestrator -->|Background| SPS[sps/collect_sps.py]
        Orchestrator -->|Background| IF[if/collect_if.py]
        Orchestrator -->|Background| Price[price/collect_price.py]
        
        SPS -->|Access| Metadata[S3: sps_metadata.yaml]
        SPS -->|Write Key| TMP[/tmp/sps_key.txt]
        SPS -->|Save| S3Raw[S3: rawdata/azure/sps/...]
        
        IF -->|Save| S3Raw
        Price -->|Save| S3Raw
    end
    
    subgraph "Phase 2: Merge & Upload"
        SPS -->|Done| Wait{Wait All}
        IF -->|Done| Wait
        Price -->|Done| Wait
        
        Wait -->|Read Key| Merge[merge/merge_data.py]
        Merge -->|Load| S3Raw
        Merge -->|Compare| PrevState[S3: latest_data]
        Merge -->|Upload| Outputs
    end
    
    Outputs --> Timestream
    Outputs --> CloudWatch
    Outputs --> S3Final[S3: rawdata/azure/localfile]
```

## 🧩 Component Details

### 1. SPS Collector (`sps/`)
*   **`collect_sps.py`**: The entry point. It handles:
    *   **Rotation Management**: Reads `sps_metadata.yaml` from S3 to determine the next 'Desired Count' (1, 5, ..., 50) to query.
    *   **Workload Generation**: If the date changes, it triggers `load_sps` to fetch fresh Price API data and re-calculate initialized request parameters (First Time Optimization).
    *   **Inter-Process Communication**: Writes the file path (key) of the collected SPS data to `/tmp/sps_key.txt` so the Merge job knows what dataset to process.
*   **`load_sps.py`**: Contains the heavy logic for interacting with Azure SPS API, including "Greedy Clustering" to optimize API calls within quota limits.

### 2. Price Collector (`price/`)
*   **`collect_price.py`**: Queries the Azure Retail Prices API.
*   **Multithreading**: Uses `ThreadPoolExecutor` to fetch pages in parallel (using `$skip` parameter) for speed.
*   **Reusability**: The logic is exposed as a function so `load_sps.py` can import it directly to generate fresh workloads.

### 3. Instance Family Collector (`if/`)
*   **`collect_if.py`**: Queries Azure Resource Graph to map Instance Types to Families (e.g., `Standard_D2s_v3` -> `StandardDSv3Family`).

### 4. Merge Logic (`merge/`)
*   **`merge_data.py`**:
    *   Loads the specific SPS file (via key) and corresponding IF/Price files (via timestamp match).
    *   Performs an Outer Join.
    *   **Score Integration**: Calculates `T2` (scores >= 2) and `T3` (scores == 3) metrics, merging with previous state to maintain historical maximums.
    *   **Change Detection**: Compares current dataset with the previous run's dataset to identify changes.
    *   **Upload**: Sends data to Amazon Timestream, S3 (raw & latest), and CloudWatch Logs.

## 🏗 Infrastructure (IaC)

The `infrastructure/` directory contains Terraform code to deploy the AWS Batch environment required to run this Azure container.

### Resources Created
*   **AWS Batch Compute Environment**: Configured with `SPOT` instances (e.g., `m5.large`, `c5.large`) and `SPOT_PRICE_CAPACITY_OPTIMIZED` strategy.
*   **AWS Batch Job Queue**: Connects collection jobs to the Compute Environment.
*   **AWS Batch Job Definition**: Defines the container properties (Image, vCPU, Memory, Roles) for the Azure Collection Job.
*   **EventBridge Scheduler**: Triggers the collection job every 10 minutes (CRON).
*   **IAM Roles**:
    *   `batch_service_role`: For Batch service operations.
    *   `batch_job_role`: Grants the container access to S3, Timestream, DynamoDB (for Azure Auth), and SSM.
    *   `ecs_instance_role`: For the underlying EC2 instances.

## 📜 Deployment Scripts

The `scripts/` directory provides utilities to automate the build and deployment lifecycle.

### 1. `build_and_push.sh`
Builds the Docker image and pushes it to Amazon ECR.

**Parameters:**
*   `-r`: AWS Region (default: `us-west-2`)
*   `-p`: AWS Profile (optional)
*   `-a`: AWS Access Key ID (for build argument)
*   `-s`: AWS Secret Access Key (for build argument)

**Example:**
```bash
./scripts/build_and_push.sh -p my-profile -a AKIA... -s SECRET...
```

### 2. `deploy_infra.sh`
Deploys the Terraform infrastructure using `terraform apply`.

**Parameters:**
*   `-v`: VPC ID (Required)
*   `-s`: Subnet IDs (JSON list, e.g., `'["subnet-1", "subnet-2"]'`) (Required)
*   `-g`: Security Group IDs (JSON list, e.g., `'["sg-1"]'`) (Required)
*   `-i`: Docker Image URI (Required, output from build script)
*   `-r`: AWS Region (default: `us-west-2`)
*   `-b`: S3 Bucket Name (default: `spotlake`)
*   `-p`: AWS Profile (optional)

**Example:**
```bash
./scripts/deploy_infra.sh \
  -v vpc-12345 \
  -s '["subnet-a", "subnet-b"]' \
  -g '["sg-123"]' \
  -i 123456789012.dkr.ecr.us-west-2.amazonaws.com/spotlake-azure-batch:latest \
  -p my-profile
```

### 3. `destroy_infra.sh`
Tears down the infrastructure using `terraform destroy`.

**Parameters:**
Same as `deploy_infra.sh`.

**Example:**
```bash
./scripts/destroy_infra.sh -v vpc-12345 ... -p my-profile
```

### 4. `delete_image.sh`
Deletes a specific image tag from the ECR repository.

**Parameters:**
*   `-r`: AWS Region
*   `-t`: Image Tag (default: `latest`)

## 🛠 Local Usage

To run the full pipeline locally (assuming you have necessary Python env or use the container):

```bash
# Usage: ./scripts/run_collection.sh <TIMESTAMP_UTC>
./scripts/run_collection.sh "2025-12-13 13:00"
```

### Environment Variables
The following environment variables (or DynamoDB entries) are expected:
*   `error_notification_slack_webhook_url`: For Slack alerts (SSM or Env).
*   **DynamoDB "AzureAuth"**: Stores `TenantId`, `ClientId`, `ClientSecret`, `SubscriptionId` for Azure Authentication.

## 💾 State Management (S3)

New implementation uses `utils/constants.py` to define S3 paths (replacing legacy `const_config.py`).
*   **Raw Data**: `s3://spotlake/rawdata/azure/{sps|spot_price|spot_if}/{YYYY}/{MM}/{DD}/...`
*   **SPS Metadata**: `s3://spotlake/rawdata/azure/localfile/sps_metadata.yaml` (Tracks rotation index & workload date)
*   **Latest State**: `s3://spotlake/latest_data/azure/...` (For change detection)
