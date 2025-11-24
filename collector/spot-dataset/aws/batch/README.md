# AWS Batch Spot Dataset Collector

이 디렉토리는 AWS Batch를 사용하여 Spot Instance 관련 데이터(Spot Price, Spot Placement Score, Spot Instance Frequency 등)를 수집, 병합 및 저장하는 시스템을 구성합니다.

## 시스템 아키텍처 및 데이터 흐름

데이터 수집 및 처리는 다음과 같은 순서로 이루어집니다.

1.  **워크로드 생성 (Workload Generation)**
    *   **트리거**: 매일 23:55 UTC (EventBridge Schedule)
    *   **동작**: `generate_workload.py`가 실행되어 다음 날 수집할 리전 및 인스턴스 타입 조합을 최적화(Bin Packing)하여 워크로드 파일을 생성하고 S3에 저장합니다.
2.  **데이터 수집 (Data Collection)**
    *   **트리거**: 10분 간격 (EventBridge Schedule)
    *   **SPS 수집**: `collect_sps.py`가 생성된 워크로드를 읽어 AWS SPS API를 호출하고 결과를 S3(`rawdata/aws/sps/`)에 저장합니다.
    *   **Spot IF 수집**: `collect_if.py`가 `spotinfo`를 실행하여 중단 빈도 데이터를 수집하고 S3(`rawdata/aws/spot_if/`)에 저장합니다.
    *   **Spot Price 수집**: `collect_price.py`가 모든 리전의 Spot Price를 조회하여 S3(`rawdata/aws/spot_price/`)에 저장합니다.
3.  **데이터 병합 및 저장 (Merge & Upload)**
    *   **트리거**: S3에 SPS 데이터 파일이 업로드되면 EventBridge Rule에 의해 자동으로 트리거됩니다.
    *   **동작**: `merge_data.py`가 실행되어 SPS, IF, Price, On-Demand Price 데이터를 하나로 병합합니다. 이전 시점의 데이터와 비교하여 변경된 부분만을 추출한 뒤 Amazon Timestream에 업로드하고, 최신 상태를 S3에 갱신합니다.

## S3 기반 시스템 통합 및 상태 관리

이 시스템은 Amazon S3를 단순한 데이터 저장소가 아닌, **상태 관리(State Management)** 및 **설정 저장소(Configuration Store)** 로 활용합니다.

### 1. S3 Data Lake 구조
수집된 모든 원본 데이터는 S3에 체계적으로 저장됩니다.
*   `rawdata/aws/sps/{YYYY}/{MM}/{DD}/{HH}-{MM}_sps_{capacity}.pkl.gz`: SPS 수집 결과
*   `rawdata/aws/spot_if/{YYYY}/{MM}/{DD}/{HH}-{MM}_spot_if.pkl.gz`: Spot IF 수집 결과
*   `rawdata/aws/spot_price/{YYYY}/{MM}/{DD}/{HH}-{MM}_spot_price.pkl.gz`: Spot Price 수집 결과
*   `rawdata/aws/workloads/{YYYY}/{MM}/{DD}/binpacked_workloads.pkl.gz`: 생성된 일일 워크로드

### 2. 상태 관리 (State Management)
AWS SPS API는 쿼리 제한이 엄격하므로, 여러 AWS 계정(Credential)을 순환하며 사용해야 합니다. `collect_sps.py`는 S3에 저장된 메타데이터 파일을 통해 현재 상태를 추적합니다.

*   **메타데이터 파일**: `rawdata/aws/localfile/sps_metadata.yaml`
*   **관리 항목**:
    *   `credential_index`: 현재 사용 중인 AWS 자격 증명의 인덱스. 쿼리 할당량을 초과하면 다음 인덱스로 넘어갑니다.
    *   `target_capacity_index`: 현재 수집 중인 Target Capacity (예: 1, 5, 10, ... 50). 10분 주기마다 순차적으로 변경됩니다.
    *   `workload_date`: 워크로드 파일의 날짜가 변경되었는지 확인하여 인덱스를 초기화합니다.

### 3. 설정 및 리소스 (Configuration & Resources)
*   **자격 증명**: `credential/credential_3699.csv` 파일에 저장된 다수의 AWS 계정 정보를 S3에서 읽어와 SPS 쿼리에 사용합니다.
*   **최신 상태 유지**: `latest_data/latest_aws.json` 파일에 가장 최근 병합된 데이터를 저장하여, 다음 병합 시 변경분(Delta)을 계산하는 기준으로 활용합니다.

## 시스템 구현 상세 (System Implementation Details)

본 프로젝트의 Terraform 코드는 AWS Batch 리소스를 생성하지만, 일부 핵심 인프라(VPC, S3, Timestream)는 외부 의존성으로 취급합니다.

### 1. VPC 및 네트워크 (External)
*   **구성**: AWS Batch Compute Environment는 기존에 생성된 VPC 및 Subnet 내에 배치됩니다.
*   **변수**: `vpc_id`, `subnet_ids`, `security_group_ids`는 `variables.tf`를 통해 주입받습니다.
*   **요구사항**: 할당된 Security Group은 ECS Agent가 AWS 서비스(Batch, S3, ECR, CloudWatch)와 통신할 수 있도록 Outbound(443) 접근이 허용되어야 합니다.

### 2. Amazon S3 (External)
*   **구성**: 데이터 레이어 및 상태 저장소로 사용되는 S3 버킷은 Terraform으로 생성하지 않고, 기존 버킷 이름을 변수(`s3_bucket`)로 받습니다.
*   **권한**: Batch Job Role(`batch_job_policy_spotlake`)은 해당 버킷에 대한 읽기/쓰기 권한(`s3:GetObject`, `s3:PutObject`, `s3:ListBucket`)을 부여받습니다.

### 3. Amazon Timestream (External)
*   **구성**: 시계열 데이터(Spot Price, SPS 등)의 최종 저장소입니다.
*   **권한**: Batch Job Role은 Timestream에 레코드를 쓸 수 있는 권한(`timestream:WriteRecords`)을 가집니다. 테이블 생성은 이 코드 범위 밖에서 관리됩니다.

### 4. ECS & AWS Batch (Managed)
*   **Compute Environment**: `infrastructure/main.tf`에서 `aws_batch_compute_environment` 리소스로 정의됩니다. Spot Instance를 사용하도록 설정되어 있으며, `SPOT_CAPACITY_OPTIMIZED` 전략을 통해 중단 가능성을 최소화합니다.
*   **Job Queue**: `aws_batch_job_queue` 리소스로 정의되며, Compute Environment와 연결되어 작업의 우선순위를 관리합니다.
*   **Job Definitions**: 각 수집 작업(SPS, IF, Price, Merge, Workload)에 대한 컨테이너 정의가 포함됩니다.

## 인프라 구성 (Terraform)

`infrastructure/` 디렉토리의 Terraform 코드는 다음과 같은 AWS 리소스를 생성합니다.

### AWS Batch Compute Environment
*   **Type**: SPOT (Spot Instance 사용)
*   **Allocation Strategy**: `SPOT_CAPACITY_OPTIMIZED` (중단 확률이 가장 낮은 스팟 인스턴스 풀 사용)
*   **Instance Types**: `optimal` (AWS Batch가 적절한 인스턴스 타입을 자동 선택)
*   **Max vCPUs**: 128

### IAM Roles
*   **`aws_batch_service_role_spotlake`**: AWS Batch 서비스가 AWS 리소스를 관리하기 위한 역할.
*   **`ecs_task_execution_role_spotlake`**: ECS 에이전트가 이미지를 pull하고 로그를 전송하기 위한 역할.
*   **`batch_job_role_spotlake`**: 실제 컨테이너 내부에서 실행되는 애플리케이션이 사용하는 역할. S3 읽기/쓰기, Timestream 쓰기, EC2 정보 조회(SPS, Price 등) 권한을 가집니다.

### Job Definitions
각 작업은 동일한 Docker 이미지를 사용하지만, `command`와 리소스 할당이 다릅니다.

| Job Name | Script | vCPU | Memory | Trigger |
| :--- | :--- | :--- | :--- | :--- |
| `spotlake-sps-job` | `collect_sps.py` | 1.0 | 2048 MiB | 10분 마다 |
| `spotlake-if-job` | `collect_if.py` | 1.0 | 1024 MiB | 10분 마다 |
| `spotlake-price-job` | `collect_price.py` | 1.0 | 1024 MiB | 10분 마다 |
| `spotlake-merge-job` | `merge_data.py` | 1.0 | 2048 MiB | S3 Upload 이벤트 |
| `spotlake-workload-job` | `generate_workload.py` | 2.0 | 4096 MiB | 매일 23:55 UTC |

## Docker 이미지

*   **Base Image**: `python:3.9-slim`
*   **설치 패키지**: `boto3`, `pandas`, `numpy`, `pyyaml`, `ortools`, `requests` 및 `spotinfo` 바이너리.
*   **빌드 및 배포**: `scripts/build_and_push.sh` 스크립트를 통해 이미지를 빌드하고 ECR(`spotlake-batch`)에 푸시합니다.
*   **실행**: 단일 이미지가 모든 Batch Job에 사용되며, 각 Job Definition에서 실행할 Python 스크립트를 지정하여 동작을 구분합니다.

## 디렉토리 상세 설명

### `if/`
*   **`collect_if.py`**: `spotinfo` CLI 도구를 내부적으로 실행하여 데이터를 파싱합니다.

### `infrastructure/`
*   **`main.tf`**: Batch 환경, Job Queue, Job Definition, IAM Role 정의.
*   **`events.tf`**: EventBridge Scheduler(Cron) 및 CloudWatch Event Rule(S3 트리거) 정의.

### `merge/`
*   **`merge_data.py`**: 데이터 병합의 핵심 로직. `compare_data.py`를 사용하여 변경분 감지.

### `price/`
*   **`collect_price.py`**: `boto3`를 사용하여 `describe_spot_price_history` API 호출.

### `sps/`
*   **`collect_sps.py`**: `get_spot_placement_scores` API를 사용하여 대량의 리전/인스턴스 조합에 대한 점수 조회.

### `workload/`
*   **`generate_workload.py`**: Google OR-Tools의 Bin Packing 알고리즘을 사용하여 API 쿼리 효율을 높이는 워크로드 배치 생성.

## 배포 및 운영 가이드 (Deployment & Operations)

### 1. 사전 준비 사항 (Prerequisites)
*   AWS CLI 설치 및 자격 증명 설정 (`aws configure`)
*   Docker 실행 중
*   Terraform 설치
*   `jq` 설치 (스크립트 실행 시 필요할 수 있음)

### 2. 시스템 배포 (Deployment)

**Step 1: Docker 이미지 빌드 및 푸시**
`scripts/build_and_push.sh` 스크립트를 실행하여 이미지를 빌드하고 ECR에 업로드합니다.
```bash
# 프로젝트 루트 디렉토리에서 실행
./collector/spot-dataset/aws/batch/scripts/build_and_push.sh
```
성공 시 출력되는 `Image URI`를 복사해둡니다.

**Step 2: 인프라 배포**
`scripts/deploy_infra.sh` 스크립트는 Terraform을 사용하여 AWS Batch 환경을 구축합니다. 필수 환경 변수를 설정한 후 실행해야 합니다.

```bash
# 필수 환경 변수 설정
export TF_VAR_vpc_id="vpc-xxxxxxx"
export TF_VAR_subnet_ids='["subnet-xxxxxxx", "subnet-yyyyyyy"]'
export TF_VAR_security_group_ids='["sg-xxxxxxx"]'
export TF_VAR_image_uri="123456789012.dkr.ecr.us-west-2.amazonaws.com/spotlake-batch:latest" # Step 1에서 획득한 URI

# 배포 스크립트 실행
./collector/spot-dataset/aws/batch/scripts/deploy_infra.sh
```

### 3. 수동 실행 (Manual Execution)
EventBridge 스케줄 외에 즉시 작업을 실행해야 할 경우 AWS CLI를 사용합니다.

```bash
# SPS 수집 작업 수동 실행
aws batch submit-job \
    --job-name manual-sps-collection \
    --job-queue spotlake-job-queue \
    --job-definition spotlake-sps-job

# 워크로드 생성 작업 수동 실행
aws batch submit-job \
    --job-name manual-workload-generation \
    --job-queue spotlake-job-queue \
    --job-definition spotlake-workload-job
```

### 4. 시스템 제거 (Removal)
배포된 리소스를 제거하려면 Terraform destroy 명령을 사용합니다.

```bash
cd collector/spot-dataset/aws/batch/infrastructure
terraform destroy
```
*주의: S3 버킷과 Timestream 테이블은 Terraform으로 관리되지 않으므로 삭제되지 않습니다.*
