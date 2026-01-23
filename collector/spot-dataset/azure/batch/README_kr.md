# Azure SpotLake Collector (Batch 구현)

[English](README.md) | [한국어](README_kr.md)

이 디렉토리는 AWS Batch용 Azure Spot Instance 데이터 수집기 구현을 포함합니다 (개념적으로는 Azure Batch이지만, 유사한 컨테이너화된 아키텍처를 활용). Spot Placement Scores (SPS), Spot Prices, Instance Family (IF) 데이터를 수집하고, 이를 병합하여 데이터 저장소 (Timestream, S3)에 업로드하도록 설계되었습니다.

## 디렉토리 구조

```graphql
azure/batch/
├── if/
│   └── collect_if.py           # Azure Resource Graph를 통해 Instance Family 데이터 수집
├── price/
│   └── collect_price.py        # Azure Retail Prices API를 통해 Spot/On-Demand 가격 수집
├── sps/
│   ├── collect_sps.py          # SPS 수집의 메인 진입점 (트리거 및 메타데이터 관리)
│   └── load_sps.py             # SPS API 쿼리 및 greedy clustering 핵심 로직
├── merge/
│   ├── merge_data.py           # 수집된 데이터 병합 (SPS + Price + IF), T2/T3 계산
│   ├── compare_data.py         # 이전 상태와의 변경 감지 로직
│   └── upload_data.py          # TimeStream, S3, CloudWatch 업로드 처리
├── sps_module/                   # SPS 파라미터 준비를 위해 포팅된 레거시 로직
│   ├── sps_location_manager.py
│   ├── sps_prepare_parameters.py
│   └── sps_shared_resources.py
├── utils/
│   ├── common.py               # 공유 핸들러 (S3, Logger, Timestream, CloudWatch)
│   ├── constants.py            # 중앙 집중식 상수 및 S3 경로
│   ├── azure_auth.py           # Azure 인증 및 자격 증명 관리
│   └── slack_msg_sender.py     # Slack 알림 유틸리티
├── infrastructure/               # Terraform IaC
│   ├── main.tf                 # Batch Compute Env, Job Queue, Job Def, IAM Roles 정의
│   ├── variables.tf            # 입력 변수 (VPC, Subnet, Image URI 등)
│   └── events.tf               # EventBridge Scheduler 정의 (CRON 트리거)
├── scripts/
│   ├── run_collection.sh       # 병렬 실행을 위한 오케스트레이션 스크립트
│   ├── build_and_push.sh       # Docker 이미지 빌드 및 ECR 푸시
│   ├── deploy_infra.sh         # Terraform Init 및 Apply 자동화
│   ├── destroy_infra.sh        # Terraform Destroy 자동화
│   └── delete_image.sh         # ECR에서 이미지 삭제
└── Dockerfile                  # 수집기용 컨테이너 정의
```

## 데이터 수집 워크플로우

수집 프로세스는 `scripts/run_collection.sh`에 의해 오케스트레이션되며 두 가지 주요 단계로 실행됩니다: **병렬 수집** 및 **병합 & 업로드**.

```mermaid
graph TD
    Start[트리거 / Cron] --> Orchestrator[run_collection.sh]
    
    subgraph "Phase 1: 병렬 수집"
        Orchestrator -->|백그라운드| SPS[sps/collect_sps.py]
        Orchestrator -->|백그라운드| IF[if/collect_if.py]
        Orchestrator -->|백그라운드| Price[price/collect_price.py]
        
        SPS -->|접근| Metadata[S3: sps_metadata.yaml]
        SPS -->|키 쓰기| TMP[/tmp/sps_key.txt]
        SPS -->|저장| S3Raw[S3: rawdata/azure/sps/...]
        
        IF -->|저장| S3Raw
        Price -->|저장| S3Raw
    end
    
    subgraph "Phase 2: 병합 & 업로드"
        SPS -->|완료| Wait{모두 대기}
        IF -->|완료| Wait
        Price -->|완료| Wait
        
        Wait -->|키 읽기| Merge[merge/merge_data.py]
        Merge -->|로드| S3Raw
        Merge -->|비교| PrevState[S3: latest_data]
        Merge -->|업로드| Outputs
    end
    
    Outputs --> Timestream
    Outputs --> CloudWatch
    Outputs --> S3Final[S3: rawdata/azure/localfile]
```

## 컴포넌트 상세

### 1. SPS 수집기 (`sps/`)
*   **`collect_sps.py`**: 진입점. 다음을 처리합니다:
    *   **로테이션 관리**: S3에서 `sps_metadata.yaml`을 읽어 다음에 쿼리할 'Desired Count' (1, 5, ..., 50)를 결정합니다.
    *   **워크로드 생성**: 날짜가 변경되면 `load_sps`를 트리거하여 새로운 Price API 데이터를 가져오고 초기화된 요청 파라미터를 다시 계산합니다 (First Time Optimization).
    *   **프로세스 간 통신**: 수집된 SPS 데이터의 파일 경로(키)를 `/tmp/sps_key.txt`에 기록하여 Merge 작업이 처리할 데이터셋을 알 수 있도록 합니다.
*   **`load_sps.py`**: 할당량 제한 내에서 API 호출을 최적화하는 "Greedy Clustering"을 포함하여 Azure SPS API와 상호 작용하는 핵심 로직을 포함합니다.

### 2. Price 수집기 (`price/`)
*   **`collect_price.py`**: Azure Retail Prices API를 쿼리합니다.
*   **멀티스레딩**: 속도를 위해 `ThreadPoolExecutor`를 사용하여 페이지를 병렬로 가져옵니다 (`$skip` 파라미터 사용).
*   **재사용성**: `load_sps.py`가 직접 임포트하여 새로운 워크로드를 생성할 수 있도록 함수로 노출됩니다.

### 3. Instance Family 수집기 (`if/`)
*   **`collect_if.py`**: Azure Resource Graph를 쿼리하여 Instance Types를 Families에 매핑합니다 (예: `Standard_D2s_v3` -> `StandardDSv3Family`).

### 4. 병합 로직 (`merge/`)
*   **`merge_data.py`**:
    *   특정 SPS 파일 (키를 통해)과 해당 IF/Price 파일 (타임스탬프 매칭을 통해)을 로드합니다.
    *   Outer Join을 수행합니다.
    *   **점수 통합**: `T2` (scores >= 2) 및 `T3` (scores == 3) 메트릭을 계산하고, 이전 상태와 병합하여 과거 최대값을 유지합니다.
    *   **변경 감지**: 현재 데이터셋을 이전 실행의 데이터셋과 비교하여 변경 사항을 식별합니다.
    *   **업로드**: Amazon Timestream, S3 (raw & latest), CloudWatch Logs에 데이터를 전송합니다.

## 인프라 (IaC)

`infrastructure/` 디렉토리는 이 Azure 컨테이너를 실행하는 데 필요한 AWS Batch 환경을 배포하기 위한 Terraform 코드를 포함합니다.

### 생성되는 리소스
*   **AWS Batch Compute Environment**: `SPOT` 인스턴스 (예: `m5.large`, `c5.large`) 및 `SPOT_PRICE_CAPACITY_OPTIMIZED` 전략으로 구성됩니다.
*   **AWS Batch Job Queue**: 수집 작업을 Compute Environment에 연결합니다.
*   **AWS Batch Job Definition**: Azure Collection Job의 컨테이너 속성 (Image, vCPU, Memory, Roles)을 정의합니다.
*   **EventBridge Scheduler**: 10분마다 수집 작업을 트리거합니다 (CRON).
*   **IAM Roles**:
    *   `batch_service_role`: Batch 서비스 운영용.
    *   `batch_job_role`: 컨테이너에 S3, Timestream, DynamoDB (Azure Auth용), SSM 접근 권한을 부여합니다.
    *   `ecs_instance_role`: 기본 EC2 인스턴스용.

## 배포 스크립트

`scripts/` 디렉토리는 빌드 및 배포 라이프사이클을 자동화하는 유틸리티를 제공합니다.

### 1. `build_and_push.sh`
Docker 이미지를 빌드하고 Amazon ECR에 푸시합니다.

**파라미터:**
*   `-r`: AWS 리전 (기본값: `us-west-2`)
*   `-p`: AWS 프로파일 (선택사항)
*   `-a`: AWS Access Key ID (빌드 인자용)
*   `-s`: AWS Secret Access Key (빌드 인자용)

**예시:**
```bash
./scripts/build_and_push.sh -p my-profile -a AKIA... -s SECRET...
```

### 2. `deploy_infra.sh`
`terraform apply`를 사용하여 Terraform 인프라를 배포합니다.

**파라미터:**
*   `-v`: VPC ID (필수)
*   `-s`: Subnet IDs (JSON 리스트, 예: `'["subnet-1", "subnet-2"]'`) (필수)
*   `-g`: Security Group IDs (JSON 리스트, 예: `'["sg-1"]'`) (필수)
*   `-i`: Docker Image URI (필수, 빌드 스크립트 출력)
*   `-r`: AWS 리전 (기본값: `us-west-2`)
*   `-b`: S3 버킷 이름 (기본값: `spotlake`)
*   `-p`: AWS 프로파일 (선택사항)

**예시:**
```bash
./scripts/deploy_infra.sh \
  -v vpc-12345 \
  -s '["subnet-a", "subnet-b"]' \
  -g '["sg-123"]' \
  -i 123456789012.dkr.ecr.us-west-2.amazonaws.com/spotlake-azure-batch:latest \
  -p my-profile
```

### 3. `destroy_infra.sh`
`terraform destroy`를 사용하여 인프라를 제거합니다.

**파라미터:**
`deploy_infra.sh`와 동일합니다.

**예시:**
```bash
./scripts/destroy_infra.sh -v vpc-12345 ... -p my-profile
```

### 4. `delete_image.sh`
ECR 리포지토리에서 특정 이미지 태그를 삭제합니다.

**파라미터:**
*   `-r`: AWS 리전
*   `-t`: 이미지 태그 (기본값: `latest`)

## 로컬 사용

전체 파이프라인을 로컬에서 실행하려면 (필요한 Python 환경 또는 컨테이너가 있다고 가정):

```bash
# 사용법: ./scripts/run_collection.sh <TIMESTAMP_UTC>
./scripts/run_collection.sh "2025-12-13 13:00"
```

### 환경 변수
다음 환경 변수 (또는 DynamoDB 항목)가 예상됩니다:
*   `error_notification_slack_webhook_url`: Slack 알림용 (SSM 또는 Env).
*   **DynamoDB "AzureAuth"**: Azure 인증을 위한 `TenantId`, `ClientId`, `ClientSecret`, `SubscriptionId`를 저장합니다.

## 상태 관리 (S3)

새로운 구현은 `utils/constants.py`를 사용하여 S3 경로를 정의합니다 (레거시 `const_config.py` 대체).
*   **Raw Data**: `s3://spotlake/rawdata/azure/{sps|spot_price|spot_if}/{YYYY}/{MM}/{DD}/...`
*   **SPS 메타데이터**: `s3://spotlake/rawdata/azure/localfile/sps_metadata.yaml` (로테이션 인덱스 및 워크로드 날짜 추적)
*   **최신 상태**: `s3://spotlake/latest_data/azure/...` (변경 감지용)
