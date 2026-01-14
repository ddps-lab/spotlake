# Azure SpotLake Collector (Batch 테스트 환경)

[English](README.md) | [한국어](README_kr.md)

이 디렉토리는 Azure Spot Instance 데이터 수집기의 **테스트 환경** 구현을 포함합니다. Production `azure/batch` 디렉토리의 구조를 미러링하지만, 격리된 환경에서 작동하도록 구성되어 테스트 리소스에 데이터를 쓰고 필요한 경우 Production 리소스에서 설정을 읽습니다.

## Production과의 주요 차이점

| 기능 | Production (`azure/batch`) | Test (`azure/batch-test`) |
| :--- | :--- | :--- |
| **S3 Write Bucket** | `spotlake` | `spotlake-test` |
| **S3 Read Bucket** | `spotlake` | `spotlake` (Config/Metadata), `spotlake-test` (Raw Data/State) |
| **DynamoDB Table** | `azure` | `azure-test` |
| **Infrastructure** | `spotlake-azure-compute-env` | `spotlake-azure-test-compute-env` |
| **Monitoring** | Basic Logs | **Enhanced**: 메모리 사용량 및 실행 시간 로깅 |

## 디렉토리 구조

```graphql
azure/batch-test/
├── if/                 # 포팅된 IF 수집기 (spotlake-test에 쓰기)
├── price/              # 포팅된 Price 수집기 (spotlake-test에 쓰기)
├── sps/                # 포팅된 SPS 수집기 (Prod Config 읽기, Test Data 쓰기)
├── merge/              # 포팅된 Merge 로직 (Test Data 읽기, Test DB/S3에 쓰기)
├── sps_module/         # 공유 SPS 로직 (포팅됨)
├── utils/              # 수정된 유틸리티 (Read/Write Bucket 분리)
├── infrastructure/     # Terraform IaC (*-test 리소스 생성)
├── scripts/            # 오케스트레이션 스크립트 (모니터링 포함)
└── Dockerfile          # 테스트 컨테이너 정의 (모니터링 도구 포함)
```

## Read/Write 분리 전략

Production 데이터를 오염시키지 않고 안전하게 테스트하기 위해 `utils/constants.py` 및 `utils/common.py`가 수정되었습니다:

*   **Read Operations**: 기본값은 `READ_BUCKET_NAME` ("spotlake")입니다. 이를 통해 테스트 수집기가 Production 버킷에서 `sps_metadata.yaml` 또는 region maps와 같은 공유 설정을 읽을 수 있습니다.
*   **Write Operations**: 기본값은 `WRITE_BUCKET_NAME` ("spotlake-test")입니다. 모든 수집된 데이터 (SPS, Price, IF)와 병합된 결과가 여기에 저장됩니다.
*   **Explicit Overrides**: `merge_data.py`와 같은 스크립트는 테스트 수집기가 생성한 raw data를 읽을 때 `bucket_name=STORAGE_CONST.WRITE_BUCKET_NAME`을 명시적으로 지정하여, Production 데이터가 아닌 *테스트* 데이터를 처리하도록 보장합니다.

## Enhanced Monitoring

테스트 환경에는 성능 및 리소스 사용량을 검증하기 위한 추가 모니터링이 포함되어 있습니다:

*   **Memory Monitoring**: `scripts/run_collection.sh`는 10초마다 Python 수집기의 RSS 메모리 사용량을 추적하는 백그라운드 프로세스를 시작합니다.
*   **Execution Stats**: 시작 시간, 수집 기간, 병합 기간이 캡처됩니다.
*   **Logs**: 이러한 통계는 분석을 위해 `s3://spotlake-test/rawdata/azure/localfile/`에 업로드됩니다.

## 배포 및 사용

### 1. 이미지 빌드 및 푸시
`scripts/`의 스크립트를 사용합니다 (프로젝트 루트에 있는지 확인):

```bash
# 예시 (루트에서 실행하는 경우 경로를 적절히 조정)
./collector/spot-dataset/azure/batch-test/scripts/build_and_push.sh ...
```

### 2. 인프라 배포
`infrastructure/`로 이동하여 Terraform을 사용합니다:

```bash
cd collector/spot-dataset/azure/batch-test/infrastructure
terraform init
terraform apply -var="image_uri=..." -var='subnet_ids=["..."]' ...
```

### 3. 로컬 실행
전체 수집 파이프라인을 로컬에서 테스트하려면:

```bash
./collector/spot-dataset/azure/batch-test/scripts/run_collection.sh "2025-12-15 10:00"
```

## 중요 사항

*   **Azure Auth**: 테스트 환경은 현재 자격 증명을 위해 `AzureAuth` DynamoDB 테이블을 공유합니다 (read-only). 이 테이블이 존재하고 유효한 자격 증명이 있는지 확인하세요.
*   **Cost**: 이 테스트 환경을 실행하면 실제 AWS Batch 리소스 (Spot Instances)가 생성되고 Azure에 실제 API 호출이 이루어집니다. 그에 따라 비용을 모니터링하세요.
