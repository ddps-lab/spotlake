# AWS Batch Spot Dataset Collector (TEST ENVIRONMENT)

[English](README.md) | [한국어](README_kr.md)

> [!WARNING]
> **이 디렉토리는 테스트 환경입니다.**
> Production 환경(`collector/spot-dataset/aws/batch`)과 코드는 유사하지만, 데이터 저장소가 분리되어 있습니다.

이 디렉토리는 AWS Batch를 사용하여 Spot Instance 관련 데이터 수집을 테스트하기 위한 환경입니다.
Production 데이터를 읽어와서 로직을 수행하되, 결과는 **Test S3 버킷** 및 **Test Timestream 테이블**에 저장합니다.

## 데이터 흐름 및 저장소 구성

이 테스트 환경은 **Read-Only Production / Write-Only Test** 전략을 따릅니다.

### 1. Read (Source)
다음 데이터는 **Production S3 (`spotlake`)** 에서 읽어옵니다.
*   **Workloads**: `rawdata/aws/workloads/...` (실제 생성된 워크로드 사용)
*   **Credentials**: `credential/credential_3699.csv` (실제 자격 증명 사용)
*   **Spot IF**: `rawdata/aws/spot_if/...` (Production에서 수집된 데이터 사용)
*   **Spot Price**: `rawdata/aws/spot_price/...` (Production에서 수집된 데이터 사용)
*   **On-Demand Price**: `rawdata/aws/ondemand_price/...` (Production에서 수집된 데이터 사용)

### 2. Write (Destination)
다음 데이터는 **Test S3 (`spotlake-test`)** 및 **Test Timestream**에 저장됩니다.
*   **SPS Data**: `rawdata/aws/sps/...` (새로 수집된 SPS 데이터)
*   **Merged Data**: `latest_data/latest_aws.json` 및 `rawdata/aws/merged/...` (병합된 결과)
*   **Metadata**: `rawdata/aws/localfile/sps_metadata.yaml` (테스트 환경의 독립적인 상태 관리)
*   **Timestream**: Database `spotlake-test`, Table `aws-test`

## 주요 변경 사항 (vs Production)

### `sps/collect_sps.py`
*   `READ_BUCKET_NAME`: `spotlake` (Production)
*   `WRITE_BUCKET_NAME`: `spotlake-test` (Test)
*   SPS 수집 결과 및 메타데이터(인덱스 등)는 `spotlake-test`에 저장됩니다.

### `merge/merge_data.py` & `upload_data.py`
*   `READ_BUCKET_NAME`: `spotlake` (Production)
*   `WRITE_BUCKET_NAME`: `spotlake-test` (Test)
*   Production의 Price/IF 데이터와 Test의 SPS 데이터를 병합합니다.
*   Timestream 업로드 대상: `spotlake-test` DB / `aws-test` Table.

### `price/collect_price.py` & `if/collect_if.py`
*   `BUCKET_NAME`: `spotlake-test`
*   테스트 목적으로 실행 시, 수집된 데이터를 `spotlake-test`에 저장합니다.

## 배포 및 실행

### Docker 이미지 빌드
`batch-test` 디렉토리를 기반으로 이미지를 빌드해야 합니다.

**Linux/Mac**
```bash
./collector/spot-dataset/aws/batch-test/scripts/build_and_push.sh
```

**Windows (PowerShell)**
```powershell
./collector/spot-dataset/aws/batch-test/scripts/build_and_push.ps1
```

### 인프라 배포
테스트용 인프라를 별도로 배포하거나, 기존 Batch 환경에서 Job Definition만 변경하여 테스트할 수 있습니다.
이 테스트 환경은 Production과 리소스 이름이 충돌하지 않도록 모든 리소스(Role, Queue, Compute Environment 등)에 `_test` 또는 `-test` 접미사가 붙습니다.
또한, Production과 마찬가지로 **통합된 Compute Environment와 Job Queue**를 사용하여 `run_collection.sh`를 통해 수집 및 병합 작업을 수행합니다.

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
