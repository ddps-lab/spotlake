# monthly_cold_freezer

월별 Warm→Cold freeze를 GitHub Actions가 아니라 AWS Batch + EventBridge로 운영하기 위한 전용 utility.

## 구성

- `batch.py`: 단일 엔트리포인트. `MODE=coordinator|worker`
- `coordinator.py`: 월별 readiness 확인, 상태 파일 관리, provider별 Batch worker submit
- `signals.py`: AWS/Azure/GCP 완료 신호 판정
- `freezer_worker.py`: Warm→Cold 변환 worker 본체
- `benchmark_runtime.py`: freezer dry-run runtime/memory/cost benchmark
- `requirements.txt`: standalone 실행/이미지 빌드 의존성
- `dockerfile`: 전용 Batch 이미지
- `infrastructure/`: Batch/EventBridge Terraform

## 동작 방식

1. coordinator가 대상 월을 결정한다. 기본값은 이전 UTC 월이다.
2. worker 입력은 `manifest base + manifest 밖의 hot tail` 이다. manifest가 stale해도 이미 존재하는 warm/hot snapshot으로 cold를 만든다.
3. readiness는 "월말까지 compaction 완료"가 아니라 "materializable warm/hot snapshot 존재" 기준이다.
4. provider별로 cold output 존재/기존 Batch job 상태를 점검한다.
5. readiness를 만족한 provider만 독립적으로 worker job을 submit한다.
6. readiness를 만족하지 못한 provider는 `waiting_on` / `noop` 으로 남고, 다른 provider 제출을 막지 않는다.

상태 파일 경로:

- `s3://titans-spotlake-data/ops/monthly_freeze/YYYY-MM/aws.json`
- `s3://titans-spotlake-data/ops/monthly_freeze/YYYY-MM/azure.json`
- `s3://titans-spotlake-data/ops/monthly_freeze/YYYY-MM/gcp.json`

`FREEZE_ENV=test` 인 경우 `test/` prefix 아래 동일 구조를 사용한다.

## 로컬 실행

```bash
cd spotlake

# coordinator
uv run --with boto3[crt] --with polars --with requests python utility/monthly_cold_freezer/batch.py \
  --mode coordinator \
  --month 2026-02 \
  --env production \
  --profile spotrank

# worker
uv run --with boto3[crt] --with polars --with requests python utility/monthly_cold_freezer/batch.py \
  --mode worker \
  --provider aws \
  --month 2026-02 \
  --env production \
  --profile spotrank \
  --skip-upload
```

자동 worker는 provider 공통으로 `ignore_completeness` 경로를 사용한다. 대신 worker가 stale manifest 뒤의 hot tail을 직접 붙여서 월말 snapshot을 만든다.

## Schedule

- coordinator scheduler는 UTC 매월 1일 `00:10`, `00:15`, `00:20` 에만 실행된다.
- 기존의 `1일~3일 30분 간격` 창은 제거했다.

## Runtime / Cost Benchmark

```bash
cd spotlake

uv run --with boto3[crt] --with polars --with requests python utility/monthly_cold_freezer/benchmark_runtime.py \
  --month 2026-03 \
  --env production \
  --profile spotrank \
  --json-out .tmp/monthly_cold_freezer_2026-03.json
```

이 스크립트는 provider별 `run_freeze(..., skip_upload=True)` 를 별도 child process에서 실행해 wall time과 peak RSS를 재고,
현재 `us-west-2` 기준 On-Demand 가격과 최근 Spot 가격으로 lane별 비용을 추정한다.

## Docker build

```bash
cd spotlake

docker build \
  -f utility/monthly_cold_freezer/dockerfile \
  -t monthly-cold-freezer:latest \
  .
```

운영 배포용 이미지는 `linux/arm64` 여야 한다. small/heavy compute environment가 모두 Graviton/ARM 인스턴스만 사용하기 때문이다.

권장 배포 방식:

```bash
cd spotlake

utility/monthly_cold_freezer/scripts/build_and_push.sh \
  -p spotrank \
  -r us-west-2 \
  -n monthly-cold-freezer \
  -t latest
```

`terraform`의 `image_uri` 는 위 스크립트가 push한 ARM64 image URI를 가리켜야 한다.

## Slack anomaly alert

coordinator는 기존 `utility/slack_msg_sender.py` 를 재사용한다.

- `MONTHLY_FREEZE_DISABLE_SLACK_ALERTS=1` 이면 알림 비활성화
- 그렇지 않으면 `error_notification_slack_webhook_url` SSM parameter 또는 동일 이름 환경변수를 사용
