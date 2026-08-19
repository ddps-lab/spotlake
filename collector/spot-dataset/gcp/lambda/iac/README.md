# GCP Collector Lambda IaC

`iac/` now has three roles:

- `module/`: shared Terraform module that packages the function, builds layers, and deploys the Lambda
- `live/`: production root
- `test/`: shadow-test root

## Operator model

Run Terraform from the environment you want:

```bash
cd spotlake/collector/spot-dataset/gcp/lambda/iac/test
terraform init
terraform apply
```

```bash
cd spotlake/collector/spot-dataset/gcp/lambda/iac/live
terraform init
terraform apply
```

No manual `package_function.sh`, no manual layer publish, and no ARN copy/paste are required. The Terraform module builds the zip and layers during plan/apply.

## GitHub Actions

There is also a manual workflow:

- [gcp-collector-terraform.yml](/home/whpark/research/TITANS/spotlake/.github/workflows/gcp-collector-terraform.yml)

From GitHub Actions, choose `test` or `live` and run the workflow manually.

Required repository secrets:

- `SPOTRANK_ACCESS_KEY_ID`
- `SPOTRANK_SECRET_ACCESS_KEY`
- `GCP_KEY_JSON_BASE64`
- `GCP_COLLECTOR_SLACK_WEBHOOK_URL`

## Local inputs

Each env root needs one ignored file:

- `live/local.auto.tfvars.json`
- `test/local.auto.tfvars.json`

Example:

```json
{
  "credential_file": "../local/gcp-hw-feature-collector-a926be14be59.json",
  "error_notification_slack_webhook_url": "REPLACE_ME"
}
```

The GCP credential JSON itself lives in `iac/local/` and is gitignored.

## Environment intent

- `live/`
  - `python3.9`
  - `x86_64`
  - `compute_api_backend = "sdk"`
  - Polars layer attached
  - TITANS disabled by default
- `test/`
  - `python3.12`
  - `arm64`
  - `compute_api_backend = "rest"`
  - Polars layer attached
  - prod-read / test-write shadow mode
  - TITANS enabled

## State bootstrap

Create the state bucket once:

```bash
cd spotlake/collector/spot-dataset/gcp/lambda/iac/state_bootstrap
terraform init
terraform apply
```

After that, `live/` and `test/` use their own fixed S3 backend keys.
