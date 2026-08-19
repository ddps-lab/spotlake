# GCP Lambda Collector

## Deployment

Use the env-specific Terraform root directly.

Shadow test:

```bash
cd spotlake/collector/spot-dataset/gcp/lambda/iac/test
cp local.auto.tfvars.example.json local.auto.tfvars.json
terraform init
terraform apply
```

Production:

```bash
cd spotlake/collector/spot-dataset/gcp/lambda/iac/live
cp local.auto.tfvars.example.json local.auto.tfvars.json
terraform init
terraform apply
```

`terraform apply` prepares the function zip, builds the required Lambda layers, publishes them, and deploys the function.

## Test root behavior

`iac/test` is configured to:

- read latest baseline from `spotlake`
- write `latest/raw` to `spotlake-test`
- disable Timestream and query-selector
- upload TITANS hot/warm data under `test/`
- use `python3.12 + arm64`
- use the REST compute backend

## Local secret handling

Only two local inputs are needed in each env root:

- `credential_file`
- `error_notification_slack_webhook_url`

The credential JSON should stay outside git, for example in `iac/local/`.
