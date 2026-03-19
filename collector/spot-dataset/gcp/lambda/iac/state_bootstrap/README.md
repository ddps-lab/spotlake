# State Bootstrap

Bootstrap stack for the Terraform state bucket used by the GCP collector Lambda IaC.

## Purpose

- Create a dedicated S3 bucket for Terraform state.
- Keep state separate from SpotLake raw/query data buckets.
- Start simple: S3 backend only, no DynamoDB lock table yet.

## Usage

```bash
cd spotlake/collector/spot-dataset/gcp/lambda/iac/state_bootstrap
AWS_PROFILE=spotrank terraform init
AWS_PROFILE=spotrank terraform apply
```

Default bucket name:

```text
spotlake-terraform-state
```

You can override it:

```bash
AWS_PROFILE=spotrank terraform apply -var='bucket_name=my-custom-state-bucket'
```

## Next step

Initialize the env root you want and run Terraform there:

```bash
cd ../test
terraform init
terraform apply
```
