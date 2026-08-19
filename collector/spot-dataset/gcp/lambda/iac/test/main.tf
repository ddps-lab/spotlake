locals {
  credential_filename = basename(var.credential_file)
}

module "collector" {
  source = "../module"

  aws_region           = "us-west-2"
  function_name        = "gcp-collector-test"
  lambda_role_arn      = "arn:aws:iam::320674564649:role/service-role/gcp-collector-role-ud71k7mh"
  credential_file      = abspath(var.credential_file)
  runtime              = "python3.12"
  architecture         = "arm64"
  timeout_seconds      = 600
  memory_mb            = 384
  ephemeral_storage_mb = 512

  build_base_layer   = true
  build_titans_layer = true

  environment_variables = {
    GOOGLE_APPLICATION_CREDENTIALS       = local.credential_filename
    error_notification_slack_webhook_url = var.error_notification_slack_webhook_url
    SLACK_MESSAGE_PREFIX                 = "[GCP Collector Test]"
    GCP_READ_BUCKET_NAME                 = "spotlake"
    GCP_WRITE_BUCKET_NAME                = "spotlake-test"
    GCP_LATEST_READ_PATH                 = "latest_data/latest_gcp.json"
    GCP_LATEST_WRITE_PATH                = "latest_data/latest_gcp.json"
    GCP_RAW_PREFIX                       = "rawdata/gcp"
    GCP_TIMESTREAM_ENABLED               = "0"
    GCP_QUERY_SELECTOR_ENABLED           = "0"
    GCP_PUBLIC_READ_ENABLED              = "0"
  }

  titans_enabled                   = true
  titans_env                       = "test"
  compute_api_backend              = "rest"
  publish_new_version              = true
  enable_alias                     = false
  alias_name                       = "test"
  manage_schedule                  = true
  schedule_rule_name               = "gcp-collector-test-hourly-v1"
  schedule_expression              = "cron(0 */1 * * ? *)"
  schedule_state                   = "ENABLED"
  schedule_target_id               = "gcp-collector-test-hourly-target"
  schedule_permission_statement_id = "AllowEventBridgeInvokeGcpCollectorTestV1"

  tags = {
    Service     = "spotlake-gcp-collector-test"
    Managed     = "terraform"
    Environment = "test"
  }
}
