locals {
  credential_filename = basename(var.credential_file)
}

module "collector" {
  source = "../module"

  aws_region        = "us-west-2"
  function_name     = "gcp-collector"
  lambda_role_arn   = "arn:aws:iam::320674564649:role/service-role/gcp-collector-role-ud71k7mh"
  credential_file   = abspath(var.credential_file)
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
    GCP_TIMESTREAM_ENABLED               = "0"
    GCP_QUERY_SELECTOR_ENABLED           = "0"
    GCP_PUBLIC_READ_ENABLED              = "0"
  }

  titans_enabled       = true
  titans_env           = "production"
  compute_api_backend  = "rest"
  publish_new_version  = true
  enable_alias         = false
  alias_name           = "prod"
  manage_schedule      = true
  schedule_rule_name   = "gcp-collector-hourly-v2"
  schedule_expression  = "cron(0 */1 * * ? *)"
  schedule_state       = "ENABLED"
  schedule_target_id   = "gcp-collector-hourly-target"
  schedule_permission_statement_id = "AllowEventBridgeInvokeGcpCollectorV2"

  tags = {
    Service     = "spotlake-gcp-collector"
    Managed     = "terraform"
    Environment = "production"
  }
}
