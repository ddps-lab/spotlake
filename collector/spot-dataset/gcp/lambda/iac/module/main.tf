locals {
  lambda_dir   = abspath("${path.module}/../..")
  scripts_dir  = "${local.lambda_dir}/scripts"
  artifact_dir = abspath("${path.root}/dist")

  package_source_files = [
    "lambda_function.py",
    "collector_core.py",
    "compare_data.py",
    "runtime_config.py",
    "s3_management.py",
    "../../../../const_config.py",
    "../../../../utility/slack_msg_sender.py",
    "../../../titans_common/__init__.py",
    "../../../titans_common/config.py",
    "../../../titans_common/partitioned_eager_merge.py",
    "../../../titans_common/upload_titans.py",
    "../../../titans_common/warm_compactor.py",
    "../../../titans_common/utils.py",
  ]

  function_zip_path = "${local.artifact_dir}/${var.function_name}.zip"

  base_layer_name     = var.base_layer_name != "" ? var.base_layer_name : "${var.function_name}-modules"
  titans_layer_name   = var.titans_layer_name != "" ? var.titans_layer_name : "${var.function_name}-titans-deps"
  base_layer_zip_path = "${local.artifact_dir}/${local.base_layer_name}-${var.runtime}-${var.architecture}.zip"
  titans_layer_zip_path = "${local.artifact_dir}/${local.titans_layer_name}-${var.runtime}-${var.architecture}.zip"

  package_cache_key = sha256(join("|", concat(
    [
      var.function_name,
      var.runtime,
      basename(var.credential_file),
      filebase64sha256(var.credential_file),
      filebase64sha256("${local.scripts_dir}/package_function.sh"),
      filebase64sha256("${local.scripts_dir}/terraform_prepare_artifact.sh"),
    ],
    [for rel in local.package_source_files : filebase64sha256("${local.lambda_dir}/${rel}")]
  )))

  base_layer_cache_key = sha256(join("|", [
    local.base_layer_name,
    var.runtime,
    var.architecture,
    var.base_layer_packages,
    filebase64sha256("${local.scripts_dir}/build_base_layer.sh"),
    filebase64sha256("${local.scripts_dir}/terraform_prepare_artifact.sh"),
  ]))

  titans_layer_cache_key = sha256(join("|", [
    local.titans_layer_name,
    var.runtime,
    var.architecture,
    var.titans_layer_packages,
    filebase64sha256("${local.scripts_dir}/build_layer.sh"),
    filebase64sha256("${local.scripts_dir}/terraform_prepare_artifact.sh"),
  ]))

  merged_environment = merge(
    var.environment_variables,
    {
      TITANS_ENABLED          = var.titans_enabled ? "1" : "0"
      TITANS_ENV              = var.titans_env
      GCP_COMPUTE_API_BACKEND = var.compute_api_backend
    }
  )

  effective_layers = concat(
    var.build_base_layer ? [aws_lambda_layer_version.base[0].arn] : [],
    var.build_titans_layer ? [aws_lambda_layer_version.titans[0].arn] : []
  )
}

check "titans_layer_required" {
  assert {
    condition     = !var.titans_enabled || var.build_titans_layer
    error_message = "build_titans_layer must be true when titans_enabled is true."
  }
}

data "external" "function_package" {
  program = ["bash", "${local.scripts_dir}/terraform_prepare_artifact.sh"]

  query = {
    mode            = "package"
    output_zip      = local.function_zip_path
    cache_key       = local.package_cache_key
    credential_file = var.credential_file
    function_name   = var.function_name
    aws_region      = var.aws_region
  }
}

data "external" "base_layer" {
  count   = var.build_base_layer ? 1 : 0
  program = ["bash", "${local.scripts_dir}/terraform_prepare_artifact.sh"]

  query = {
    mode         = "base_layer"
    output_zip   = local.base_layer_zip_path
    cache_key    = local.base_layer_cache_key
    aws_region   = var.aws_region
    layer_name   = local.base_layer_name
    runtime      = var.runtime
    architecture = var.architecture
    packages     = var.base_layer_packages
  }
}

data "external" "titans_layer" {
  count   = var.build_titans_layer ? 1 : 0
  program = ["bash", "${local.scripts_dir}/terraform_prepare_artifact.sh"]

  query = {
    mode         = "titans_layer"
    output_zip   = local.titans_layer_zip_path
    cache_key    = local.titans_layer_cache_key
    aws_region   = var.aws_region
    layer_name   = local.titans_layer_name
    runtime      = var.runtime
    architecture = var.architecture
    packages     = var.titans_layer_packages
  }
}

resource "aws_lambda_layer_version" "base" {
  count = var.build_base_layer ? 1 : 0

  layer_name               = local.base_layer_name
  filename                 = data.external.base_layer[0].result.filename
  source_code_hash         = data.external.base_layer[0].result.source_code_hash
  compatible_runtimes      = [var.runtime]
  compatible_architectures = [var.architecture]
  description              = "GCP collector base modules (${var.runtime}, ${var.architecture})"
}

resource "aws_lambda_layer_version" "titans" {
  count = var.build_titans_layer ? 1 : 0

  layer_name               = local.titans_layer_name
  filename                 = data.external.titans_layer[0].result.filename
  source_code_hash         = data.external.titans_layer[0].result.source_code_hash
  compatible_runtimes      = [var.runtime]
  compatible_architectures = [var.architecture]
  description              = "GCP collector TITANS deps (${var.runtime}, ${var.architecture})"
}

resource "aws_lambda_function" "gcp_collector" {
  function_name = var.function_name
  role          = var.lambda_role_arn
  handler       = var.handler
  runtime       = var.runtime
  architectures = [var.architecture]

  filename         = data.external.function_package.result.filename
  source_code_hash = data.external.function_package.result.source_code_hash

  timeout     = var.timeout_seconds
  memory_size = var.memory_mb
  publish     = var.publish_new_version
  layers      = local.effective_layers

  ephemeral_storage {
    size = var.ephemeral_storage_mb
  }

  environment {
    variables = local.merged_environment
  }

  tags = var.tags
}

resource "aws_lambda_alias" "prod" {
  count = var.enable_alias ? 1 : 0

  name             = var.alias_name
  description      = "Stable production alias for gcp collector."
  function_name    = aws_lambda_function.gcp_collector.function_name
  function_version = aws_lambda_function.gcp_collector.version
}

resource "aws_cloudwatch_event_rule" "collector_schedule" {
  count = var.manage_schedule ? 1 : 0

  name                = var.schedule_rule_name
  description         = "GCP collector periodic schedule managed by Terraform."
  schedule_expression = var.schedule_expression
  state               = var.schedule_state
  tags                = var.tags
}

resource "aws_cloudwatch_event_target" "collector_schedule_target" {
  count = var.manage_schedule ? 1 : 0

  rule      = aws_cloudwatch_event_rule.collector_schedule[0].name
  target_id = var.schedule_target_id
  arn       = var.enable_alias ? aws_lambda_alias.prod[0].arn : aws_lambda_function.gcp_collector.arn
}

resource "aws_lambda_permission" "allow_schedule_invoke" {
  count = var.manage_schedule ? 1 : 0

  statement_id  = var.schedule_permission_statement_id
  action        = "lambda:InvokeFunction"
  function_name = var.enable_alias ? aws_lambda_alias.prod[0].arn : aws_lambda_function.gcp_collector.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.collector_schedule[0].arn
}
