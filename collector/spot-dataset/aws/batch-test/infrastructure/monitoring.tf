# ------ Batch Job Failure Monitoring (Optional) ------
# Only deployed if slack_webhook_url is provided
# Set SLACK_WEBHOOK_URL environment variable or pass -var to enable

locals {
  # Enable monitoring only if webhook URL is provided
  enable_monitoring = var.slack_webhook_url != null && var.slack_webhook_url != ""
}

# ------ Lambda Resources (created only if monitoring enabled) ------

resource "aws_iam_role" "batch_notifier_lambda_role" {
  count = local.enable_monitoring ? 1 : 0
  name  = "batch-failure-notifier-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  count      = local.enable_monitoring ? 1 : 0
  role       = aws_iam_role.batch_notifier_lambda_role[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_lambda_function" "batch_failure_notifier" {
  count         = local.enable_monitoring ? 1 : 0
  filename      = "${path.module}/lambda/batch_failure_notifier.zip"
  function_name = "batch-failure-notifier"
  role          = aws_iam_role.batch_notifier_lambda_role[0].arn
  handler       = "batch_failure_notifier.lambda_handler"
  runtime       = "python3.11"
  timeout       = 30

  environment {
    variables = {
      SLACK_WEBHOOK_URL = var.slack_webhook_url
    }
  }

  source_code_hash = filebase64sha256("${path.module}/lambda/batch_failure_notifier.zip")
}

# ------ EventBridge Rule (only if monitoring enabled) ------

resource "aws_cloudwatch_event_rule" "batch_job_failure" {
  count       = local.enable_monitoring ? 1 : 0
  name        = "aws-batch-test-job-failure"
  description = "Capture AWS Batch job failures for Slack notification"

  event_pattern = jsonencode({
    source      = ["aws.batch"]
    detail-type = ["Batch Job State Change"]
    detail = {
      status = ["FAILED"]
      jobQueue = [aws_batch_job_queue.spot_job_queue.arn]
    }
  })
}

resource "aws_cloudwatch_event_target" "batch_failure_to_lambda" {
  count     = local.enable_monitoring ? 1 : 0
  rule      = aws_cloudwatch_event_rule.batch_job_failure[0].name
  target_id = "BatchFailureNotifierLambda"
  arn       = aws_lambda_function.batch_failure_notifier[0].arn
}

resource "aws_lambda_permission" "allow_eventbridge" {
  count         = local.enable_monitoring ? 1 : 0
  statement_id  = "AllowExecutionFromEventBridge_${aws_cloudwatch_event_rule.batch_job_failure[0].name}"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.batch_failure_notifier[0].function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.batch_job_failure[0].arn
}
