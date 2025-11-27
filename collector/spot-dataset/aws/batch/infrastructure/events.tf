# ------ IAM Role for Scheduler ------

resource "aws_iam_role" "scheduler_role" {
  name = "eventbridge_scheduler_role_spotlake"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "scheduler.amazonaws.com"
        }
      },
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_policy" "scheduler_policy" {
  name        = "eventbridge_scheduler_policy_spotlake"
  description = "Policy for EventBridge Scheduler to invoke Batch"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "batch:SubmitJob"
        ]
        Resource = [
          aws_batch_job_definition.sps_job.arn,
          aws_batch_job_definition.if_job.arn,
          aws_batch_job_definition.price_job.arn,
          aws_batch_job_definition.merge_job.arn,
          aws_batch_job_definition.workload_job.arn,
          aws_batch_job_queue.spot_job_queue.arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "iam:PassRole"
        ]
        Resource = [
          aws_iam_role.batch_job_role.arn,
          aws_iam_role.ecs_task_execution_role.arn
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "scheduler_policy_attachment" {
  role       = aws_iam_role.scheduler_role.name
  policy_arn = aws_iam_policy.scheduler_policy.arn
}

# ------ Schedules using CloudWatch Events ------

# SPS Collection (Every 10 minutes)
resource "aws_cloudwatch_event_rule" "sps_schedule" {
  name                = "spotlake-sps-schedule"
  description         = "Trigger SPS collection every 10 minutes"
  schedule_expression = "cron(0/10 * * * ? *)"
}

resource "aws_cloudwatch_event_target" "sps_schedule" {
  rule      = aws_cloudwatch_event_rule.sps_schedule.name
  target_id = "spotlake-sps-job-target"
  arn       = aws_batch_job_queue.spot_job_queue.arn
  role_arn  = aws_iam_role.scheduler_role.arn

  batch_target {
    job_definition = aws_batch_job_definition.sps_job.arn
    job_name       = "sps-collection-scheduled"
  }

  input_transformer {
    input_paths = {
      time = "$.time"
    }
    input_template = <<EOF
{
  "Parameters": {
    "timestamp": "<time>"
  }
}
EOF
  }
}

# IF Collection (Every 10 minutes)
resource "aws_cloudwatch_event_rule" "if_schedule" {
  name                = "spotlake-if-schedule"
  description         = "Trigger IF collection every 10 minutes"
  schedule_expression = "cron(0/10 * * * ? *)"
}

resource "aws_cloudwatch_event_target" "if_schedule" {
  rule      = aws_cloudwatch_event_rule.if_schedule.name
  target_id = "spotlake-if-job-target"
  arn       = aws_batch_job_queue.spot_job_queue.arn
  role_arn  = aws_iam_role.scheduler_role.arn

  batch_target {
    job_definition = aws_batch_job_definition.if_job.arn
    job_name       = "if-collection-scheduled"
  }

  input_transformer {
    input_paths = {
      time = "$.time"
    }
    input_template = <<EOF
{
  "Parameters": {
    "timestamp": "<time>"
  }
}
EOF
  }
}

# Price Collection (Every 10 minutes)
resource "aws_cloudwatch_event_rule" "price_schedule" {
  name                = "spotlake-price-schedule"
  description         = "Trigger price collection every 10 minutes"
  schedule_expression = "cron(0/10 * * * ? *)"
}

resource "aws_cloudwatch_event_target" "price_schedule" {
  rule      = aws_cloudwatch_event_rule.price_schedule.name
  target_id = "spotlake-price-job-target"
  arn       = aws_batch_job_queue.spot_job_queue.arn
  role_arn  = aws_iam_role.scheduler_role.arn

  batch_target {
    job_definition = aws_batch_job_definition.price_job.arn
    job_name       = "price-collection-scheduled"
  }

  input_transformer {
    input_paths = {
      time = "$.time"
    }
    input_template = <<EOF
{
  "Parameters": {
    "timestamp": "<time>"
  }
}
EOF
  }
}

# Workload Generation (Daily at 23:55 UTC)
resource "aws_cloudwatch_event_rule" "workload_schedule" {
  name                = "spotlake-workload-schedule"
  description         = "Trigger workload generation daily at 23:55 UTC"
  schedule_expression = "cron(55 23 * * ? *)"
}

resource "aws_cloudwatch_event_target" "workload_schedule" {
  rule      = aws_cloudwatch_event_rule.workload_schedule.name
  target_id = "spotlake-workload-job-target"
  arn       = aws_batch_job_queue.spot_job_queue.arn
  role_arn  = aws_iam_role.scheduler_role.arn

  batch_target {
    job_definition = aws_batch_job_definition.workload_job.arn
    job_name       = "workload-generation-scheduled"
  }

  input_transformer {
    input_paths = {
      time = "$.time"
    }
    input_template = <<EOF
{
  "Parameters": {
    "timestamp": "<time>"
  }
}
EOF
  }
}

# ------ S3 Event Trigger for Merge ------

resource "aws_cloudwatch_event_rule" "s3_merge_rule" {
  name        = "spotlake-sps-upload-rule"
  description = "Trigger Merge Job when SPS file is uploaded"

  event_pattern = jsonencode({
    source = ["aws.s3"]
    detail-type = ["Object Created"]
    detail = {
      bucket = {
        name = [var.s3_bucket]
      }
      object = {
        key = [{
          prefix = "rawdata/aws/sps/"
        }]
      }
    }
  })
}

resource "aws_cloudwatch_event_target" "s3_merge_target" {
  rule      = aws_cloudwatch_event_rule.s3_merge_rule.name
  target_id = "spotlake-merge-job-target"
  arn       = aws_batch_job_queue.spot_job_queue.arn
  role_arn  = aws_iam_role.scheduler_role.arn # Reusing role, ensure it has batch:SubmitJob

  batch_target {
    job_definition = aws_batch_job_definition.merge_job.arn
    job_name       = "merge-data-event"
  }

  input_transformer {
    input_paths = {
      sps_key = "$.detail.object.key"
    }
    
    input_template = <<EOF
{
  "Parameters": {
    "sps_key": <sps_key>
  }
}
EOF
  }
}
