# ------ IAM Role for Scheduler ------

resource "aws_iam_role" "scheduler_role" {
  name = "eventbridge_scheduler_role_spotlake_azure_test"

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
  name        = "eventbridge_scheduler_policy_spotlake_azure_test"
  description = "Policy for EventBridge Scheduler to invoke Azure Batch Test Jobs"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "batch:SubmitJob"
        ]
        Resource = [
          aws_batch_job_definition.collection_job.arn,
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

# 1. Collection Schedule (Triggers run_collection.sh every 10 minutes)
resource "aws_cloudwatch_event_rule" "collection_schedule" {
  name                = "spotlake-azure-collection-schedule-test"
  description         = "Triggers SpotLake Azure Data Collection (Test) every 10 minutes"
  schedule_expression = "cron(0/10 * * * ? *)"
}

resource "aws_cloudwatch_event_target" "collection_target" {
  rule      = aws_cloudwatch_event_rule.collection_schedule.name
  target_id = "spotlake-azure-collection-target-test"
  arn       = aws_batch_job_queue.spot_job_queue.arn
  role_arn  = aws_iam_role.scheduler_role.arn

  batch_target {
    job_definition = aws_batch_job_definition.collection_job.arn
    job_name       = "spotlake-azure-collection-scheduled-test"
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
