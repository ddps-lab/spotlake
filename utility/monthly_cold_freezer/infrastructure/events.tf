resource "aws_iam_role" "scheduler_role" {
  name = "${local.name_base}-scheduler-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
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
  name        = "${local.name_base}-scheduler-policy"
  description = "Allows EventBridge to submit the monthly freezer coordinator job"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "batch:SubmitJob"
        ]
        Resource = [
          aws_batch_job_definition.coordinator.arn,
          aws_batch_job_queue.freeze_small.arn
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

resource "aws_cloudwatch_event_rule" "coordinator_schedule" {
  count               = var.enable_schedule ? 1 : 0
  name                = "${local.name_base}-schedule"
  description         = "Triggers monthly cold freezer coordinator at 00:10/00:15/00:20 UTC on day 1"
  schedule_expression = "cron(10,15,20 0 1 * ? *)"
}

resource "aws_cloudwatch_event_target" "coordinator_schedule" {
  count     = var.enable_schedule ? 1 : 0
  rule      = aws_cloudwatch_event_rule.coordinator_schedule[0].name
  target_id = "${local.name_base}-coordinator"
  arn       = aws_batch_job_queue.freeze_small.arn
  role_arn  = aws_iam_role.scheduler_role.arn

  batch_target {
    job_definition = aws_batch_job_definition.coordinator.arn
    job_name       = "${local.name_base}-scheduled"
  }
}
