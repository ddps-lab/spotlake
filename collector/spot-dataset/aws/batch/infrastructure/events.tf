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
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "scheduler_policy_attachment" {
  role       = aws_iam_role.scheduler_role.name
  policy_arn = aws_iam_policy.scheduler_policy.arn
}

# ------ Schedules ------

# SPS Collection (Every 10 minutes)
resource "aws_scheduler_schedule" "sps_schedule" {
  name = "spotlake-sps-schedule"
  group_name = "default"
  
  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression = "cron(0/10 * * * ? *)"

  target {
    arn      = aws_batch_job_queue.spot_job_queue.arn
    role_arn = aws_iam_role.scheduler_role.arn
    
    input = jsonencode({
      # No overrides needed, script defaults to current time
    })

    batch_parameters {
      job_definition = aws_batch_job_definition.sps_job.arn
      job_name       = "sps-collection-scheduled"
    }
  }
}

# IF Collection (Every 10 minutes)
resource "aws_scheduler_schedule" "if_schedule" {
  name = "spotlake-if-schedule"
  group_name = "default"
  
  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression = "cron(0/10 * * * ? *)"

  target {
    arn      = aws_batch_job_queue.spot_job_queue.arn
    role_arn = aws_iam_role.scheduler_role.arn
    
    batch_parameters {
      job_definition = aws_batch_job_definition.if_job.arn
      job_name       = "if-collection-scheduled"
    }
  }
}

# Price Collection (Every 10 minutes)
resource "aws_scheduler_schedule" "price_schedule" {
  name = "spotlake-price-schedule"
  group_name = "default"
  
  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression = "cron(0/10 * * * ? *)"

  target {
    arn      = aws_batch_job_queue.spot_job_queue.arn
    role_arn = aws_iam_role.scheduler_role.arn
    
    batch_parameters {
      job_definition = aws_batch_job_definition.price_job.arn
      job_name       = "price-collection-scheduled"
    }
  }
}

# Workload Generation (Daily at 23:55 UTC)
resource "aws_scheduler_schedule" "workload_schedule" {
  name = "spotlake-workload-schedule"
  group_name = "default"
  
  flexible_time_window {
    mode = "OFF"
  }

  schedule_expression = "cron(55 23 * * ? *)"

  target {
    arn      = aws_batch_job_queue.spot_job_queue.arn
    role_arn = aws_iam_role.scheduler_role.arn
    
    batch_parameters {
      job_definition = aws_batch_job_definition.workload_job.arn
      job_name       = "workload-generation-scheduled"
    }
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
      bucket  = "$.detail.bucket.name"
    }
    
    # Construct ContainerOverrides to pass arguments
    input_template = <<EOF
{
  "ContainerOverrides": {
    "Command": ["python3", "collector/spot-dataset/aws/batch/merge/merge_data.py", "--sps_key", <sps_key>, "--bucket", <bucket>]
  }
}
EOF
  }
}
