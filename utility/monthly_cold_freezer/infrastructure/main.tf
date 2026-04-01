provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile
}

locals {
  suffix                = var.environment == "production" ? "" : "-${var.environment}"
  name_base             = "${var.name_prefix}${local.suffix}"
  log_group_name        = "/aws/batch/${local.name_base}"
  small_compute_name    = "${local.name_base}-small-ce"
  heavy_compute_name    = "${local.name_base}-heavy-ce"
  small_queue_name      = "${local.name_base}-small"
  heavy_queue_name      = "${local.name_base}-heavy"
  coordinator_job_name  = "${local.name_base}-coordinator"
  aws_worker_job_name   = "${local.name_base}-aws-worker"
  azure_worker_job_name = "${local.name_base}-azure-worker"
  gcp_worker_job_name   = "${local.name_base}-gcp-worker"
  common_environment = [
    { name = "AWS_REGION", value = var.aws_region },
    { name = "AWS_DEFAULT_REGION", value = var.aws_region },
    { name = "FREEZE_ENV", value = var.freeze_env },
    { name = "TITANS_BUCKET", value = var.titans_bucket },
    { name = "RAW_BUCKET", value = var.raw_bucket }
  ]
  common_tags = {
    Application = local.name_base
    Environment = var.environment
    ManagedBy   = "terraform"
    VpcId       = var.vpc_id
  }
}

resource "aws_cloudwatch_log_group" "monthly_freezer" {
  name              = local.log_group_name
  retention_in_days = 30
}

resource "aws_iam_role" "batch_service_role" {
  name = "${local.name_base}-batch-service-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "batch.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "batch_service_role_policy" {
  role       = aws_iam_role.batch_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole"
}

resource "aws_iam_role" "ecs_task_execution_role" {
  name = "${local.name_base}-ecs-task-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "ecs_task_execution_role_policy" {
  role       = aws_iam_role.ecs_task_execution_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "batch_job_role" {
  name = "${local.name_base}-job-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ecs-tasks.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_policy" "batch_job_policy" {
  name        = "${local.name_base}-job-policy"
  description = "Permissions for monthly cold freezer Batch jobs"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${var.titans_bucket}",
          "arn:aws:s3:::${var.titans_bucket}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${var.raw_bucket}"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "batch:DescribeJobs",
          "batch:SubmitJob"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "ssm:GetParameter"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "batch_job_policy_attachment" {
  role       = aws_iam_role.batch_job_role.name
  policy_arn = aws_iam_policy.batch_job_policy.arn
}

resource "aws_iam_role" "ecs_instance_role" {
  name = "${local.name_base}-ecs-instance-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "ecs_instance_role_policy" {
  role       = aws_iam_role.ecs_instance_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"
}

resource "aws_iam_instance_profile" "ecs_instance_role" {
  name = "${local.name_base}-ecs-instance-profile"
  role = aws_iam_role.ecs_instance_role.name
}

resource "aws_batch_compute_environment" "freeze_small" {
  name = local.small_compute_name

  compute_resources {
    type                = "SPOT"
    min_vcpus           = 0
    desired_vcpus       = 0
    max_vcpus           = var.small_max_vcpus
    instance_type       = ["r7g.xlarge", "x2gd.xlarge"]
    subnets             = var.subnet_ids
    security_group_ids  = var.security_group_ids
    instance_role       = aws_iam_instance_profile.ecs_instance_role.arn
    allocation_strategy = "SPOT_PRICE_CAPACITY_OPTIMIZED"
    tags                = local.common_tags
  }

  service_role = aws_iam_role.batch_service_role.arn
  type         = "MANAGED"

  depends_on = [
    aws_iam_role_policy_attachment.batch_service_role_policy,
  ]
}

resource "aws_batch_compute_environment" "freeze_heavy" {
  name = local.heavy_compute_name

  compute_resources {
    type                = "SPOT"
    min_vcpus           = 0
    desired_vcpus       = 0
    max_vcpus           = var.heavy_max_vcpus
    instance_type       = ["x2gd.2xlarge", "r8g.4xlarge"]
    subnets             = var.subnet_ids
    security_group_ids  = var.security_group_ids
    instance_role       = aws_iam_instance_profile.ecs_instance_role.arn
    allocation_strategy = "SPOT_PRICE_CAPACITY_OPTIMIZED"
    tags                = local.common_tags
  }

  service_role = aws_iam_role.batch_service_role.arn
  type         = "MANAGED"

  depends_on = [
    aws_iam_role_policy_attachment.batch_service_role_policy,
  ]
}

resource "aws_batch_job_queue" "freeze_small" {
  name     = local.small_queue_name
  state    = "ENABLED"
  priority = 10

  compute_environment_order {
    order               = 1
    compute_environment = aws_batch_compute_environment.freeze_small.arn
  }
}

resource "aws_batch_job_queue" "freeze_heavy" {
  name     = local.heavy_queue_name
  state    = "ENABLED"
  priority = 5

  compute_environment_order {
    order               = 1
    compute_environment = aws_batch_compute_environment.freeze_heavy.arn
  }
}

resource "aws_batch_job_definition" "coordinator" {
  name = local.coordinator_job_name
  type = "container"

  container_properties = jsonencode({
    image            = var.image_uri
    command          = ["python", "-m", "monthly_cold_freezer.batch"]
    jobRoleArn       = aws_iam_role.batch_job_role.arn
    executionRoleArn = aws_iam_role.ecs_task_execution_role.arn
    environment = concat(local.common_environment, [
      { name = "MODE", value = "coordinator" },
      { name = "MONTHLY_FREEZE_SMALL_JOB_QUEUE", value = aws_batch_job_queue.freeze_small.name },
      { name = "MONTHLY_FREEZE_HEAVY_JOB_QUEUE", value = aws_batch_job_queue.freeze_heavy.name },
      { name = "MONTHLY_FREEZE_AWS_JOB_DEFINITION", value = aws_batch_job_definition.aws_worker.name },
      { name = "MONTHLY_FREEZE_AZURE_JOB_DEFINITION", value = aws_batch_job_definition.azure_worker.name },
      { name = "MONTHLY_FREEZE_GCP_JOB_DEFINITION", value = aws_batch_job_definition.gcp_worker.name }
    ])
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.monthly_freezer.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "coordinator"
      }
    }
    resourceRequirements = [
      { type = "VCPU", value = "1" },
      { type = "MEMORY", value = "2048" }
    ]
  })
}

resource "aws_batch_job_definition" "aws_worker" {
  name = local.aws_worker_job_name
  type = "container"

  container_properties = jsonencode({
    image            = var.image_uri
    command          = ["python", "-m", "monthly_cold_freezer.batch"]
    jobRoleArn       = aws_iam_role.batch_job_role.arn
    executionRoleArn = aws_iam_role.ecs_task_execution_role.arn
    environment = concat(local.common_environment, [
      { name = "MODE", value = "worker" },
      { name = "FREEZE_PROVIDER", value = "aws" },
      { name = "IGNORE_COMPLETENESS", value = "0" }
    ])
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.monthly_freezer.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "aws-worker"
      }
    }
    resourceRequirements = [
      { type = "VCPU", value = "1" },
      { type = "MEMORY", value = "12288" }
    ]
  })
}

resource "aws_batch_job_definition" "gcp_worker" {
  name = local.gcp_worker_job_name
  type = "container"

  container_properties = jsonencode({
    image            = var.image_uri
    command          = ["python", "-m", "monthly_cold_freezer.batch"]
    jobRoleArn       = aws_iam_role.batch_job_role.arn
    executionRoleArn = aws_iam_role.ecs_task_execution_role.arn
    environment = concat(local.common_environment, [
      { name = "MODE", value = "worker" },
      { name = "FREEZE_PROVIDER", value = "gcp" },
      { name = "IGNORE_COMPLETENESS", value = "1" }
    ])
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.monthly_freezer.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "gcp-worker"
      }
    }
    resourceRequirements = [
      { type = "VCPU", value = "1" },
      { type = "MEMORY", value = "2048" }
    ]
  })
}

resource "aws_batch_job_definition" "azure_worker" {
  name = local.azure_worker_job_name
  type = "container"

  container_properties = jsonencode({
    image            = var.image_uri
    command          = ["python", "-m", "monthly_cold_freezer.batch"]
    jobRoleArn       = aws_iam_role.batch_job_role.arn
    executionRoleArn = aws_iam_role.ecs_task_execution_role.arn
    environment = concat(local.common_environment, [
      { name = "MODE", value = "worker" },
      { name = "FREEZE_PROVIDER", value = "azure" },
      { name = "IGNORE_COMPLETENESS", value = "0" }
    ])
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.monthly_freezer.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "azure-worker"
      }
    }
    resourceRequirements = [
      { type = "VCPU", value = "8" },
      { type = "MEMORY", value = "122880" }
    ]
  })
}
