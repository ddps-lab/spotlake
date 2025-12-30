provider "aws" {
  region = var.aws_region
}

# ------ IAM Roles ------

# Batch Service Role
resource "aws_iam_role" "batch_service_role" {
  name = "aws_batch_service_role_spotlake_azure_test"

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

# Additional policy for ECS permissions
resource "aws_iam_policy" "batch_service_ecs_policy" {
  name        = "batch_service_ecs_policy_spotlake_azure_test"
  description = "Additional ECS permissions for Batch Service Role"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ecs:DescribeClusters",
          "ecs:DescribeContainerInstances",
          "ecs:ListContainerInstances"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "batch_service_ecs_policy_attachment" {
  role       = aws_iam_role.batch_service_role.name
  policy_arn = aws_iam_policy.batch_service_ecs_policy.arn
}

# ECS Task Execution Role
resource "aws_iam_role" "ecs_task_execution_role" {
  name = "ecs_task_execution_role_spotlake_azure_test"

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

# Job Role (used by the container)
resource "aws_iam_role" "batch_job_role" {
  name = "batch_job_role_spotlake_azure_test"

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

# Policy for S3, Timestream, Logs, DynamoDB (AzureAuth), SSM (Slack URL)
resource "aws_iam_policy" "batch_job_policy" {
  name        = "batch_job_policy_spotlake_azure_test"
  description = "Policy for SpotLake Azure Batch Test Jobs"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:PutObjectAcl"
        ]
        Resource = [
          "arn:aws:s3:::${var.s3_bucket}",
          "arn:aws:s3:::${var.s3_bucket}/*",
          "arn:aws:s3:::spotlake",
          "arn:aws:s3:::spotlake/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "timestream:WriteRecords",
          "timestream:DescribeEndpoints"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
            "dynamodb:GetItem",
            "dynamodb:PutItem",
            "dynamodb:UpdateItem",
            "dynamodb:Query",
            "dynamodb:Scan"
        ]
        Resource = [
            "arn:aws:dynamodb:${var.aws_region}:*:table/AzureAuth",
            "arn:aws:dynamodb:${var.aws_region}:*:table/azure-test"
        ]
      },
      {
          Effect = "Allow"
          Action = [
              "ssm:GetParameter",
              "ssm:GetParameters"
          ]
          Resource = "arn:aws:ssm:${var.aws_region}:*:parameter/error_notification_slack_webhook_url"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "batch_job_policy_attachment" {
  role       = aws_iam_role.batch_job_role.name
  policy_arn = aws_iam_policy.batch_job_policy.arn
}

# ECS Instance Role (required for EC2 launch type in AWS Batch)
resource "aws_iam_role" "ecs_instance_role" {
  name = "ecs_instance_role_spotlake_azure_test"

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
  name = "ecs_instance_role_profile_spotlake_azure_test"
  role = aws_iam_role.ecs_instance_role.name
}

# ------ Batch Compute Environment ------

resource "time_sleep" "wait_for_iam_propagation" {
  depends_on = [
    aws_iam_role_policy_attachment.batch_service_role_policy,
    aws_iam_role_policy_attachment.batch_service_ecs_policy_attachment
  ]

  create_duration = "10s"
}

resource "aws_batch_compute_environment" "spot_compute_env" {
  name = "spotlake-azure-test-compute-env"

  compute_resources {
    type = "SPOT"
    max_vcpus = 8
    min_vcpus = 0
    desired_vcpus = 0
    
    # Generic instance types good for container workloads
    instance_type = ["c6gd.medium", "c6gn.medium", "c7g.medium", "c7gd.medium", "c7gn.medium", "c8g.medium", "c8gd.medium", "c8gn.medium", "m8g.medium", "m8gd.medium", "r7g.medium", "r7gd.medium", "r8g.medium", "r8gb.medium", "r8gd.medium", "r8gn.medium", "x2gd.medium"]
    
    subnets = var.subnet_ids
    security_group_ids = var.security_group_ids
    
    instance_role = aws_iam_instance_profile.ecs_instance_role.arn
    
    allocation_strategy = "SPOT_PRICE_CAPACITY_OPTIMIZED"
  }

  service_role = aws_iam_role.batch_service_role.arn
  type         = "MANAGED"
  depends_on   = [
    time_sleep.wait_for_iam_propagation
  ]
}

resource "aws_batch_job_queue" "spot_job_queue" {
  name     = "spotlake-azure-test-job-queue"
  state    = "ENABLED"
  priority = 1
  
  compute_environment_order {
    order              = 1
    compute_environment = aws_batch_compute_environment.spot_compute_env.arn
  }
}

# ------ Batch Job Definitions ------

# Consolidated Azure Collection Job
resource "aws_batch_job_definition" "collection_job" {
  name = "spotlake-azure-test-collection-job"
  type = "container"

  container_properties = jsonencode({
    image = var.image_uri
    command = ["/bin/bash", "/app/collector/spot-dataset/azure/batch-test/scripts/run_collection.sh", "Ref::timestamp"]
    jobRoleArn = aws_iam_role.batch_job_role.arn
    environment = [
      { name = "S3_BUCKET", value = var.s3_bucket },
      { name = "AWS_REGION", value = var.aws_region }
    ]
    resourceRequirements = [
      { type = "VCPU", value = "1" },
      { type = "MEMORY", value = "2048" }
    ]
  })
  
  parameters = {
    "timestamp" = "placeholder"
  }
}
