provider "aws" {
  region = var.aws_region
}

# ------ IAM Roles ------

# Batch Service Role
resource "aws_iam_role" "batch_service_role" {
  name = "aws_batch_service_role_spotlake_test"

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
  name        = "batch_service_ecs_policy_spotlake_test"
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
  name = "ecs_task_execution_role_spotlake_test"

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
  name = "batch_job_role_spotlake_test"

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

# Policy for S3, Timestream, Logs, EC2 (for SPS)
resource "aws_iam_policy" "batch_job_policy" {
  name        = "batch_job_policy_spotlake_test"
  description = "Policy for SpotLake Batch Jobs"

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
          "arn:aws:s3:::${var.s3_bucket}/*"
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
          "ec2:DescribeRegions",
          "ec2:DescribeAvailabilityZones",
          "ec2:DescribeInstanceTypeOfferings",
          "ec2:DescribeSpotPriceHistory",
          "ec2:GetSpotPlacementScores"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "pricing:GetProducts"
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

# ECS Instance Role (required for EC2 launch type)
resource "aws_iam_role" "ecs_instance_role" {
  name = "ecs_instance_role_spotlake_test"

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
  name = "ecs_instance_role_profile_spotlake_test"
  role = aws_iam_role.ecs_instance_role.name
}

# ------ Batch Compute Environment ------

# ------ Batch Compute Environments & Job Queues ------

# 1. Shared Compute Environment
resource "aws_batch_compute_environment" "spot_compute_env" {
  name = "spotlake-compute-env-test"

  compute_resources {
    type = "SPOT"
    max_vcpus = 8
    min_vcpus = 0
    desired_vcpus = 0
    
    instance_type = ["a1.medium", "c1.medium", "c6gd.medium", "c6gn.medium", "c7a.medium", "c7g.medium", "c7gd.medium", "c7gn.medium", "c8g.medium", "c8gd.medium", "c8gn.medium", "is4gen.medium", "m1.medium", "m3.medium", "m8a.medium", "m8g.medium", "m8gd.medium", "r7g.medium", "r7gd.medium", "r8a.medium", "r8g.medium", "r8gb.medium", "r8gd.medium", "r8gn.medium", "x2gd.medium"]
    
    subnets = var.subnet_ids
    security_group_ids = var.security_group_ids
    
    instance_role = aws_iam_instance_profile.ecs_instance_role.arn
    
    allocation_strategy = "SPOT_PRICE_CAPACITY_OPTIMIZED"
  }

  service_role = aws_iam_role.batch_service_role.arn
  type         = "MANAGED"
  depends_on   = [
    aws_iam_role_policy_attachment.batch_service_role_policy,
    aws_iam_role_policy_attachment.batch_service_ecs_policy_attachment
  ]
}

resource "aws_batch_job_queue" "spot_job_queue" {
  name     = "spotlake-job-queue-test"
  state    = "ENABLED"
  priority = 1
  
  compute_environment_order {
    order              = 1
    compute_environment = aws_batch_compute_environment.spot_compute_env.arn
  }
}

# ------ Batch Job Definitions ------

# Consolidated Collection Job
resource "aws_batch_job_definition" "collection_job" {
  name = "spotlake-collection-job-test"
  type = "container"

  container_properties = jsonencode({
    image = var.image_uri
    command = ["/bin/bash", "/app/collector/spot-dataset/aws/batch-test/scripts/run_collection.sh", "Ref::timestamp"]
    jobRoleArn = aws_iam_role.batch_job_role.arn
    environment = [
      { name = "S3_BUCKET", value = var.s3_bucket },
      { name = "AWS_REGION", value = var.aws_region }
    ]
    resourceRequirements = [
      { type = "VCPU", value = "2" },
      { type = "MEMORY", value = "2048" }
    ]
  })
  
  parameters = {
    "timestamp" = "placeholder"
  }
}

# Workload Generation Job
resource "aws_batch_job_definition" "workload_job" {
  name = "spotlake-workload-job-test"
  type = "container"

  container_properties = jsonencode({
    image = var.image_uri
    command = ["python3", "collector/spot-dataset/aws/batch-test/workload/generate_workload.py", "--timestamp", "Ref::timestamp"]
    jobRoleArn = aws_iam_role.batch_job_role.arn
    environment = [
      { name = "S3_BUCKET", value = var.s3_bucket },
      { name = "AWS_REGION", value = var.aws_region }
    ]
    resourceRequirements = [
      { type = "VCPU", value = "2" },
      { type = "MEMORY", value = "1024" }
    ]
  })
  
  parameters = {
    "timestamp" = "placeholder"
  }
}
