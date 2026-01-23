variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-west-2"
}

variable "vpc_id" {
  description = "VPC ID for Batch Compute Environment"
  type        = string
}

variable "s3_bucket" {
  description = "S3 bucket for SpotLake data"
  type        = string
  default     = "spotlake-test"
}

variable "image_uri" {
  description = "URI of the Docker image in ECR"
  type        = string
}

variable "subnet_ids" {
  description = "List of subnet IDs for Batch Compute Environment"
  type        = list(string)
}

variable "security_group_ids" {
  description = "List of security group IDs for Batch Compute Environment"
  type        = list(string)
}

# Slack Webhook URL for failure notifications (optional)
# If not provided, monitoring infrastructure will not be deployed
variable "slack_webhook_url" {
  description = "Slack Webhook URL for Batch job failure notifications (optional)"
  type        = string
  sensitive   = true
  default     = null
}

# Use existing Lambda function (shared monitoring)
variable "use_existing_lambda" {
  description = "Whether to use existing batch-failure-notifier Lambda (auto-detected by deploy script)"
  type        = bool
  default     = false
}
