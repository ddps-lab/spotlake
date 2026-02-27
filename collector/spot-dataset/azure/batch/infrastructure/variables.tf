variable "aws_region" {
  description = "AWS Region"
  type        = string
  default     = "us-west-2"
}

variable "dynamodb_region" {
  description = "AWS Region for DynamoDB tables (AzureAuth, azure)"
  type        = string
  default     = "us-east-1"
}

variable "vpc_id" {
  description = "VPC ID for Batch Compute Environment"
  type        = string
}

variable "subnet_ids" {
  description = "Subnet IDs for Batch Compute Environment"
  type        = list(string)
}

variable "security_group_ids" {
  description = "Security Group IDs for Batch Compute Environment"
  type        = list(string)
}

variable "s3_bucket" {
  description = "S3 Bucket for SpotLake data"
  type        = string
  default     = "spotlake"
}

variable "titans_bucket" {
  description = "S3 Bucket for TITANS Hot/Warm tier parquet data"
  type        = string
  default     = "titans-spotlake-data"
}

variable "image_uri" {
  description = "Docker Image URI for Batch Jobs"
  type        = string
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
variable "titans_enabled" {
  description = "Enable TITANS Hot/Warm tier upload (0=off, 1=on)"
  type        = string
  default     = "1"
}

variable "use_existing_lambda" {
  description = "Whether to use existing batch-failure-notifier Lambda (auto-detected by deploy script)"
  type        = bool
  default     = false
}
