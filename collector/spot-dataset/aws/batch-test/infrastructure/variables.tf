variable "aws_region" {
  description = "AWS Region"
  type        = string
  default     = "us-west-2"
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

variable "image_uri" {
  description = "Docker Image URI for Batch Jobs"
  type        = string
}

variable "job_role_arn" {
  description = "IAM Role ARN for Batch Jobs (if existing)"
  type        = string
  default     = null
}
