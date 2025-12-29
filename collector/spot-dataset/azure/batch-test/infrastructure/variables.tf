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
