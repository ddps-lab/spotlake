variable "aws_region" {
  description = "AWS region for Batch resources"
  type        = string
  default     = "us-west-2"
}

variable "aws_profile" {
  description = "AWS CLI profile used for local Terraform runs"
  type        = string
  default     = "spotrank"
}

variable "environment" {
  description = "Deployment environment name"
  type        = string
  default     = "production"
}

variable "name_prefix" {
  description = "Base resource prefix for monthly cold freezer"
  type        = string
  default     = "monthly-cold-freezer"
}

variable "vpc_id" {
  description = "VPC ID for Batch compute environments"
  type        = string
}

variable "subnet_ids" {
  description = "Subnet IDs for Batch compute environments"
  type        = list(string)
}

variable "security_group_ids" {
  description = "Security group IDs for Batch compute environments"
  type        = list(string)
}

variable "image_uri" {
  description = "Container image URI for monthly freezer jobs"
  type        = string
}

variable "titans_bucket" {
  description = "S3 bucket for TITANS warm/cold data"
  type        = string
  default     = "titans-spotlake-data"
}

variable "raw_bucket" {
  description = "S3 bucket for SpotLake raw CSV snapshots"
  type        = string
  default     = "spotlake"
}

variable "freeze_env" {
  description = "FREEZE_ENV passed to the jobs"
  type        = string
  default     = "production"
}

variable "small_max_vcpus" {
  description = "Max vCPU capacity for the small compute environment"
  type        = number
  default     = 8
}

variable "heavy_max_vcpus" {
  description = "Max vCPU capacity for the heavy compute environment"
  type        = number
  default     = 32
}

variable "enable_schedule" {
  description = "Whether to create the EventBridge schedule for the coordinator"
  type        = bool
  default     = true
}
