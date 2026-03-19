variable "aws_region" {
  description = "AWS region for the Terraform state bucket."
  type        = string
  default     = "us-west-2"
}

variable "bucket_name" {
  description = "S3 bucket name for Terraform state."
  type        = string
  default     = "spotlake-terraform-state"
}

variable "tags" {
  description = "Tags applied to bootstrap resources."
  type        = map(string)
  default = {
    Service = "spotlake-terraform-state"
    Managed = "terraform"
  }
}
