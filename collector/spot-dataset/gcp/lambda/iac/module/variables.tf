variable "aws_region" {
  description = "AWS region where the Lambda is deployed."
  type        = string
}

variable "function_name" {
  description = "Lambda function name."
  type        = string
}

variable "lambda_role_arn" {
  description = "Execution role ARN for the Lambda."
  type        = string
}

variable "credential_file" {
  description = "Local path to the GCP service-account JSON used during packaging."
  type        = string

  validation {
    condition     = var.credential_file != ""
    error_message = "credential_file must point to a local JSON file."
  }
}

variable "handler" {
  description = "Lambda handler."
  type        = string
  default     = "lambda_function.lambda_handler"
}

variable "runtime" {
  description = "Lambda runtime."
  type        = string

  validation {
    condition     = contains(["python3.9", "python3.12"], var.runtime)
    error_message = "runtime must be one of: python3.9, python3.12."
  }
}

variable "architecture" {
  description = "Lambda architecture."
  type        = string

  validation {
    condition     = contains(["x86_64", "arm64"], var.architecture)
    error_message = "architecture must be one of: x86_64, arm64."
  }
}

variable "timeout_seconds" {
  description = "Lambda timeout in seconds."
  type        = number
  default     = 600
}

variable "memory_mb" {
  description = "Lambda memory size in MB."
  type        = number
  default     = 384
}

variable "ephemeral_storage_mb" {
  description = "Lambda ephemeral storage size in MB."
  type        = number
  default     = 512
}

variable "build_base_layer" {
  description = "Build and attach the slim GCP base layer."
  type        = bool
  default     = true
}

variable "build_titans_layer" {
  description = "Build and attach the Polars/TITANS dependency layer."
  type        = bool
  default     = false
}

variable "base_layer_name" {
  description = "Optional override for the base layer name."
  type        = string
  default     = ""
}

variable "titans_layer_name" {
  description = "Optional override for the TITANS layer name."
  type        = string
  default     = ""
}

variable "base_layer_packages" {
  description = "Packages installed into the base layer."
  type        = string
  default     = "requests==2.32.5 google-auth"
}

variable "titans_layer_packages" {
  description = "Packages installed into the Polars/TITANS layer."
  type        = string
  default     = "polars==1.37.0"
}

variable "environment_variables" {
  description = "Environment variables for the function."
  type        = map(string)
  default     = {}
}

variable "titans_enabled" {
  description = "Enable TITANS pipeline in collector."
  type        = bool
  default     = false
}

variable "titans_env" {
  description = "TITANS environment (production/test)."
  type        = string
  default     = "production"

  validation {
    condition     = contains(["production", "test"], var.titans_env)
    error_message = "titans_env must be production or test."
  }
}

variable "compute_api_backend" {
  description = "GCP machine type inventory backend."
  type        = string
  default     = "sdk"

  validation {
    condition     = contains(["sdk", "rest"], var.compute_api_backend)
    error_message = "compute_api_backend must be sdk or rest."
  }
}

variable "publish_new_version" {
  description = "Publish a new Lambda version on update."
  type        = bool
  default     = true
}

variable "enable_alias" {
  description = "Create/manage a stable alias."
  type        = bool
  default     = false
}

variable "alias_name" {
  description = "Alias name used for stable traffic routing."
  type        = string
  default     = "prod"
}

variable "manage_schedule" {
  description = "Create/manage EventBridge schedule for this stack."
  type        = bool
  default     = false
}

variable "schedule_rule_name" {
  description = "EventBridge rule name when manage_schedule=true."
  type        = string
}

variable "schedule_expression" {
  description = "EventBridge cron/rate expression."
  type        = string
  default     = "cron(0 */1 * * ? *)"
}

variable "schedule_state" {
  description = "EventBridge rule state."
  type        = string
  default     = "DISABLED"

  validation {
    condition     = contains(["ENABLED", "DISABLED"], var.schedule_state)
    error_message = "schedule_state must be ENABLED or DISABLED."
  }
}

variable "schedule_target_id" {
  description = "EventBridge target id."
  type        = string
  default     = "gcp-collector-hourly-target"
}

variable "schedule_permission_statement_id" {
  description = "Lambda permission statement id for EventBridge."
  type        = string
}

variable "tags" {
  description = "Tags applied to resources."
  type        = map(string)
  default     = {}
}
