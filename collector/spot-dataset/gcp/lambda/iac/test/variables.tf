variable "credential_file" {
  description = "Local path to the GCP service-account JSON."
  type        = string

  validation {
    condition     = var.credential_file != "" && fileexists(var.credential_file)
    error_message = "credential_file must point to an existing local JSON file."
  }
}

variable "error_notification_slack_webhook_url" {
  description = "Slack webhook used by the collector."
  type        = string
  sensitive   = true

  validation {
    condition     = var.error_notification_slack_webhook_url != ""
    error_message = "error_notification_slack_webhook_url must not be empty."
  }
}
