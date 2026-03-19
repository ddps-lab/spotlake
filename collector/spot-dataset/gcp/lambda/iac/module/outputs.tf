output "lambda_function_arn" {
  description = "Lambda function ARN."
  value       = aws_lambda_function.gcp_collector.arn
}

output "lambda_function_name" {
  description = "Lambda function name."
  value       = aws_lambda_function.gcp_collector.function_name
}

output "lambda_function_version" {
  description = "Published Lambda version."
  value       = aws_lambda_function.gcp_collector.version
}

output "lambda_alias_arn" {
  description = "Alias ARN (null when alias disabled)."
  value       = var.enable_alias ? aws_lambda_alias.prod[0].arn : null
}

output "schedule_rule_arn" {
  description = "EventBridge rule ARN (null when schedule disabled)."
  value       = var.manage_schedule ? aws_cloudwatch_event_rule.collector_schedule[0].arn : null
}

output "applied_layer_arns" {
  description = "Final list of layer ARNs attached to Lambda."
  value       = local.effective_layers
}

output "function_package_path" {
  description = "Local path to the prepared Lambda zip."
  value       = data.external.function_package.result.filename
}
