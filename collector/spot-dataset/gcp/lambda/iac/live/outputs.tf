output "lambda_function_arn" {
  value = module.collector.lambda_function_arn
}

output "lambda_function_name" {
  value = module.collector.lambda_function_name
}

output "lambda_function_version" {
  value = module.collector.lambda_function_version
}

output "applied_layer_arns" {
  value = module.collector.applied_layer_arns
}
