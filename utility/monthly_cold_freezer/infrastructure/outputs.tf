output "coordinator_job_definition" {
  value = aws_batch_job_definition.coordinator.name
}

output "aws_worker_job_definition" {
  value = aws_batch_job_definition.aws_worker.name
}

output "azure_worker_job_definition" {
  value = aws_batch_job_definition.azure_worker.name
}

output "gcp_worker_job_definition" {
  value = aws_batch_job_definition.gcp_worker.name
}

output "small_job_queue" {
  value = aws_batch_job_queue.freeze_small.name
}

output "heavy_job_queue" {
  value = aws_batch_job_queue.freeze_heavy.name
}
