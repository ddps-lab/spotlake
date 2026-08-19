output "bucket_name" {
  description = "Terraform state bucket name."
  value       = aws_s3_bucket.terraform_state.bucket
}

output "backend_summary" {
  description = "Backend targets used by the live/test Terraform roots."
  value       = <<-EOT
live backend:
  bucket = "${aws_s3_bucket.terraform_state.bucket}"
  key    = "spotlake/gcp-collector/terraform.tfstate"

test backend:
  bucket = "${aws_s3_bucket.terraform_state.bucket}"
  key    = "spotlake/gcp-collector-test/terraform.tfstate"
  EOT
}
