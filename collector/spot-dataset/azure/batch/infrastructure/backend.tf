terraform {
  backend "s3" {
    bucket       = "spotlake-terraform-state"
    key          = "spotlake/azure-collector/terraform.tfstate"
    region       = "us-west-2"
    encrypt      = true
    use_lockfile = true
  }
}
