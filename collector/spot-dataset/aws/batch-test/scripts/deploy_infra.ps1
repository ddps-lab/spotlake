# deploy_infra.ps1
param (
    [Parameter(Mandatory=$true)][string]$VpcId,
    [Parameter(Mandatory=$true)][string]$SubnetIds,
    [Parameter(Mandatory=$true)][string]$SecurityGroupIds,
    [Parameter(Mandatory=$true)][string]$ImageUri,
    [string]$Region = "us-west-2",
    [string]$S3Bucket = "spotlake-test",
    [string]$Profile = $null
)

$ErrorActionPreference = "Stop"

if ($Profile) {
    $env:AWS_PROFILE = $Profile
}

Write-Host "Deploying Infrastructure..."
Set-Location "collector/spot-dataset/aws/batch-test/infrastructure"

Write-Host "Initializing Terraform..."
terraform init

Write-Host "Applying Terraform..."
# Note: SubnetIds and SecurityGroupIds should be passed as JSON strings, e.g. '["subnet-1", "subnet-2"]'
# PowerShell might require escaping quotes if passed directly, but if passed as string variable it should be fine.

terraform apply -auto-approve `
    -var "vpc_id=$VpcId" `
    -var "subnet_ids=$SubnetIds" `
    -var "security_group_ids=$SecurityGroupIds" `
    -var "image_uri=$ImageUri" `
    -var "aws_region=$Region" `
    -var "s3_bucket=$S3Bucket"

Write-Host "Deployment Complete."
