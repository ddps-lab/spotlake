# build_and_push.ps1
param (
    [string]$Region = "us-west-2",
    [string]$Profile = $null
)

$ErrorActionPreference = "Stop"

$RepoName = "spotlake-batch-test"

if ($Profile) {
    $env:AWS_PROFILE = $Profile
    Write-Host "AWS Profile: $Profile"
}

$AccountId = aws sts get-caller-identity --query Account --output text
$ImageUri = "${AccountId}.dkr.ecr.${Region}.amazonaws.com/${RepoName}:latest"

Write-Host "Region: $Region"
Write-Host "Repository: $RepoName"
Write-Host "Account ID: $AccountId"
Write-Host "Image URI: $ImageUri"

# Create ECR repository if it doesn't exist
Write-Host "Checking ECR repository..."
try {
    aws ecr describe-repositories --repository-names "${RepoName}" --region "${Region}" | Out-Null
} catch {
    Write-Host "Repository not found, creating..."
    aws ecr create-repository --repository-name "${RepoName}" --region "${Region}" | Out-Null
}

# Login to ECR
Write-Host "Logging in to ECR..."
aws ecr get-login-password --region "${Region}" | docker login --username AWS --password-stdin "${AccountId}.dkr.ecr.${Region}.amazonaws.com"

# Build Docker image
Write-Host "Building Docker image..."
# Assuming script is run from project root
docker build -t "${RepoName}" -f collector/spot-dataset/aws/batch-test/Dockerfile collector/spot-dataset/aws/batch-test/

# Tag Docker image
Write-Host "Tagging Docker image..."
docker tag "${RepoName}:latest" "${ImageUri}"

# Push Docker image
Write-Host "Pushing Docker image..."
docker push "${ImageUri}"

Write-Host "Successfully built and pushed ${ImageUri}"
