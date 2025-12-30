# Azure Batch Collector Dockerfile
FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Install AWS CLI (for S3 and Batch interactions if needed)
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then \
    URL="https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip"; \
    elif [ "$ARCH" = "aarch64" ]; then \
    URL="https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip"; \
    else \
    echo "Unsupported architecture: $ARCH" && exit 1; \
    fi && \
    curl "$URL" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf awscliv2.zip aws

# Install Python dependencies
# Added azure-identity, azure-core, scikit-learn (for KMeans in SPS)
# boto3, pandas, numpy, pyyaml, requests are standard
RUN pip install --no-cache-dir \
    boto3 \
    pandas \
    numpy \
    pyyaml \
    requests \
    azure-identity \
    azure-core \
    scikit-learn

# Copy application code
# Assuming build context is the project root (spotlake/)
# We copy the entire azure/batch directory to /app/collector/spot-dataset/azure/batch
COPY collector/spot-dataset/azure/batch /app/collector/spot-dataset/azure/batch

# Make scripts executable
RUN chmod +x /app/collector/spot-dataset/azure/batch/scripts/run_collection.sh

# Set PYTHONPATH to include /app so imports work (e.g. from utils)
# Our modules are in collector/spot-dataset/azure/batch
# Scripts import 'utils' assuming parent dir is in path?
# No, scripts do sys.path.append(os.path.dirname(os.path.dirname(...))) but strictly setting PYTHONPATH is safer.
ENV PYTHONPATH=/app/collector/spot-dataset/azure/batch

WORKDIR /app

# Default entrypoint
CMD ["python3", "--version"]
