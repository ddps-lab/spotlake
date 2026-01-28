# Unified Docker Image for AWS and Azure Batch Collectors
FROM python:3.12-slim

ARG AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY

ENV AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
ENV AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY

WORKDIR /app

# Install system dependencies (procps for memory monitoring in test environments)
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Install AWS CLI based on architecture
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

# Download and install spotinfo (for AWS) based on architecture
RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then \
    URL="https://github.com/alexei-led/spotinfo/releases/download/2.2.0/spotinfo_linux_amd64"; \
    elif [ "$ARCH" = "aarch64" ]; then \
    URL="https://github.com/alexei-led/spotinfo/releases/download/2.2.0/spotinfo_linux_arm64"; \
    else \
    echo "Unsupported architecture: $ARCH" && exit 1; \
    fi && \
    curl -L $URL -o /usr/local/bin/spotinfo && \
    chmod +x /usr/local/bin/spotinfo

# Install Python dependencies (for both AWS and Azure)
RUN pip install --no-cache-dir \
    boto3 \
    pandas \
    numpy \
    pyyaml \
    ortools \
    requests \
    azure-identity \
    azure-core \
    scikit-learn \
    polars>=1.37.0 \
    pyarrow

# Copy utility module (shared by both AWS and Azure)
COPY utility /app/utility

# Copy titans_common module (TITANS Hot/Warm tier integration)
COPY collector/titans_common /app/collector/titans_common

# Copy AWS collector code
COPY collector/spot-dataset/aws/batch /app/collector/spot-dataset/aws/batch
RUN chmod +x /app/collector/spot-dataset/aws/batch/scripts/run_collection.sh

# Copy Azure collector code
COPY collector/spot-dataset/azure/batch /app/collector/spot-dataset/azure/batch
RUN chmod +x /app/collector/spot-dataset/azure/batch/scripts/run_collection.sh

# Set PYTHONPATH to include /app so imports work
ENV PYTHONPATH=/app

# Default entrypoint (can be overridden)
CMD ["python3", "--version"]
