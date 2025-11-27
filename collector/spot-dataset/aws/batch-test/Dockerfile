# Stage 1: Get spotinfo binary
FROM ghcr.io/alexei-led/spotinfo:latest AS spotinfo-source

# Stage 2: Final image
FROM python:3.12-slim

ARG AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY

ENV AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
ENV AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir \
    boto3 \
    pandas \
    numpy \
    pyyaml \
    ortools \
    requests

# Copy spotinfo from builder
COPY --from=spotinfo-source /spotinfo /usr/local/bin/spotinfo

# Copy application code
# Assuming build context is the project root (spotlake/)
COPY collector/spot-dataset/aws/batch-test /app/collector/spot-dataset/aws/batch-test
# COPY const_config.py /app/
COPY utility /app/utility

# Set PYTHONPATH to include /app so imports work
ENV PYTHONPATH=/app

# Default entrypoint (can be overridden)
CMD ["python3", "--version"]
