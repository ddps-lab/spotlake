

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

# Download and install spotinfo based on architecture
# Download and install spotinfo based on architecture
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

# Copy application code
# Assuming build context is the project root (spotlake/)
COPY collector/spot-dataset/aws/batch-test /app/collector/spot-dataset/aws/batch-test
# COPY const_config.py /app/
COPY utility /app/utility
RUN chmod +x /app/collector/spot-dataset/aws/batch-test/scripts/run_collection.sh

# Set PYTHONPATH to include /app so imports work
ENV PYTHONPATH=/app

# Default entrypoint (can be overridden)
CMD ["python3", "--version"]
