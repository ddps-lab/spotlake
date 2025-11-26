FROM python:3.9-slim

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

# Install spotinfo
RUN curl -L https://github.com/alexei-led/spotinfo/releases/download/v1.0.7/spotinfo_1.0.7_linux_amd64.tar.gz -o spotinfo.tar.gz \
    && tar -xzf spotinfo.tar.gz \
    && mv spotinfo /usr/local/bin/ \
    && rm spotinfo.tar.gz LICENSE README.md

# Copy application code
# Assuming build context is the project root (spotlake/)
COPY collector/spot-dataset/aws/batch-test /app/collector/spot-dataset/aws/batch-test
COPY const_config.py /app/
COPY utility /app/utility

# Set PYTHONPATH to include /app so imports work
ENV PYTHONPATH=/app

# Default entrypoint (can be overridden)
CMD ["python3", "--version"]
