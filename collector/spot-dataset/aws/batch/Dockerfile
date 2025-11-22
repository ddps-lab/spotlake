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
# We assume the build context is collector/spot-dataset/aws/batch/ or root?
# If root, we copy specific dirs.
# Let's assume build context is collector/spot-dataset/aws/batch/
COPY . /app/

# Set PYTHONPATH
ENV PYTHONPATH=/app

# Default entrypoint (can be overridden)
CMD ["python3", "--version"]
