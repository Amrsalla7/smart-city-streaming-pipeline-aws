FROM public.ecr.aws/bitnami/spark:latest

# Step 1: Use root for setup
USER root

# Install dependencies
RUN install_packages curl python3 python3-pip

# Create sparkuser with fixed UID 1001 (matches Bitnami base image)
RUN useradd -u 1001 -ms /bin/bash sparkuser || true \
    && mkdir -p /tmp/.ivy2 \
    && mkdir -p /opt/bitnami/spark/jars \
    && chown -R sparkuser:root /tmp /opt/bitnami/spark

# Step 2: Download Hadoop-AWS and AWS SDK v2 JARs (required for S3A)
RUN curl -L -o /opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o /opt/bitnami/spark/jars/software.amazon.awssdk.bundle-2.25.1.jar \
      https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.25.1/bundle-2.25.1.jar




# Step 3: Switch to non-root user
USER sparkuser

# Environment variables
ENV SPARK_LOCAL_DIRS=/tmp \
    SPARK_JARS_IVY=/tmp/.ivy2 \
    HADOOP_USER_NAME=sparkuser \
    USER=sparkuser \
    HOME=/home/sparkuser

# Set working directory
WORKDIR /opt/bitnami/spark

# Step 4: Install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Step 5: Copy Spark job files
COPY jobs /opt/bitnami/spark/jobs

# Default command
CMD ["bash"]
