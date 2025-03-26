#!/bin/bash

# spark-submit-job.sh - Script to submit log analytics Spark job

# Default values
INPUT_PATH=""
OUTPUT_PATH=""
CATALOG_NAME="hive_catalog"
DATABASE_NAME="logs_db"
RAW_TABLE="raw_logs"
PROCESSED_TABLE="processed_logs"
DEPLOY_MODE="cluster"
K8S_NAMESPACE="default"
K8S_MASTER="" # Kubernetes API server URL
EXTRA_CONF=() # Array to hold additional configurations

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --input-path)
      INPUT_PATH="$2"
      shift 2
      ;;
    --output-path)
      OUTPUT_PATH="$2"
      shift 2
      ;;
    --catalog-name)
      CATALOG_NAME="$2"
      shift 2
      ;;
    --database-name)
      DATABASE_NAME="$2"
      shift 2
      ;;
    --raw-table)
      RAW_TABLE="$2"
      shift 2
      ;;
    --processed-table)
      PROCESSED_TABLE="$2"
      shift 2
      ;;
    --deploy-mode)
      DEPLOY_MODE="$2"
      shift 2
      ;;
    --k8s-master)
      K8S_MASTER="$2"
      shift 2
      ;;
    --k8s-namespace)
      K8S_NAMESPACE="$2"
      shift 2
      ;;
    --conf)
      EXTRA_CONF+=("$2")
      shift 2
      ;;
    --help)
      echo "Usage: $0 [options]"
      # Help text here...
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Validate required parameters
if [ -z "$INPUT_PATH" ]; then
  echo "Error: --input-path is required"
  exit 1
fi

if [ -z "$OUTPUT_PATH" ]; then
  echo "Error: --output-path is required"
  exit 1
fi

if [ -z "$K8S_MASTER" ]; then
  # Try to get K8S master from current context
  K8S_MASTER=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')
  if [ -z "$K8S_MASTER" ]; then
    echo "Error: --k8s-master is required or kubectl must be configured"
    exit 1
  fi
fi

# Extract S3 bucket from output path for file uploads
S3_BUCKET=$(echo $OUTPUT_PATH | sed -E 's|s3://([^/]+)/.*|\1|')
FILE_UPLOAD_PATH="s3a://${S3_BUCKET}/spark-uploads"

echo "Submitting Spark job to Kubernetes with the following parameters:"
echo "  Kubernetes master: $K8S_MASTER"
echo "  Kubernetes namespace: $K8S_NAMESPACE"
echo "  Input path: $INPUT_PATH"
echo "  Output path: $OUTPUT_PATH"
echo "  File upload path: $FILE_UPLOAD_PATH"

# Build the conf array with all configurations
CONF_PARAMS=(
  "--conf" "spark.kubernetes.namespace=$K8S_NAMESPACE"
  "--conf" "spark.kubernetes.authenticate.driver.serviceAccountName=spark"
  "--conf" "spark.kubernetes.container.image=apache/spark:3.5.1"
  "--conf" "spark.executor.instances=3"
  "--conf" "spark.kubernetes.driver.request.cores=1"
  "--conf" "spark.kubernetes.executor.request.cores=1"
  "--conf" "spark.driver.memory=2g"
  "--conf" "spark.executor.memory=2g"
  "--conf" "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
  "--conf" "spark.sql.catalog.$CATALOG_NAME=org.apache.iceberg.spark.SparkCatalog"
  "--conf" "spark.sql.catalog.$CATALOG_NAME.type=hive"
  "--conf" "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem"
  "--conf" "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
  "--conf" "spark.sql.adaptive.enabled=true"
  "--conf" "spark.sql.parquet.compression.codec=snappy"
  "--conf" "spark.kubernetes.file.upload.path=$FILE_UPLOAD_PATH"
)

# Add any extra configurations passed via --conf
for conf in "${EXTRA_CONF[@]}"; do
  CONF_PARAMS+=("--conf" "$conf")
done

# Submit the Spark job to Kubernetes
spark-submit \
  --master "k8s://$K8S_MASTER" \
  --deploy-mode $DEPLOY_MODE \
  --name "IcebergLogAnalytics" \
  "${CONF_PARAMS[@]}" \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4 \
  src/main.py \
  --input-path "$INPUT_PATH" \
  --output-path "$OUTPUT_PATH" \
  --catalog-name "$CATALOG_NAME" \
  --database-name "$DATABASE_NAME" \
  --raw-table-name "$RAW_TABLE" \
  --processed-table-name "$PROCESSED_TABLE"

# Check if the job was submitted successfully
if [ $? -eq 0 ]; then
  echo "Job submitted successfully to Kubernetes!"
else
  echo "Error submitting job to Kubernetes."
  exit 1
fi