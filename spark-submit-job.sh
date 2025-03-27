#!/bin/bash
# spark-submit-job.sh - Script to submit log analytics Spark job

# Load configuration
source ./spark-config.sh

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --input-path) INPUT_PATH="$2"; shift 2 ;;
    --output-path) OUTPUT_PATH="$2"; shift 2 ;;
    --catalog-name) CATALOG_NAME="$2"; shift 2 ;;
    --database-name) DATABASE_NAME="$2"; shift 2 ;;
    --raw-table) RAW_TABLE="$2"; shift 2 ;;
    --processed-table) PROCESSED_TABLE="$2"; shift 2 ;;
    --deploy-mode) DEPLOY_MODE="$2"; shift 2 ;;
    --k8s-master) K8S_MASTER="$2"; shift 2 ;;
    --k8s-namespace) K8S_NAMESPACE="$2"; shift 2 ;;
    --src-dir) SRC_DIR="$2"; shift 2 ;;
    --conf) EXTRA_CONF+=("$2"); shift 2 ;;
    --help) echo "Usage: $0 [options]"; exit 0 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

# Validate required parameters
if [ -z "$INPUT_PATH" ] || [ -z "$OUTPUT_PATH" ]; then
  echo "Error: --input-path and --output-path are required"
  exit 1
fi

# Check source directory
if [ ! -d "$SRC_DIR" ]; then
  echo "Error: Source directory '$SRC_DIR' does not exist"
  exit 1
fi

# Create src.zip package
echo "Creating source package from $SRC_DIR..."
cd "$SRC_DIR" || exit 1
zip -r src.zip . > /dev/null
cd - > /dev/null || exit 1
ZIP_FILE="$SRC_DIR/src.zip"

# Get K8S master if not provided
if [ -z "$K8S_MASTER" ]; then
  K8S_MASTER=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')
  if [ -z "$K8S_MASTER" ]; then
    echo "Error: --k8s-master is required or kubectl must be configured"
    exit 1
  fi
fi

# Extract S3 bucket from output path
S3_BUCKET=$(echo $OUTPUT_PATH | sed -E 's|s3://([^/]+)/.*|\1|')
FILE_UPLOAD_PATH="s3a://${S3_BUCKET}/spark-uploads"

echo "Submitting Spark job to Kubernetes..."

# Prepare catalog configuration
SPARK_CONF+=("spark.sql.catalog.$CATALOG_NAME=org.apache.iceberg.spark.SparkCatalog")
SPARK_CONF+=("spark.sql.catalog.$CATALOG_NAME.type=hive")
SPARK_CONF+=("spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem")
SPARK_CONF+=("spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
SPARK_CONF+=("spark.kubernetes.file.upload.path=$FILE_UPLOAD_PATH")

# Build conf parameters
CONF_PARAMS=()
for conf in "${SPARK_CONF[@]}"; do
  CONF_PARAMS+=("--conf" "$conf")
done

# Add extra configurations
for conf in "${EXTRA_CONF[@]}"; do
  CONF_PARAMS+=("--conf" "$conf")
done

# Submit the Spark job
spark-submit \
  --master "k8s://$K8S_MASTER" \
  --deploy-mode $DEPLOY_MODE \
  --name "IcebergLogAnalytics" \
  "${CONF_PARAMS[@]}" \
  --py-files "$ZIP_FILE" \
  --packages "$SPARK_PACKAGES" \
  $SRC_DIR/main.py \
  --input-path "$INPUT_PATH" \
  --output-path "$OUTPUT_PATH" \
  --catalog-name "$CATALOG_NAME" \
  --database-name "$DATABASE_NAME" \
  --raw-table-name "$RAW_TABLE" \
  --processed-table-name "$PROCESSED_TABLE"

# Check job submission status
if [ $? -eq 0 ]; then
  echo "Job submitted successfully to Kubernetes!"
else
  echo "Error submitting job to Kubernetes."
  exit 1
fi