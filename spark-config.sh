#!/bin/bash
# spark-config.sh - Configuration for the Spark job submission

# Default values
CATALOG_NAME="hive"
DATABASE_NAME="logs_db"
RAW_TABLE="raw_logs"
PROCESSED_TABLE="processed_logs"
DEPLOY_MODE="cluster"
K8S_NAMESPACE="spark"
SRC_DIR="./src"
ACCESS_KEYS="<TO BE FILLED>"
SECRET_KEYS="<To BE FILLED>"


# Spark configurations
SPARK_CONF=(
  "spark.kubernetes.namespace=$K8S_NAMESPACE"
  "spark.kubernetes.authenticate.driver.serviceAccountName=spark"
  "spark.kubernetes.container.image=apache/spark:3.5.1"
  "spark.executor.instances=2"
  "spark.kubernetes.driver.request.cores=1"
  "spark.kubernetes.executor.request.cores=1"
  "spark.driver.memory=2g"
  "spark.executor.memory=2g"
  "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
  "spark.sql.adaptive.enabled=true"
  "spark.sql.parquet.compression.codec=snappy"
  "spark.kubernetes.driver.podTemplateFile=driver-pod-template.yaml"
  "spark.hadoop.datanucleus.schema.autoCreateAll=true"
  "spark.hadoop.datanucleus.autoCreateSchema=true"
  "spark.hadoop.datanucleus.fixedDatastore=false"
  "spark.hadoop.datanucleus.schema.autoCreateTables=true"
  "spark.hadoop.fs.s3a.access.key=$ACCESS_KEYS"
  "spark.hadoop.fs.s3a.secret.key=$SECRET_KEYS"
)

# Spark packages
SPARK_PACKAGES="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,org.apache.hadoop:hadoop-aws:3.3.4"