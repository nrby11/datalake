# Data Lake Deployment Guide

This guide walks you through deploying a data lake with Apache Spark, Hive Metastore, and Iceberg using a two-phase approach.

## Prerequisites

1. AWS CLI installed and configured with appropriate permissions
2. Terraform installed (v1.0.0+)
3. kubectl installed
4. A unique S3 bucket name

## Preparing Your Environment

1. Create a directory structure:
   ```bash
   mkdir -p data-lake-deployment/kubernetes
   cd data-lake-deployment
   ```

2. Copy all provided terraform files into their respective directories:
   - `main.tf`, `variables.tf`, and `terraform.tfvars` in the root directory
   - `kubernetes/main.tf` and `kubernetes/variables.tf` in the kubernetes subdirectory

3. Edit `terraform.tfvars` to customize your deployment:
   - Set a unique `s3_bucket_name`
   - Configure credentials, regions, and other settings

## Phase 1: Deploy AWS Infrastructure

1. Initialize Terraform in the root directory:
   ```bash
   terraform init
   ```

2. Review the plan:
   ```bash
   terraform plan
   ```

3. Apply the configuration to create AWS resources:
   ```bash
   terraform apply
   ```

4. When complete, Terraform will create a script called `setup-kubeconfig.sh`. Run it to configure kubectl:
   ```bash
   ./setup-kubeconfig.sh
   ```

## Phase 2: Deploy Kubernetes Resources

1. Gather outputs from Phase 1:
   ```bash
   terraform output
   ```

2. Create a file `kubernetes/terraform.tfvars` with the following content:
   ```hcl
   eks_cluster_endpoint = "https://your-cluster-endpoint.eks.amazonaws.com"
   eks_ca_data          = "your-base64-encoded-ca-data"
   cluster_name         = "data-lake"
   region               = "us-east-1"
   
   # Copy other variables from the root terraform.tfvars
   environment         = "dev"
   spark_namespace     = "spark"
   s3_bucket_name      = "your-unique-data-lake-bucket-name"
   db_username         = "hiveuser"
   db_password         = "YourStrongPassword123!"
   
   # From the RDS output
   rds_endpoint        = "your-rds-endpoint.region.rds.amazonaws.com"
   rds_port            = "5432"
   rds_database        = "hivemetastore"
   
   # Other settings
   aws_auth_method     = "iam_role"
   spark_worker_count  = 2
   spark_worker_memory = "2G"
   spark_worker_cores  = "2"
   enable_history_server = true
   ```

3. Initialize Terraform in the kubernetes directory:
   ```bash
   cd kubernetes
   terraform init
   ```

4. Review the plan:
   ```bash
   terraform plan
   ```

5. Apply the configuration to deploy Kubernetes resources:
   ```bash
   terraform apply
   ```

6. When complete, check the deployment:
   ```bash
   kubectl get pods -n spark
   ```

## Accessing Your Data Lake

1. Forward Spark UI to your local machine:
   ```bash
   kubectl port-forward svc/spark-master 8080:8080 -n spark
   ```

2. Open a web browser and navigate to: http://localhost:8080

3. To run Spark SQL and interact with Iceberg tables:
   ```bash
   kubectl run spark-shell --namespace=spark --rm -it \
     --image=bitnami/spark:3.5.1 \
     --command -- spark-shell --master spark://spark-master:7077
   ```

4. Inside the spark-shell, create and query tables:
   ```scala
   spark.sql("CREATE TABLE hive.default.users (id INT, name STRING) USING iceberg")
   spark.sql("INSERT INTO hive.default.users VALUES (1, 'Alice'), (2, 'Bob')")
   spark.sql("SELECT * FROM hive.default.users").show()
   ```

## Troubleshooting

1. Check Hive Metastore logs:
   ```bash
   kubectl logs -n spark -l app=hive-metastore
   ```

2. Check Spark Master logs:
   ```bash
   kubectl logs -n spark -l app=spark-master
   ```

3. If the Iceberg catalog setup job fails, check its logs:
   ```bash
   kubectl logs -n spark job/iceberg-catalog-setup
   ```