Below is the README.md content in a plain text format that you can copy easily:

---

```markdown
# Data Lake Deployment Guide

This guide walks you through deploying a data lake with Apache Spark, Hive Metastore, and Iceberg using a two-phase approach.

## Prerequisites

1. AWS CLI installed and configured with appropriate permissions
2. Terraform installed (v1.0.0+)
3. kubectl installed
4. A unique S3 bucket name

## AWS Authentication

This project requires AWS credentials for accessing S3 storage, RDS, and Glue resources. Follow these steps to set up authentication:

### AWS Access Keys Setup

1. Create an IAM user with the following permissions:
   - AmazonS3FullAccess
   - AmazonRDSFullAccess
   - AmazonEKSClusterPolicy
   - AmazonEKSWorkerNodePolicy
   - AmazonEC2ContainerRegistryReadOnly

2. Generate an access key and secret key for this user.

3. Set these credentials in the project by searching for the following and replacing the key appropriately  :
<name>fs.s3a.access.key</name>
<name>fs.s3a.secret.key</name>
    "spark.hadoop.fs.s3a.access.key",""
    "spark.hadoop.fs.s3a.secret.key", ""
```
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

3. Initialize Terraform in the `kubernetes` directory:

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

## Running Spark Jobs and Using Spark CLI

After deployment, follow these steps to run jobs and access the Spark CLI:

### Running a Spark Job

Use the provided submission script by specifying your S3 paths and Kubernetes master:

```bash
./spark-submit-job.sh \
  --input-path "s3a://your-bucket-name/test-data/Simulated Logs.log" \
  --output-path "s3a://your-bucket-name/output" \
  --k8s-master "https://your-eks-cluster-endpoint.eks.amazonaws.com" \
  --k8s-namespace "spark"
```

You can get your EKS cluster endpoint from the Terraform outputs.

### Using Spark CLI

To use the Spark query CLI, set up port forwarding to access the services:

1. Forward the Spark Master port:

   ```bash
   kubectl port-forward service/spark-master 7077:7077 -n spark
   ```

2. Forward the Hive Metastore port in a separate terminal:

   ```bash
   kubectl port-forward service/hive-metastore 9083:9083 -n spark
   ```

3. With port forwarding active, run the Spark query CLI:

   ```bash
   python spark_query_cli.py
   ```

This setup allows you to interactively query your Iceberg tables and analyze data.



# Performance and Scalability

## Processing Capacity:

**Default configuration (2 workers with 2 cores each) can process:**

- **100K logs** in ~2 minutes
- **500K logs** in ~10 minutes
- **1M logs** in ~20-25 minutes

## Storage Scalability:

- **S3-based data lake** provides virtually unlimited storage.
- **Iceberg table format** supports efficient handling of large datasets.

## Compute Scalability:

- Spark worker count can be adjusted by modifying `spark_worker_count`.
- Worker resources can be increased by changing `spark_worker_memory` and `spark_worker_cores`.

## Optimal Settings for 1M Logs (~5-7 minutes):

- **4 workers** with **4 cores each**
- **8GB memory per worker**
- **20-40 partitions** for processing

## Bottlenecks to Watch:

- Network bandwidth between EKS and S3.
- Memory pressure during analytics operations.
- Small file issues with high-volume log processing.

---