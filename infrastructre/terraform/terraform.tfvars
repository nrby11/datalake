# Basic configuration
region          = "us-east-1"
environment     = "dev"
cluster_name    = "data-lake"
cluster_version = "1.30"
vpc_cidr        = "10.0.0.0/16"

# S3 bucket name must be globally unique
s3_bucket_name  = ""

# Database configuration
db_username        = "hiveuser"
db_password        = "hmsuser123"  # Replace with a secure password
db_instance_class  = "db.t3.micro"
db_allocated_storage = 20

# EKS node configuration
node_instance_types   = ["t3.medium"]
node_group_min_size   = 1
node_group_max_size   = 3
node_group_desired_size = 2

# Spark configuration
spark_namespace    = "spark"
spark_worker_count = 3
spark_worker_memory = "2G"
spark_worker_cores = "2"
enable_history_server = false

# AWS authentication method (keys or iam_role)
aws_auth_method = "iam_role"

# Only needed if aws_auth_method = "keys"
# aws_access_key = ""
# aws_secret_key = ""

# Additional tags
tags = {
  Project     = "Granica Interview"
  Owner       = "Nirbhay"
  ManagedBy   = "Nirbhay"
}


