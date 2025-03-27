variable "region" {
  description = "The AWS region to deploy resources into"
  type        = string
  default     = "us-east-1"
}

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "cluster_name" {
  description = "Name of the EKS cluster"
  type        = string
  default     = "data-lake-cluster"
}

variable "cluster_version" {
  description = "Kubernetes version to use for the EKS cluster"
  type        = string
  default     = "1.30"
}

variable "environment" {
  description = "Environment name (e.g., dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "spark_namespace" {
  description = "Kubernetes namespace for Spark resources"
  type        = string
  default     = "default"
}

variable "s3_bucket_name" {
  description = "Name of the S3 bucket for data storage"
  type        = string
  default     = "datalake-test-bucket"
}

variable "db_instance_class" {
  description = "RDS instance class"
  type        = string
  default     = "db.t3.micro"
}

variable "db_allocated_storage" {
  description = "Allocated storage for RDS in GB"
  type        = number
  default     = 1
}

variable "db_username" {
  description = "Username for the RDS instance"
  type        = string
  sensitive   = true
}

variable "db_password" {
  description = "Password for the RDS instance"
  type        = string
  sensitive   = true
}

variable "node_instance_types" {
  description = "EC2 instance types for EKS node group"
  type        = list(string)
  default     = ["t3.medium"]
}

variable "node_group_min_size" {
  description = "Minimum size of the EKS node group"
  type        = number
  default     = 1
}

variable "node_group_max_size" {
  description = "Maximum size of the EKS node group"
  type        = number
  default     = 3
}

variable "node_group_desired_size" {
  description = "Desired size of the EKS node group"
  type        = number
  default     = 2
}

variable "spark_worker_count" {
  description = "Number of Spark worker nodes"
  type        = number
  default     = 2
}

variable "spark_worker_memory" {
  description = "Memory allocation for each Spark worker"
  type        = string
  default     = "2G"
}

variable "spark_worker_cores" {
  description = "CPU cores allocation for each Spark worker"
  type        = string
  default     = "2"
}

variable "aws_auth_method" {
  description = "AWS authentication method: 'keys' or 'iam_role'"
  type        = string
  default     = "keys"
  validation {
    condition     = contains(["keys", "iam_role"], var.aws_auth_method)
    error_message = "Valid values for aws_auth_method are 'keys' or 'iam_role'."
  }
}

variable "aws_access_key" {
  description = "AWS access key (only needed if aws_auth_method = 'keys')"
  type        = string
  default     = ""
  sensitive   = true
}

variable "aws_secret_key" {
  description = "AWS secret key (only needed if aws_auth_method = 'keys')"
  type        = string
  default     = ""
  sensitive   = true
}

variable "enable_history_server" {
  description = "Whether to deploy Spark History Server"
  type        = bool
  default     = true
}

variable "tags" {
  description = "Additional tags for all resources"
  type        = map(string)
  default     = {}
}