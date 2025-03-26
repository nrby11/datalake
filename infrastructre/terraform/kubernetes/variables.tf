variable "eks_cluster_endpoint" {
  description = "EKS cluster endpoint"
  type        = string
}

variable "eks_ca_data" {
  description = "EKS cluster CA certificate data"
  type        = string
}

variable "cluster_name" {
  description = "EKS cluster name"
  type        = string
}

variable "region" {
  description = "AWS region"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
}

variable "spark_namespace" {
  description = "Kubernetes namespace for Spark resources"
  type        = string
  default     = "spark"
}

variable "s3_bucket_name" {
  description = "S3 bucket name for data storage"
  type        = string
}

variable "db_username" {
  description = "Database username"
  type        = string
  sensitive   = true
}

variable "db_password" {
  description = "Database password"
  type        = string
  sensitive   = true
}

variable "rds_endpoint" {
  description = "RDS endpoint"
  type        = string
}

variable "rds_port" {
  description = "RDS port"
  type        = string
  default     = "5432"
}

variable "rds_database" {
  description = "RDS database name"
  type        = string
  default     = "hivemetastore"
}

variable "aws_auth_method" {
  description = "AWS authentication method ('keys' or 'iam_role')"
  type        = string
  default     = "iam_role"
}

variable "aws_access_key" {
  description = "AWS access key (only if aws_auth_method = 'keys')"
  type        = string
  sensitive   = true
}

variable "aws_secret_key" {
  description = "AWS secret key (only if aws_auth_method = 'keys')"
  type        = string
  sensitive   = true
}

variable "spark_worker_count" {
  description = "Number of Spark workers"
  type        = number
  default     = 2
}

variable "spark_worker_memory" {
  description = "Memory for each Spark worker"
  type        = string
  default     = "2G"
}

variable "spark_worker_cores" {
  description = "CPU cores for each Spark worker"
  type        = string
  default     = "2"
}

variable "enable_history_server" {
  description = "Whether to enable Spark History Server"
  type        = bool
  default     = false
}

variable "tags" {
  description = "Additional resource tags"
  type        = map(string)
  default     = {}
}