# AWS Infrastructure - Phase 1

# Providers
provider "aws" {
  region = var.region
}

# Fetch available AZs
data "aws_availability_zones" "available" {}

# Create VPC
resource "aws_vpc" "main" {
  cidr_block = var.vpc_cidr

  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = merge(
    {
      Name = "${var.environment}-vpc-eks"
    },
    var.tags
  )
}

# Create public subnets
resource "aws_subnet" "public_subnet" {
  count                   = 2
  vpc_id                  = aws_vpc.main.id
  cidr_block              = cidrsubnet(aws_vpc.main.cidr_block, 8, count.index)
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = true

  tags = merge(
    {
      Name                                        = "${var.environment}-public-subnet-${count.index}"
      "kubernetes.io/cluster/${var.cluster_name}" = "shared"
      "kubernetes.io/role/elb"                    = 1
    },
    var.tags
  )
}

# Create private subnets for RDS
resource "aws_subnet" "private_subnet" {
  count                   = 2
  vpc_id                  = aws_vpc.main.id
  cidr_block              = cidrsubnet(aws_vpc.main.cidr_block, 8, count.index + 10)
  availability_zone       = data.aws_availability_zones.available.names[count.index]
  map_public_ip_on_launch = false

  tags = merge(
    {
      Name = "${var.environment}-private-subnet-${count.index}"
    },
    var.tags
  )
}

# Internet Gateway
resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = merge(
    {
      Name = "${var.environment}-igw"
    },
    var.tags
  )
}

# Public Route Table
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = merge(
    {
      Name = "${var.environment}-route-table"
    },
    var.tags
  )
}

# Route Table Association for public subnets
resource "aws_route_table_association" "public" {
  count          = 2
  subnet_id      = aws_subnet.public_subnet[count.index].id
  route_table_id = aws_route_table.public.id
}

# Create DB subnet group for RDS
resource "aws_db_subnet_group" "hms_db" {
  name       = "${var.environment}-hms-db-subnet-group"
  subnet_ids = aws_subnet.public_subnet[*].id  # Using public subnets for simplicity

  tags = merge(
    {
      Name = "${var.environment} HMS DB Subnet Group"
    },
    var.tags
  )
}

# Create security group for RDS
resource "aws_security_group" "rds_sg" {
  name        = "${var.environment}-rds-sg"
  description = "Allow PostgreSQL traffic"
  vpc_id      = aws_vpc.main.id

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]  # Consider restricting this in production
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = merge(
    {
      Name = "${var.environment}-rds-security-group"
    },
    var.tags
  )
}

# Create RDS PostgreSQL instance for Hive Metastore
resource "aws_db_instance" "hms_db" {
  identifier             = "${var.environment}-hms-instance"
  allocated_storage      = var.db_allocated_storage
  storage_type           = "gp2"
  engine                 = "postgres"
  engine_version         = "15.10"
  instance_class         = var.db_instance_class
  db_name                = "hivemetastore"
  username               = var.db_username
  password               = var.db_password
  parameter_group_name   = "default.postgres15"
  db_subnet_group_name   = aws_db_subnet_group.hms_db.name
  vpc_security_group_ids = [aws_security_group.rds_sg.id]
  publicly_accessible    = true
  skip_final_snapshot    = true

  tags = merge(
    {
      Name = "${var.environment}-HMS-Database"
    },
    var.tags
  )
}

# Create S3 bucket for Hive warehouse and Iceberg data
resource "aws_s3_bucket" "data_bucket" {
  bucket = var.s3_bucket_name
  force_destroy = true

  tags = merge(
    {
      Name = "${var.environment}-Data-Lake-Storage"
    },
    var.tags
  )
}

# S3 bucket ownership controls
resource "aws_s3_bucket_ownership_controls" "data_bucket_ownership" {
  bucket = aws_s3_bucket.data_bucket.id
  rule {
    object_ownership = "BucketOwnerPreferred"
  }
}

# S3 bucket public access block
resource "aws_s3_bucket_public_access_block" "data_bucket_access" {
  bucket = aws_s3_bucket.data_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# EKS Cluster Module
module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.0"

  cluster_name    = var.cluster_name
  cluster_version = var.cluster_version

  cluster_endpoint_public_access = true
  enable_cluster_creator_admin_permissions = true

  vpc_id     = aws_vpc.main.id
  subnet_ids = aws_subnet.public_subnet[*].id

  eks_managed_node_groups = {
    main = {
      instance_types = var.node_instance_types
      min_size       = var.node_group_min_size
      max_size       = var.node_group_max_size
      desired_size   = var.node_group_desired_size
    }
  }

  tags = merge(
    {
      Environment = var.environment
      Terraform   = "true"
    },
    var.tags
  )
}

# IAM policy for S3 access
resource "aws_iam_policy" "s3_access_policy" {
  name        = "${var.environment}-EKS-S3-Access-Policy"
  description = "Policy to allow EKS nodes to access S3"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:*"
        ],
        Resource = [
          "${aws_s3_bucket.data_bucket.arn}",
          "${aws_s3_bucket.data_bucket.arn}/*"
        ]
      }
    ]
  })
}

# Attach S3 policy to EKS node role
resource "aws_iam_role_policy_attachment" "s3_access_attachment" {
  for_each = module.eks.eks_managed_node_groups

  policy_arn = aws_iam_policy.s3_access_policy.arn
  role       = each.value.iam_role_name
}

# Generate kubeconfig file and setup script
resource "local_file" "kubeconfig_setup" {
  depends_on = [module.eks]

  filename = "${path.module}/setup-kubeconfig.sh"
  content  = <<-EOT
    #!/bin/bash

    echo "Configuring kubectl for EKS cluster: ${module.eks.cluster_name}"
    aws eks update-kubeconfig --region ${var.region} --name ${module.eks.cluster_name}

    echo "Creating namespace: ${var.spark_namespace}"
    kubectl create namespace ${var.spark_namespace} --dry-run=client -o yaml | kubectl apply -f -

    echo "Configuration complete. Run the following to proceed with Phase 2:"
    echo "terraform -chdir=kubernetes apply -var=\"eks_cluster_endpoint=${module.eks.cluster_endpoint}\" -var=\"eks_ca_data=${module.eks.cluster_certificate_authority_data}\" -var=\"cluster_name=${module.eks.cluster_name}\""
  EOT

  file_permission = "0755"
}

# Output AWS infrastructure details
output "eks_cluster_endpoint" {
  description = "Endpoint for EKS cluster"
  value       = module.eks.cluster_endpoint
}

output "eks_cluster_name" {
  description = "Name of the EKS cluster"
  value       = module.eks.cluster_name
}

output "eks_cluster_certificate_authority_data" {
  description = "Certificate authority data for the EKS cluster"
  value       = module.eks.cluster_certificate_authority_data
  sensitive   = true
}

output "rds_endpoint" {
  description = "RDS endpoint for the Hive metastore database"
  value       = aws_db_instance.hms_db.address
}

output "rds_port" {
  description = "RDS port"
  value       = aws_db_instance.hms_db.port
}

output "rds_database" {
  description = "RDS database name"
  value       = aws_db_instance.hms_db.db_name
}

output "s3_bucket_name" {
  description = "Name of the S3 bucket created for data storage"
  value       = aws_s3_bucket.data_bucket.bucket
}

output "next_steps" {
  description = "Instructions for next steps"
  value       = "Run the setup-kubeconfig.sh script to configure kubectl, then run terraform for the kubernetes module"
}