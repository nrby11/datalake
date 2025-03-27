# Kubernetes Resources - Phase 2

# Configure Kubernetes provider with EKS cluster details
provider "kubernetes" {
  host                   = var.eks_cluster_endpoint
  cluster_ca_certificate = base64decode(var.eks_ca_data)

  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args        = ["eks", "get-token", "--region", var.region, "--cluster-name", var.cluster_name]
  }
}

# Helm provider configuration
provider "helm" {
  kubernetes {
    host                   = var.eks_cluster_endpoint
    cluster_ca_certificate = base64decode(var.eks_ca_data)

    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "aws"
      args        = ["eks", "get-token", "--region", var.region, "--cluster-name", var.cluster_name]
    }
  }
}

# Define AWS auth configuration based on selected method
locals {
  aws_auth_config = var.aws_auth_method == "keys" ? {
    access_key_config = <<-EOT
      <property>
          <name>fs.s3a.access.key</name>
          <value>${var.aws_access_key}</value>
      </property>
      <property>
          <name>fs.s3a.secret.key</name>
          <value>${var.aws_secret_key}</value>
      </property>
    EOT
    spark_s3_config = <<-EOT
      # S3 configuration with access keys
      spark.hadoop.fs.s3a.endpoint=s3.amazonaws.com
      spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
      spark.hadoop.fs.s3a.access.key=${var.aws_access_key}
      spark.hadoop.fs.s3a.secret.key=${var.aws_secret_key}
      spark.hadoop.fs.s3a.path.style.access=true

    EOT
    container_env = [
      {
        name  = "AWS_ACCESS_KEY_ID"
        value = var.aws_access_key
      },
      {
        name  = "AWS_SECRET_ACCESS_KEY"
        value = var.aws_secret_key
      }
    ]
  } : {
    access_key_config = <<-EOT
        <property>
          <name>fs.s3a.access.key</name>
          <value>${var.aws_access_key}</value>
        </property>
        <property>
            <name>fs.s3a.secret.key</name>
            <value>${var.aws_secret_key}</value>
        </property>
    EOT
    spark_s3_config = <<-EOT
      # S3 configuration with IAM role
      spark.hadoop.fs.s3a.endpoint=s3.amazonaws.com
      spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
      spark.hadoop.fs.s3a.path.style.access=true
      spark.hadoop.fs.s3a.access.key=${var.aws_access_key}
      spark.hadoop.fs.s3a.secret.key=${var.aws_secret_key}

    EOT
    container_env = []
  }
}

# Create Hive Metastore ConfigMap
resource "kubernetes_config_map" "hive_site_config" {
  metadata {
    name      = "hive-site-config"
    namespace = var.spark_namespace
  }

  data = {
    "hive-site.xml" = <<-EOT
      <?xml version="1.0" encoding="UTF-8"?>
      <configuration>
          <!-- PostgreSQL Connection Details -->
          <property>
              <name>javax.jdo.option.ConnectionURL</name>
              <value>jdbc:postgresql://${var.rds_endpoint}:${var.rds_port}/${var.rds_database}</value>
          </property>
          <property>
              <name>javax.jdo.option.ConnectionDriverName</name>
              <value>org.postgresql.Driver</value>
          </property>
          <property>
              <name>javax.jdo.option.ConnectionUserName</name>
              <value>${var.db_username}</value>
          </property>
          <property>
              <name>javax.jdo.option.ConnectionPassword</name>
              <value>${var.db_password}</value>
          </property>

          <!-- Hive Metastore URI -->
          <property>
              <name>hive.metastore.uris</name>
              <value>thrift://hive-metastore:9083</value>
          </property>

          <!-- Set S3 as the storage backend -->
          <property>
              <name>hive.metastore.warehouse.dir</name>
              <value>s3a://${var.s3_bucket_name}/hive/warehouse</value>
          </property>

          <!-- Schema Verification -->
          <property>
              <name>hive.metastore.schema.verification</name>
              <value>false</value>
          </property>
          <property>
              <name>datanucleus.schema.autoCreateAll</name>
              <value>true</value>
          </property>
          <property>
              <name>datanucleus.fixedDatastore</name>
              <value>false</value>
          </property>
          <property>
              <name>datanucleus.autoCreateSchema</name>
              <value>true</value>
          </property>

          <!-- Disable HDFS dependency -->
          <property>
              <name>fs.defaultFS</name>
              <value>file:///</value>
          </property>

          <!-- Enable AWS S3 access -->
          <property>
              <name>fs.s3a.endpoint</name>
              <value>s3.amazonaws.com</value>
          </property>
          ${local.aws_auth_config.access_key_config}
          <property>
              <name>fs.s3a.impl</name>
              <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
          </property>
          <property>
              <name>fs.s3a.path.style.access</name>
              <value>true</value>
          </property>
          <property>
              <name>fs.s3a.connection.ssl.enabled</name>
              <value>true</value>
          </property>
      </configuration>
    EOT
  }
}

# Create Hive Metastore Deployment
resource "kubernetes_deployment" "hive_metastore" {
  depends_on = [kubernetes_config_map.hive_site_config]

  metadata {
    name      = "hive-metastore"
    namespace = var.spark_namespace
    labels = {
      app = "hive-metastore"
    }
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "hive-metastore"
      }
    }

    template {
      metadata {
        labels = {
          app = "hive-metastore"
        }
      }

      spec {
        init_container {
          name  = "download-drivers"
          image = "curlimages/curl:latest"

          command = ["/bin/sh", "-c"]
          args = [<<-EOT
            # Using newer PostgreSQL driver (supports SCRAM-SHA-256)
            curl -L -o /drivers/postgresql.jar https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar

            # Add Iceberg runtime jar
            curl -L -o /drivers/iceberg-hive-runtime.jar https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-hive-runtime/1.4.0/iceberg-hive-runtime-1.4.0.jar

            # AWS libraries
            curl -L -o /drivers/hadoop-aws.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
            curl -L -o /drivers/aws-sdk.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.400/aws-java-sdk-bundle-1.12.400.jar

            echo "Downloaded drivers:"
            ls -la /drivers/
          EOT
          ]

          volume_mount {
            name       = "lib-volume"
            mount_path = "/drivers"
          }
        }

        container {
          name  = "hive-metastore"
          image = "apache/hive:4.0.0"

          security_context {
            run_as_user = 0  # Run as root to avoid permission issues
          }

          command = ["/bin/bash", "-c"]
          args = [<<-EOT
            # Create directory for external jars
            mkdir -p /opt/hive/auxlib

            # Copy the downloaded drivers
            cp /drivers/* /opt/hive/auxlib/

            # Set environment variables for external jars
            export HIVE_AUX_JARS_PATH=/opt/hive/auxlib

            # Initialize the schema
            echo "Initializing Hive metastore schema..."
            /opt/hive/bin/schematool -dbType postgres -initSchema

            echo "Schema initialization complete. Starting metastore service..."
            # Start the metastore
            /opt/hive/bin/hive --service metastore
          EOT
          ]

          port {
            container_port = 9083
          }

          env {
            name  = "HADOOP_CLASSPATH"
            value = "/opt/hive/auxlib/*:/opt/hadoop/share/hadoop/tools/lib/*"
          }

          # Add AWS credentials if using key-based auth
          dynamic "env" {
            for_each = local.aws_auth_config.container_env
            content {
              name  = env.value.name
              value = env.value.value
            }
          }

          volume_mount {
            name       = "hive-config-volume"
            mount_path = "/opt/hive/conf/hive-site.xml"
            sub_path   = "hive-site.xml"
          }

          volume_mount {
            name       = "lib-volume"
            mount_path = "/drivers"
          }
        }

        volume {
          name = "hive-config-volume"
          config_map {
            name = kubernetes_config_map.hive_site_config.metadata[0].name
          }
        }

        volume {
          name = "lib-volume"
          empty_dir {}
        }
      }
    }
  }
}

# Create Hive Metastore Service
resource "kubernetes_service" "hive_metastore" {
  depends_on = [kubernetes_deployment.hive_metastore]

  metadata {
    name      = "hive-metastore"
    namespace = var.spark_namespace
  }

  spec {
    selector = {
      app = kubernetes_deployment.hive_metastore.metadata[0].labels.app
    }

    port {
      port        = 9083
      target_port = 9083
    }
  }
}

# Create Spark ConfigMap with simplified catalog configuration
resource "kubernetes_config_map" "spark_conf" {
  depends_on = [kubernetes_service.hive_metastore]

  metadata {
    name      = "spark-conf"
    namespace = var.spark_namespace
  }

  data = {
    "spark-defaults.conf" = <<-EOT
      # Hive Metastore connection
      spark.sql.warehouse.dir=s3a://${var.s3_bucket_name}/hive/warehouse
      spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083

      # Iceberg configuration
      spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions

      # Define hive catalog for Iceberg support
      spark.sql.catalog.hive=org.apache.iceberg.spark.SparkCatalog
      spark.sql.catalog.hive.type=hive
      spark.sql.catalog.hive.uri=thrift://hive-metastore:9083

      # Default catalog configuration
      spark.sql.defaultCatalog=hive

      # Add these lines for Iceberg jars
      spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2

      # Performance optimizations
      spark.sql.iceberg.vectorization.enabled=true
      spark.sql.parquet.enableVectorizedReader=true
      spark.sql.adaptive.enabled=true
      spark.sql.adaptive.coalescePartitions.enabled=true
      spark.sql.adaptive.advisoryPartitionSizeInBytes=64m
      spark.sql.adaptive.skewJoin.enabled=true
      spark.sql.files.maxPartitionBytes=128m

      ${local.aws_auth_config.spark_s3_config}
    EOT

    "core-site.xml" = <<-EOT
      <?xml version="1.0" encoding="UTF-8"?>
      <configuration>
        <property>
          <name>fs.s3a.impl</name>
          <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
        </property>
        ${local.aws_auth_config.access_key_config}
      </configuration>
    EOT
  }
}

# Create Spark Master Service
resource "kubernetes_service" "spark_master" {
  depends_on = [kubernetes_config_map.spark_conf]

  metadata {
    name      = "spark-master"
    namespace = var.spark_namespace
  }

  spec {
    selector = {
      app = "spark-master"
    }

    port {
      name        = "web-ui"
      port        = 8080
      target_port = 8080
    }

    port {
      name        = "master"
      port        = 7077
      target_port = 7077
    }
  }
}

# Create Spark Master Deployment
resource "kubernetes_deployment" "spark_master" {
  depends_on = [kubernetes_service.spark_master]

  metadata {
    name      = "spark-master"
    namespace = var.spark_namespace
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "spark-master"
      }
    }

    template {
      metadata {
        labels = {
          app = "spark-master"
        }
      }

      spec {
        container {
          name              = "spark-master"
          image             = "bitnami/spark:3.5.1"
          image_pull_policy = "IfNotPresent"

          env {
            name  = "SPARK_MODE"
            value = "master"
          }

          env {
            name  = "SPARK_MASTER_PORT"
            value = "7077"
          }

          env {
            name  = "SPARK_MASTER_WEBUI_PORT"
            value = "8080"
          }

          # Add AWS credentials if using key-based auth
          dynamic "env" {
            for_each = local.aws_auth_config.container_env
            content {
              name  = env.value.name
              value = env.value.value
            }
          }

          port {
            container_port = 7077
          }

          port {
            container_port = 8080
          }

          volume_mount {
            name       = "spark-conf"
            mount_path = "/opt/bitnami/spark/conf/spark-defaults.conf"
            sub_path   = "spark-defaults.conf"
          }

          volume_mount {
            name       = "spark-conf"
            mount_path = "/opt/bitnami/spark/conf/core-site.xml"
            sub_path   = "core-site.xml"
          }
        }

        volume {
          name = "spark-conf"
          config_map {
            name = kubernetes_config_map.spark_conf.metadata[0].name
          }
        }
      }
    }
  }
}

# Create Spark Worker Deployment
resource "kubernetes_deployment" "spark_worker" {
  depends_on = [kubernetes_deployment.spark_master]

  metadata {
    name      = "spark-worker"
    namespace = var.spark_namespace
  }

  spec {
    replicas = var.spark_worker_count

    selector {
      match_labels = {
        app = "spark-worker"
      }
    }

    template {
      metadata {
        labels = {
          app = "spark-worker"
        }
      }

      spec {
        container {
          name              = "spark-worker"
          image             = "bitnami/spark:3.5.1"
          image_pull_policy = "IfNotPresent"

          env {
            name  = "SPARK_MODE"
            value = "worker"
          }

          env {
            name  = "SPARK_MASTER_URL"
            value = "spark://spark-master:7077"
          }

          env {
            name  = "SPARK_WORKER_MEMORY"
            value = var.spark_worker_memory
          }

          env {
            name  = "SPARK_WORKER_CORES"
            value = var.spark_worker_cores
          }
          env {
            name  = "SPARK_RPC_AUTHENTICATION_ENABLED"
            value = "no"
          }

          env {
            name  = "SPARK_RPC_ENCRYPTION_ENABLED"
            value = "no"
          }

          env {
            name  = "SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED"
            value = "no"
          }

          env {
            name  = "SPARK_SSL_ENABLED"
            value = "no"
          }

          # Add AWS credentials if using key-based auth
          dynamic "env" {
            for_each = local.aws_auth_config.container_env
            content {
              name  = env.value.name
              value = env.value.value
            }
          }

          port {
            container_port = 8081
          }

          volume_mount {
            name       = "spark-conf"
            mount_path = "/opt/bitnami/spark/conf/spark-defaults.conf"
            sub_path   = "spark-defaults.conf"
          }

          volume_mount {
            name       = "spark-conf"
            mount_path = "/opt/bitnami/spark/conf/core-site.xml"
            sub_path   = "core-site.xml"
          }
        }

        volume {
          name = "spark-conf"
          config_map {
            name = kubernetes_config_map.spark_conf.metadata[0].name
          }
        }
      }
    }
  }
}

# Create Spark History Server Deployment (conditional)
resource "kubernetes_deployment" "spark_history_server" {
  count = var.enable_history_server ? 1 : 0

  # The deployment should depend on the service, not the other way around
  # Also, this would create a circular dependency, so remove it
  # depends_on = [kubernetes_service.spark_history_server]

  metadata {
    name      = "spark-history-server"
    namespace = var.spark_namespace
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "spark-history-server"
      }
    }

    template {
      metadata {
        labels = {
          app = "spark-history-server"
        }
      }

      spec {
        container {
          name  = "spark-history-server"
          image = "bitnami/spark:3.5.1"

          command = ["/opt/bitnami/spark/bin/spark-class"]
          args    = ["org.apache.spark.deploy.history.HistoryServer"]

          port {
            container_port = 18080
          }

          # Fix the environment variables section - Terraform expects a list of objects
          env {
            name  = "SPARK_NO_DAEMONIZE"
            value = "true"
          }

          env {
            name  = "SPARK_HISTORY_OPTS"
            value = "-Dspark.history.fs.logDirectory=s3a://${var.s3_bucket_name}/spark-history -Dspark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem -Dspark.hadoop.fs.defaultFS=file:/// -Dspark.hadoop.security.authentication=simple"
          }
          resources {
            limits = {
              cpu    = "1"
              memory = "1Gi"
            }
            requests = {
              cpu    = "500m"
              memory = "512Mi"
            }
          }

          volume_mount {
            name       = "spark-conf"
            mount_path = "/opt/bitnami/spark/conf/spark-defaults.conf"
            sub_path   = "spark-defaults.conf"
          }

          volume_mount {
            name       = "spark-conf"
            mount_path = "/opt/bitnami/spark/conf/core-site.xml"
            sub_path   = "core-site.xml"
          }

          # Add a local directory for event logs as a fallback
          volume_mount {
            name       = "spark-events"
            mount_path = "/tmp/spark-events"
          }

          # Optional: Add liveness and readiness probes
          liveness_probe {
            http_get {
              path = "/"
              port = 18080
            }
            initial_delay_seconds = 30
            period_seconds        = 10
          }
        }

        volume {
          name = "spark-conf"
          config_map {
            name = kubernetes_config_map.spark_conf.metadata[0].name
          }
        }

        # Add a local volume for event logs as a fallback
        volume {
          name = "spark-events"
          empty_dir {}
        }

        # Consider adding a service account if needed for S3 access
        # service_account_name = kubernetes_service_account.spark_service_account[0].metadata[0].name
      }
    }
  }
}
# Create Spark History Server Deployment (conditional)
# Spark Service Account and Roles for Kubernetes
resource "kubernetes_service_account" "spark_sa" {
  metadata {
    name      = "spark"
    namespace = var.spark_namespace
  }
}

resource "kubernetes_role" "spark_role" {
  metadata {
    name      = "spark-role"
    namespace = var.spark_namespace
  }

  rule {
    api_groups = [""]
    resources  = ["*"]
    verbs      = ["*"]
  }
}

resource "kubernetes_role_binding" "spark_role_binding" {
  metadata {
    name      = "spark-role-binding"
    namespace = var.spark_namespace
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role.spark_role.metadata[0].name
  }

  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.spark_sa.metadata[0].name
    namespace = var.spark_namespace
  }
}

resource "kubernetes_cluster_role" "spark_cluster_role" {
  metadata {
    name = "spark-cluster-role"
  }

  rule {
    api_groups = [""]
    resources  = ["nodes"]
    verbs      = ["get", "list", "watch"]
  }
}

resource "kubernetes_cluster_role_binding" "spark_cluster_role_binding" {
  metadata {
    name = "spark-cluster-role-binding"
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = kubernetes_cluster_role.spark_cluster_role.metadata[0].name
  }

  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.spark_sa.metadata[0].name
    namespace = var.spark_namespace
  }
}

