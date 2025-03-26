
# Outputs for the kubernetes resources
output "hive_metastore_service" {
  description = "Hive Metastore Service"
  value       = kubernetes_service.hive_metastore.metadata[0].name
}

output "spark_master_service" {
  description = "Spark Master Service"
  value       = kubernetes_service.spark_master.metadata[0].name
}

output "usage_instructions" {
  description = "Usage instructions for the data lake"
  value       = <<-EOT
    Your data lake is now fully deployed!

    Accessing services:
    ------------------
    1. Forward Spark Master UI:
       kubectl port-forward svc/spark-master 8080:8080 -n ${var.spark_namespace}
       Then open http://localhost:8080 in your browser

    2. Forward Spark History Server (if enabled):
       kubectl port-forward svc/spark-history-server 18080:18080 -n ${var.spark_namespace}
       Then open http://localhost:18080 in your browser

    Using Spark SQL with Iceberg:
    ----------------------------
    1. Submit a Spark job using kubectl:
       kubectl run spark-shell --namespace=${var.spark_namespace} --rm -it \
         --image=bitnami/spark:3.5.1 \
         --command -- spark-shell --master spark://spark-master:7077

    2. Inside spark-shell, create an Iceberg table:
       ```scala
       spark.sql("CREATE TABLE hive.default.my_table (id INT, name STRING, value DOUBLE) USING iceberg")
       spark.sql("INSERT INTO hive.default.my_table VALUES (1, 'test', 123.45)")
       spark.sql("SELECT * FROM hive.default.my_table").show()
       ```

    S3 Data Locations:
    ----------------
    - Hive Warehouse: s3://${var.s3_bucket_name}/hive/warehouse
    - Iceberg Data: s3://${var.s3_bucket_name}/iceberg/warehouse
    - Spark History Logs: s3://${var.s3_bucket_name}/spark-history
  EOT
}