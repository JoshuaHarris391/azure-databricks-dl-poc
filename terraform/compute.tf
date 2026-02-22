# --- Databricks SQL Warehouse ---
# dbt and BI tools connect to this endpoint to execute SQL.
resource "databricks_sql_endpoint" "this" {
  name             = "${var.project_id}-${var.environment}-warehouse"
  cluster_size     = "2X-Small"   # Smallest available
  auto_stop_mins   = 10           # Suspend after 10 minutes of idle
  enable_serverless_compute = true

  tags {
    custom_tags {
      key   = "project"
      value = var.project_id
    }
  }
}

# --- Databricks Job: FHIR Ingestion ---
resource "databricks_job" "fhir_ingestion" {
  name = "${var.project_id}-${var.environment}-fhir-ingest"

  task {
    task_key = "ingest_fhir"

    new_cluster {
      num_workers   = 0  # Single-node (driver only) — sufficient for API fetch + upload
      spark_version = "15.4.x-scala2.12"
      node_type_id  = "Standard_D4ds_v5"

      spark_conf = {
        "spark.databricks.cluster.profile" = "singleNode"
        "spark.master"                     = "local[*]"
      }

      custom_tags = {
        "ResourceClass" = "SingleNode"
        "project"       = var.project_id
      }

      data_security_mode = "SINGLE_USER"
    }

    library {
      pypi {
        package = "requests>=2.31.0"
      }
    }

    spark_python_task {
      python_file = "${databricks_repo.this.path}/ingestion/ingest_fhir.py"
      source      = "WORKSPACE"
    }
  }

}