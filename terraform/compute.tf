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