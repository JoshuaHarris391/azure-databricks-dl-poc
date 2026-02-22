output "resource_group_name" {
  value = azurerm_resource_group.this.name
}

output "storage_account_name" {
  value = azurerm_storage_account.datalake.name
}

output "databricks_workspace_url" {
  value = azurerm_databricks_workspace.this.workspace_url
}

output "databricks_workspace_id" {
  value = azurerm_databricks_workspace.this.id
}

output "sql_warehouse_id" {
  value = databricks_sql_endpoint.this.id
}

output "catalog_name" {
  value = databricks_catalog.this.name
}