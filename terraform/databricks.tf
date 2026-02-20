# --- Databricks Workspace ---
resource "azurerm_databricks_workspace" "this" {
  name                = "dbw-${var.project_id}-${var.environment}"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  sku                 = "premium"   # Required for Unity Catalog

  tags = azurerm_resource_group.this.tags
}

output "databricks_workspace_url" {
  value = azurerm_databricks_workspace.this.workspace_url
  description = "URL of the Databricks workspace"
}
