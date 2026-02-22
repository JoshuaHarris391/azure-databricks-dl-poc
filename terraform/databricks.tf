# --- Databricks Workspace ---
resource "azurerm_databricks_workspace" "this" {
  name                = "dbw-${var.project_id}-${var.environment}"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  sku                 = "premium"   # Required for Unity Catalog

  tags = azurerm_resource_group.this.tags
}

# --- Databricks Repo ---
# Syncs this Git repository into the Databricks workspace so jobs can reference scripts directly.
resource "databricks_repo" "this" {
  url          = "https://github.com/JoshuaHarris391/azure-databricks-dl-poc"
  provider     = databricks
  path         = "/Repos/${var.project_id}/azure-databricks-dl-poc"
  git_provider = "gitHub"
}
