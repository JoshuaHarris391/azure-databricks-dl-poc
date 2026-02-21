# --- ADLS Gen2 Storage Account ---
resource "azurerm_storage_account" "datalake" {
  name                     = "storage${var.project_id}${var.environment}"
  resource_group_name      = azurerm_resource_group.this.name
  location                 = azurerm_resource_group.this.location
  account_tier             = "Standard"
  account_replication_type = "LRS"                                    # Locally Redundant — cheapest for dev
  account_kind             = "StorageV2"
  is_hns_enabled           = true                                     # This makes it ADLS Gen2

  tags = azurerm_resource_group.this.tags
}

# --- Bronze Container ---
resource "azurerm_storage_container" "bronze" {
  name                  = "bronze"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}