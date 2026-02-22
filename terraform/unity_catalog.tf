# --- Access Connector for Databricks ---
resource "azurerm_databricks_access_connector" "this" {
  name                = "ac-${var.project_id}-${var.environment}"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location

  identity {
    type = "SystemAssigned"  # Azure manages the credentials automatically
  }
}

# --- Grant the Access Connector permission to read/write ADLS Gen2 ---
resource "azurerm_role_assignment" "connector_storage" {
  scope                = azurerm_storage_account.datalake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.this.identity[0].principal_id
}

# --- Unity Catalog: Storage Credential ---
# This tells Databricks "use this Access Connector identity when accessing ADLS Gen2".
resource "databricks_storage_credential" "datalake" {
  name = "${var.project_id}-datalake-credential"

  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.this.id
  }

  depends_on = [azurerm_role_assignment.connector_storage]
}

# --- Unity Catalog: External Location ---
# This maps an ADLS Gen2 path to a Unity Catalog location that tables can reference.
resource "databricks_external_location" "bronze" {
  name            = "bronze-external"
  url             = "abfss://bronze@${azurerm_storage_account.datalake.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.datalake.name

  depends_on = [databricks_storage_credential.datalake]
}

resource "databricks_external_location" "silver" {
  name            = "silver-external"
  url             = "abfss://silver@${azurerm_storage_account.datalake.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.datalake.name

  depends_on = [databricks_storage_credential.datalake]
}

resource "databricks_external_location" "gold" {
  name            = "gold-external"
  url             = "abfss://gold@${azurerm_storage_account.datalake.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.datalake.name

  depends_on = [databricks_storage_credential.datalake]
}


# --- Catalog ---
resource "databricks_catalog" "this" {
  name          = "healthcare_poc"
  comment       = "Healthcare POC data catalog"
  storage_root  = "abfss://bronze@${azurerm_storage_account.datalake.name}.dfs.core.windows.net/"
  force_destroy = var.force_destroy_catalog

  depends_on = [
    databricks_external_location.bronze,
    databricks_external_location.silver,
    databricks_external_location.gold,
  ]
}

# --- Schemas/databases ---
resource "databricks_schema" "bronze" {
  catalog_name  = databricks_catalog.this.name
  name          = "bronze"
  comment       = "Raw ingested FHIR data"
  storage_root  = "abfss://bronze@${azurerm_storage_account.datalake.name}.dfs.core.windows.net/"
  force_destroy = var.force_destroy_catalog
}

resource "databricks_schema" "silver" {
  catalog_name  = databricks_catalog.this.name
  name          = "silver"
  comment       = "Cleaned and conformed clinical data"
  storage_root  = "abfss://silver@${azurerm_storage_account.datalake.name}.dfs.core.windows.net/"
  force_destroy = var.force_destroy_catalog
}

resource "databricks_schema" "gold" {
  catalog_name  = databricks_catalog.this.name
  name          = "gold"
  comment       = "Aggregated and curated analytics data"
  storage_root  = "abfss://gold@${azurerm_storage_account.datalake.name}.dfs.core.windows.net/"
  force_destroy = var.force_destroy_catalog
}