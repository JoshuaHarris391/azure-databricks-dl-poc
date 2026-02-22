# --- Get the current user's identity ---
data "azurerm_client_config" "current" {}

# --- Grant yourself Storage Blob Data Contributor on the Storage Account ---
# need this to upload FHIR data from your local machine to ADLS Gen2.
resource "azurerm_role_assignment" "user_storage" {
  scope                = azurerm_storage_account.datalake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_client_config.current.object_id
}