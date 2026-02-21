terraform {
  required_version = ">= 1.5.0"

  backend "azurerm" {
    resource_group_name  = "rg-azuredbpoc-tfstate-dev"
    storage_account_name = "storageazuredbpoctfstate"
    container_name       = "tfstate"
    key                  = "terraform.tfstate"
  }

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.100"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
  }
}

# Azure provider - uses Azure CLI authentication
provider "azurerm" {
  features {}
  subscription_id             = var.subscription_id
  skip_provider_registration  = true
}

# Databricks provider - uses Azure AD authentication
provider "databricks" {
  host = azurerm_databricks_workspace.this.workspace_url
}

# A resource group for this terraform deployment
resource "azurerm_resource_group" "this" {
  name     = "rg-${var.project_id}-${var.environment}"
  location = var.location

  tags = {
    project     = var.project_id
    environment = var.environment
    managed_by  = "terraform"
  }
}