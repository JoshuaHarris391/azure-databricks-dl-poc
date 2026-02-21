#!/bin/bash
#
# Terraform Backend Setup Script
# 
# This script provisions the Azure infrastructure required to store Terraform state files remotely.
# It creates:
#   1. A resource group to contain all backend resources
#   2. A storage account with secure HTTPS-only access
#   3. A blob container named 'tfstate' to store the Terraform state files
#
# Prerequisites:
#   - Azure CLI installed and authenticated (az login)
#   - Appropriate permissions to create resources in the subscription
#

# Create resource group
echo "Creating resource group..."
az group create --name rg-azuredbpoc-tfstate-dev --location australiaeast

# Create storage account
echo "Creating storage account..."
az storage account create \
  --name storageazuredbpoctfstate \
  --resource-group rg-azuredbpoc-tfstate-dev \
  --sku Standard_LRS \
  --kind StorageV2 \
  --https-only true

# Create tfstate container
echo "Creating tfstate container..."
az storage container create \
  --name tfstate \
  --account-name storageazuredbpoctfstate

echo "Backend setup complete!"