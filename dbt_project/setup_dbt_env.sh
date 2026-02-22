#!/bin/bash
# Setup environment variables for dbt to connect to Databricks
# Run this script with: source setup_dbt_env.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TERRAFORM_DIR="$SCRIPT_DIR/terraform"

echo "Fetching Databricks workspace URL from Terraform..."
export DBT_DATABRICKS_HOST=$(cd "$TERRAFORM_DIR" && terraform output -raw databricks_workspace_url)

echo "Fetching SQL Warehouse ID from Terraform..."
WAREHOUSE_ID=$(cd "$TERRAFORM_DIR" && terraform output -raw sql_warehouse_id)
export DBT_DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/$WAREHOUSE_ID"

echo ""
echo "Environment variables set:"
echo "  DBT_DATABRICKS_HOST=$DBT_DATABRICKS_HOST"
echo "  DBT_DATABRICKS_HTTP_PATH=$DBT_DATABRICKS_HTTP_PATH"
echo ""

# Generate a personal access token
echo "Generating Databricks personal access token..."
TOKEN_JSON=$(databricks tokens create --lifetime-seconds 86400 --comment "dbt POC" --profile hcpoc)
export DBT_DATABRICKS_TOKEN=$(echo "$TOKEN_JSON" | jq -r '.token_value')

echo "  DBT_DATABRICKS_TOKEN=<set>"
echo ""
echo "Done! You can now run dbt commands from the dbt_project directory."
