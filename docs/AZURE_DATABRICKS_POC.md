# Azure Databricks Healthcare Data Platform — Proof of Concept

A step-by-step guide to building a small but production-patterned data pipeline on Azure using Databricks, Delta Lake, Unity Catalog, and dbt. This guide ingests real clinical data from a public FHIR API, lands it in Azure Data Lake Storage Gen2 (ADLS Gen2) as Bronze Delta tables, and uses dbt to transform it into Silver-layer analytics-ready tables.

Everything is provisioned with **Terraform IaC** and driven through the **CLI**.

---

## Table of Contents

1. [What We Are Building](#1-what-we-are-building)
2. [Architecture Overview](#2-architecture-overview)
3. [Core Concepts (with AWS Analogies)](#3-core-concepts-with-aws-analogies)
4. [Prerequisites](#4-prerequisites)
5. [Step 1 — Azure CLI Authentication](#step-1--azure-cli-authentication)
6. [Step 2 — Project Scaffold](#step-2--project-scaffold)
7. [Step 3 — Terraform: Foundation (Resource Group, ADLS Gen2)](#step-3--terraform-foundation-resource-group-adls-gen2)
8. [Step 4 — Terraform: Databricks Workspace](#step-4--terraform-databricks-workspace)
9. [Step 5 — Terraform: Unity Catalog & External Location](#step-5--terraform-unity-catalog--external-location)
10. [Step 6 — Terraform: RBAC & Managed Identities](#step-6--terraform-rbac--managed-identities)
11. [Step 7 — Terraform: SQL Warehouse (Compute)](#step-7--terraform-sql-warehouse-compute)
12. [Step 8 — Ingest FHIR Data into ADLS Gen2 (Python)](#step-8--ingest-fhir-data-into-adls-gen2-python)
13. [Step 9 — Create Bronze Delta Tables in Databricks](#step-9--create-bronze-delta-tables-in-databricks)
14. [Step 10 — dbt Project: Silver Transformations](#step-10--dbt-project-silver-transformations)
15. [Step 11 — Run & Validate End-to-End](#step-11--run--validate-end-to-end)
16. [Step 12 — Cleanup](#step-12--cleanup)
17. [Troubleshooting](#troubleshooting)

---

## 1. What We Are Building

We are building a **miniature healthcare data lakehouse** that demonstrates:

| Capability | What this POC does |
|---|---|
| **Data Ingestion** | Pulls Patient, Condition, and Encounter resources from a free public FHIR R4 API |
| **Cloud Storage** | Lands raw JSON in ADLS Gen2 (Bronze layer) |
| **Delta Lake** | Converts raw JSON into queryable Delta tables registered in Unity Catalog |
| **dbt Transformations** | Transforms Bronze → Silver using SQL models via the `dbt-databricks` adapter |
| **IaC** | All infrastructure provisioned with Terraform |
| **Authentication** | Azure CLI auth, Service Principals, Managed Identities |
| **RBAC** | Role-based access control on storage and Databricks |
| **Compute** | Databricks SQL Warehouse (serverless) |

**What this is NOT**: This is not a production deployment. It skips VNet isolation, advanced networking, customer-managed keys, and HA configuration. Those are important but orthogonal to understanding the data flow.

---

## 2. Architecture Overview

```
    ┌───────────────────┐
    │  Public FHIR API  │   (HAPI FHIR R4 — free, synthetic clinical data)
    │  hapi.fhir.org    │
    └────────┬──────────┘
             │  HTTP GET (Python script)
             ▼
    ┌───────────────────────────────────────────────┐
    │  ADLS Gen2 Storage Account                    │
    │  Container: bronze/                           │
    │    ├── patients/    (raw FHIR JSON)           │
    │    ├── conditions/  (raw FHIR JSON)           │
    │    └── encounters/  (raw FHIR JSON)           │
    └────────┬──────────────────────────────────────┘
             │  Databricks reads via External Location
             ▼
    ┌───────────────────────────────────────────────┐
    │  Databricks Workspace                         │
    │                                               │
    │  Unity Catalog                                │
    │  └── catalog: healthcare_poc                  │
    │      ├── schema: bronze                       │
    │      │   ├── patients      (Delta table)      │
    │      │   ├── conditions    (Delta table)      │
    │      │   └── encounters    (Delta table)      │
    │      └── schema: silver                       │
    │          ├── patients      (dbt model)        │
    │          ├── conditions    (dbt model)        │
    │          └── encounters    (dbt model)        │
    │                                               │
    │  SQL Warehouse (serverless compute)           │
    │  └── dbt connects here to run transformations │
    └───────────────────────────────────────────────┘
```

---

## 3. Core Concepts (with AWS Analogies)

If you have built on AWS before, this section maps every Azure concept to something you already know. Think of it as a Rosetta Stone.

### 3.1 Azure Resource Group → (No direct AWS equivalent — closest is a CloudFormation Stack)

A **Resource Group** is a container that holds related Azure resources. Every resource in Azure must belong to exactly one Resource Group. It is a logical grouping for lifecycle management — when you delete the Resource Group, everything inside it is deleted.

**AWS analogy**: Imagine if every CloudFormation stack had a mandatory "folder" in the AWS Console. That is a Resource Group. In your CDK project, each stack deploys resources — in Azure, all of those resources would sit inside one (or more) Resource Groups.

```
AWS:    CloudFormation Stack  →  groups related resources
Azure:  Resource Group        →  groups related resources
```

### 3.2 ADLS Gen2 → S3 (but with real directories)

**Azure Data Lake Storage Gen2 (ADLS Gen2)** is Azure's object storage for analytics. It is built on top of Azure Blob Storage but adds a **hierarchical namespace** — meaning directories are real filesystem objects, not just key prefixes like S3.

**AWS analogy**: ADLS Gen2 is S3 with one key upgrade: S3 fakes directories (the `/` in `s3://bucket/folder/file` is just part of the key name). ADLS Gen2 has real directories, which means operations like "rename a folder" or "delete a folder" are atomic and fast — critical for data lake patterns where tools like Spark write to temporary directories and then rename them.

```
AWS:    S3 Bucket             →  stores objects with key prefixes
Azure:  ADLS Gen2 Container   →  stores objects with real directory hierarchy

AWS:    s3://my-bucket/bronze/patients/file.json
Azure:  abfss://bronze@storageaccount.dfs.core.windows.net/patients/file.json
```

**Key terms**:
- **Storage Account**: The top-level resource (like an S3 account namespace). Must be globally unique, lowercase, 3–24 chars.
- **Container**: A partition within a Storage Account (like an S3 bucket). In this POC, we create a `bronze` container.
- **`abfss://`**: The protocol used by Spark/Databricks to read from ADLS Gen2. The `ss` stands for "secure" (TLS). Think of it as the `s3://` equivalent.

### 3.3 Azure Active Directory (Entra ID) → IAM

**Entra ID** (formerly Azure Active Directory / Azure AD) is the identity backbone of Azure. Every user, service, and application authenticates through it. It combines what AWS splits across IAM Users, IAM Roles, and AWS SSO.

**AWS analogy**: In AWS, you create IAM roles and attach policies. In Azure, you create **Entra ID identities** (users or Service Principals) and assign them **RBAC roles** on specific resources.

```
AWS:    IAM Role + Policy (e.g. s3:GetObject on arn:aws:s3:::my-bucket/*)
Azure:  Service Principal + RBAC Role Assignment (e.g. "Storage Blob Data Contributor" on Storage Account)
```

### 3.4 Service Principal → IAM Role (for services)

A **Service Principal** is an identity for an application or service (like Databricks or Terraform). It is the Azure equivalent of an IAM Role that a service assumes.

**AWS analogy**: When your Glue job runs, it assumes an IAM Role. In Azure, when Databricks needs to read ADLS Gen2, it uses a Service Principal (or Managed Identity — see below).

### 3.5 Managed Identity → IAM Role (auto-assumed, no credentials to manage)

A **Managed Identity** is a special type of Service Principal where Azure manages the credentials for you. You never see a password or secret — Azure rotates them automatically.

**AWS analogy**: It is like an EC2 Instance Profile — your code calls `boto3` and it "just works" because the IAM role is attached to the instance. Managed Identity works the same way: your Databricks workspace has an identity that Azure manages, and you grant it RBAC roles.

There are two types:
- **System-assigned**: Tied to a specific resource (created and deleted with it). Like an EC2 Instance Profile.
- **User-assigned**: Independent resource you can attach to multiple services. Like a shared IAM Role.

### 3.6 RBAC (Role-Based Access Control) → IAM Policies

**Azure RBAC** controls who can do what on which resource. It uses a **role assignment** model:

```
Role Assignment = WHO (identity) + WHAT (role definition) + WHERE (scope/resource)
```

**AWS analogy**:
```
AWS:    iam:PolicyStatement { actions: ["s3:GetObject"], resources: ["arn:aws:s3:::bucket/*"] }
Azure:  Role Assignment { role: "Storage Blob Data Reader", scope: "/subscriptions/.../storageAccounts/..." }
```

Common RBAC roles used in this POC:

| Azure RBAC Role | AWS Equivalent | What it allows |
|---|---|---|
| `Storage Blob Data Contributor` | `s3:GetObject` + `s3:PutObject` | Read/write blobs in a storage account |
| `Storage Blob Data Reader` | `s3:GetObject` | Read-only blob access |
| `Contributor` | `AdministratorAccess` (scoped) | Full control of a resource (not identity) |

### 3.7 Databricks Workspace → (No direct AWS equivalent — closest is EMR + Glue Console combined)

A **Databricks Workspace** is a managed environment where you write notebooks, run Spark jobs, query data with SQL, and manage the data catalog. Think of it as a single portal that replaces what you currently spread across Glue Console + Athena Console + Step Functions Console.

**AWS analogy**: Imagine if the Athena query editor, the Glue job console, and the Glue Data Catalog were all in one unified UI — that is a Databricks Workspace.

### 3.8 Unity Catalog → Glue Data Catalog + Lake Formation

**Unity Catalog** is Databricks' metadata and governance layer. It stores table definitions (like Glue Data Catalog) and manages access control (like Lake Formation). It has a three-level namespace:

```
Catalog  →  Schema  →  Table
```

**AWS analogy**:
```
AWS Glue:       Database    →  Table
Unity Catalog:  Catalog.Schema  →  Table
```

In your AWS pipeline, you had `acdc_test_raw_bronze` as a Glue database. In Unity Catalog, that becomes `healthcare_poc.bronze` (catalog dot schema).

### 3.9 Delta Lake → Parquet + Glue Catalog (but better)

**Delta Lake** is an open-source storage layer that adds ACID transactions on top of Parquet files. Your AWS pipeline stores raw Parquet in S3 and registers them in Glue. Delta does the same but adds:

- **Transaction log**: Every write is atomic. No partial files.
- **Schema enforcement**: Writes that violate the schema are rejected.
- **Time travel**: Query historical versions of a table (`SELECT * FROM table VERSION AS OF 3`).
- **MERGE (upserts)**: Efficiently update/insert rows — critical for CDC patterns.

**AWS analogy**: Think of Delta as "Parquet + Glue Catalog + DynamoDB Locking" all rolled into a file format. When you wrote Parquet to S3 via Athena CTAS, you had to be careful about concurrent writes and partial failures. Delta handles all of that.

### 3.10 SQL Warehouse → Athena Workgroup

A **Databricks SQL Warehouse** is a serverless SQL compute endpoint. You point BI tools or dbt at it, and it executes queries. It auto-scales and auto-suspends when idle.

**AWS analogy**: Your `AthenaStack` created a Workgroup. A SQL Warehouse is the same idea — a named compute endpoint that BI tools and dbt connect to. The pricing model is similar: pay for what you use, with auto-shutdown.

### 3.11 Terraform → CDK (different paradigm)

You used **CDK** (imperative — you write TypeScript code that generates CloudFormation). **Terraform** is **declarative** — you describe the desired end state and Terraform figures out how to get there.

**AWS analogy**:
```
CDK:        "new s3.Bucket(this, 'MyBucket', { ... })"  →  generates CloudFormation  →  AWS creates it
Terraform:  "resource azurerm_storage_account { ... }"   →  Terraform plans diff     →  Azure creates it
```

Key Terraform concepts:
- **`terraform init`**: Installs providers (like `npm install`).
- **`terraform plan`**: Shows what will change (like `cdk diff`).
- **`terraform apply`**: Deploys changes (like `cdk deploy`).
- **`terraform destroy`**: Tears everything down (like deleting CloudFormation stacks).
- **State file**: Terraform tracks what it has created in a `.tfstate` file. This is its "memory" — like CloudFormation's stack state.

---

## 4. Prerequisites

Install the following tools before starting. All commands are macOS-focused.

### 4.1 Azure Account

You need an Azure subscription. A free trial gives you $200 USD credit for 30 days.

1. Go to [https://azure.microsoft.com/en-au/free/](https://azure.microsoft.com/en-au/free/)
2. Sign up with a Microsoft account.
3. Note your **Subscription ID** — you will need it later.

### 4.2 Azure CLI

The Azure CLI (`az`) is the equivalent of the AWS CLI (`aws`).

```bash
# Install via Homebrew
brew install azure-cli

# Verify
az version
```

### 4.3 Terraform

```bash
# Install via Homebrew
brew tap hashicorp/tap
brew install hashicorp/tap/terraform

# Verify
terraform version
```

### 4.4 Databricks CLI

```bash
# Install via Homebrew
brew tap databricks/tap
brew install databricks

# Verify
databricks --version
```

### 4.5 Python 3.9+

```bash
# Check your Python version
python3 --version

# If needed, install via pyenv
pyenv install 3.12.10
pyenv shell 3.12.10
```

### 4.6 dbt with Databricks adapter

```bash
pip install dbt-databricks
dbt --version
```

### 4.7 jq (JSON processor — useful for CLI work)

```bash
brew install jq
```

---

## Step 1 — Azure CLI Authentication

**Goal**: Log in to Azure and set your active subscription. This is how Terraform and all CLI commands will authenticate.

**AWS analogy**: This is like running `aws configure` or `aws sso login` — it stores a session token locally.

### 1.1 Log in

```bash
az login
```

This opens a browser window. Sign in with your Microsoft account. Once authenticated, the CLI prints your subscriptions:

```json
[
  {
    "id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    "name": "Azure subscription 1",
    "state": "Enabled",
    "isDefault": true
  }
]
```

### 1.2 Set the active subscription

If you have multiple subscriptions, set the one you want to use:

```bash
# List subscriptions
az account list --output table

# Set the active one
az account set --subscription "<SUBSCRIPTION_ID>"

# Verify
az account show --output table
```

### 1.3 Understand what just happened

When you ran `az login`, Azure gave you an **OAuth token** stored in `~/.azure/`. Every subsequent `az` command (and Terraform) uses this token. This is identical to how `aws configure` stores credentials in `~/.aws/credentials`.

---

## Step 2 — Project Scaffold

Create the project directory structure:

```bash
mkdir -p healthcare-databricks-poc/{terraform,ingestion,dbt_project,scripts}
cd healthcare-databricks-poc
```

The final structure will look like this:

```
healthcare-databricks-poc/
├── terraform/
│   ├── main.tf              # Provider config, resource group
│   ├── storage.tf           # ADLS Gen2
│   ├── databricks.tf        # Databricks workspace
│   ├── unity_catalog.tf     # Catalog, schemas, external locations
│   ├── rbac.tf              # Role assignments
│   ├── compute.tf           # SQL Warehouse
│   ├── variables.tf         # Input variables
│   ├── outputs.tf           # Output values
│   └── terraform.tfvars     # Your specific values (git-ignored)
├── ingestion/
│   ├── requirements.txt
│   └── ingest_fhir.py       # Python script to pull FHIR data
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       └── silver/
│           ├── schema.yml
│           ├── stg_patients.sql
│           ├── stg_conditions.sql
│           └── stg_encounters.sql
├── scripts/
│   ├── create_bronze_tables.sql
│   └── validate.sql
└── README.md
```

---

## Step 3 — Terraform: Foundation (Resource Group, ADLS Gen2)

### 3.1 `terraform/variables.tf`

Define the input variables. These are like your CDK `config.ts` interface — they parameterise the deployment.

```hcl
variable "project_id" {
  description = "Short project identifier, used in resource names"
  type        = string
  default     = "hcpoc"
}

variable "environment" {
  description = "Deployment environment (dev, test, prod)"
  type        = string
  default     = "dev"
}

variable "location" {
  description = "Azure region to deploy into"
  type        = string
  default     = "australiaeast"
}

variable "subscription_id" {
  description = "Azure subscription ID"
  type        = string
}
```

### 3.2 `terraform/terraform.tfvars`

Your specific values. **Add this file to `.gitignore`** — it may contain sensitive info.

```hcl
subscription_id = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"  # Your Azure subscription ID
project_id      = "hcpoc"
environment     = "dev"
location        = "australiaeast"
```

### 3.3 `terraform/main.tf`

Configure the Azure and Databricks providers and create the Resource Group.

```hcl
terraform {
  required_version = ">= 1.5.0"

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

provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
}

# The Databricks provider is configured AFTER the workspace is created.
# We use the workspace URL from the azurerm resource.
provider "databricks" {
  host = azurerm_databricks_workspace.this.workspace_url
}

# --- Resource Group ---
# AWS analogy: Like a CloudFormation stack — a logical container for all resources.
resource "azurerm_resource_group" "this" {
  name     = "rg-${var.project_id}-${var.environment}"
  location = var.location

  tags = {
    project     = var.project_id
    environment = var.environment
    managed_by  = "terraform"
  }
}
```

### 3.4 `terraform/storage.tf`

Create the ADLS Gen2 Storage Account and Bronze container.

```hcl
# --- ADLS Gen2 Storage Account ---
# AWS analogy: This is your S3. One storage account can hold multiple containers (like multiple S3 buckets).
# `is_hns_enabled = true` turns on the hierarchical namespace, making this ADLS Gen2 (not plain Blob Storage).
resource "azurerm_storage_account" "datalake" {
  name                     = "storage${var.project_id}${var.environment}"  # Must be globally unique, lowercase, no hyphens
  resource_group_name      = azurerm_resource_group.this.name
  location                 = azurerm_resource_group.this.location
  account_tier             = "Standard"
  account_replication_type = "LRS"                                    # Locally Redundant — cheapest for dev
  account_kind             = "StorageV2"
  is_hns_enabled           = true                                     # This makes it ADLS Gen2

  tags = azurerm_resource_group.this.tags
}

# --- Bronze Container ---
# AWS analogy: Like creating an S3 bucket for your Bronze layer.
resource "azurerm_storage_container" "bronze" {
  name                  = "bronze"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}
```

### 3.5 Initialise and apply

```bash
cd terraform

# Initialise — downloads provider plugins (like npm install)
terraform init

# Preview what will be created (like cdk diff)
terraform plan

# Apply — creates the resources (like cdk deploy)
terraform apply
```

Type `yes` when prompted. Terraform will create:
1. A Resource Group named `rg-hcpoc-dev`
2. A Storage Account named `sthcpocdev`
3. A container named `bronze` inside it

**Verify via CLI**:
```bash
# List storage accounts in the resource group
az storage account list --resource-group rg-hcpoc-dev --output table

# List containers
az storage container list --account-name sthcpocdev --auth-mode login --output table
```

---

## Step 4 — Terraform: Databricks Workspace

### 4.1 `terraform/databricks.tf`

```hcl
# --- Databricks Workspace ---
# AWS analogy: There is no single AWS equivalent. This creates a managed environment
# that combines what you get from Athena (SQL queries), Glue (ETL), and the Glue Console (UI)
# into one unified workspace.
#
# The "premium" SKU is required for Unity Catalog. Think of it like choosing an AWS service tier
# that unlocks features (like choosing Athena engine v3 over v2).
resource "azurerm_databricks_workspace" "this" {
  name                = "dbw-${var.project_id}-${var.environment}"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  sku                 = "premium"   # Required for Unity Catalog

  tags = azurerm_resource_group.this.tags
}
```

### 4.2 Apply

```bash
terraform apply
```

This takes 3–5 minutes. Terraform is provisioning a full Databricks workspace in your Azure subscription.

**Verify**:
```bash
# Get the workspace URL
terraform output -raw databricks_workspace_url

# Or via Azure CLI
az databricks workspace show \
  --resource-group rg-hcpoc-dev \
  --name dbw-hcpoc-dev \
  --query workspaceUrl \
  --output tsv
```

You can open the URL in your browser to see the Databricks UI.

### 4.3 Configure the Databricks CLI

```bash
# Configure the Databricks CLI to talk to your workspace
# This uses your Azure CLI token — no separate Databricks credentials needed
databricks configure --host https://$(terraform output -raw databricks_workspace_url) --profile hcpoc

# Verify connectivity
databricks workspace list / --profile hcpoc
```

---

## Step 5 — Terraform: Unity Catalog & External Location

This step connects Databricks to your ADLS Gen2 storage so it can read/write Delta tables there.

### 5.1 Understand the chain

For Databricks to access ADLS Gen2, we need this chain:

```
Unity Catalog Table
  └── points to → Schema (bronze / silver)
       └── managed by → Catalog (healthcare_poc)
            └── reads from → External Location (bronze container path)
                 └── authenticated by → Storage Credential (Access Connector)
                      └── backed by → Managed Identity with RBAC on Storage Account
```

**AWS analogy**: In your pipeline, Glue tables point to S3 locations, and the Glue IAM role has an S3 policy. Here, Unity Catalog tables point to ADLS Gen2 locations, and an Access Connector (with a Managed Identity) has an RBAC role on the Storage Account.

### 5.2 `terraform/unity_catalog.tf`

```hcl
# --- Access Connector for Databricks ---
# AWS analogy: This is like creating an IAM Role specifically for Databricks to access S3.
# The Access Connector is a Managed Identity that Databricks uses to authenticate to ADLS Gen2.
resource "azurerm_databricks_access_connector" "this" {
  name                = "ac-${var.project_id}-${var.environment}"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location

  identity {
    type = "SystemAssigned"  # Azure manages the credentials automatically
  }
}

# --- Grant the Access Connector permission to read/write ADLS Gen2 ---
# AWS analogy: This is the IAM policy statement that allows s3:GetObject + s3:PutObject.
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
# AWS analogy: Like a Glue Database's "Location" property pointing to s3://bucket/prefix.
# This maps an ADLS Gen2 path to a Unity Catalog location that tables can reference.
resource "databricks_external_location" "bronze" {
  name            = "bronze-external"
  url             = "abfss://bronze@${azurerm_storage_account.datalake.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.datalake.name

  depends_on = [databricks_storage_credential.datalake]
}

# --- Unity Catalog: Metastore assignment ---
# Note: Unity Catalog requires a metastore. On new Databricks accounts, a default metastore
# is auto-created per region. If your account does not have one, you may need to create it
# manually in the Databricks Account Console first.
#
# If a metastore already exists for your region, Databricks auto-assigns it to new workspaces
# on Premium SKU. We proceed assuming this is the case.

# --- Catalog ---
# AWS analogy: A Catalog is a level ABOVE a Glue Database. It is a namespace that contains
# multiple schemas (databases). Your AWS project had databases like "acdc_test_raw_bronze".
# Here, we have a catalog "healthcare_poc" with schemas "bronze" and "silver".
resource "databricks_catalog" "this" {
  name    = "healthcare_poc"
  comment = "Healthcare POC data catalog"
}

# --- Schemas (Bronze and Silver) ---
# AWS analogy: Each schema is equivalent to one Glue Database.
resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.this.name
  name         = "bronze"
  comment      = "Raw ingested FHIR data"
}

resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.this.name
  name         = "silver"
  comment      = "Cleaned and conformed clinical data"
}
```

### 5.3 Apply

```bash
terraform apply
```

**Verify via Databricks CLI**:
```bash
# List catalogs
databricks catalogs list --profile hcpoc

# List schemas in our catalog
databricks schemas list healthcare_poc --profile hcpoc
```

---

## Step 6 — Terraform: RBAC & Managed Identities

### 6.1 `terraform/rbac.tf`

This step grants your own user account the necessary permissions to upload data and interact with the workspace.

```hcl
# --- Get the current user's identity ---
# AWS analogy: Like running `aws sts get-caller-identity` to get your own ARN.
data "azurerm_client_config" "current" {}

# --- Grant yourself Storage Blob Data Contributor on the Storage Account ---
# AWS analogy: Like attaching an IAM policy that allows s3:PutObject to your own IAM user.
# You need this to upload FHIR data from your local machine to ADLS Gen2.
resource "azurerm_role_assignment" "user_storage" {
  scope                = azurerm_storage_account.datalake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_client_config.current.object_id
}
```

**Important**: RBAC role assignments can take **up to 5 minutes** to propagate in Azure. If you get "AuthorizationFailed" errors after applying, wait a few minutes and retry.

### 6.2 Apply

```bash
terraform apply
```

**Verify your access**:
```bash
# Try listing files in the bronze container (should be empty)
az storage fs file list \
  --file-system bronze \
  --account-name sthcpocdev \
  --auth-mode login \
  --output table
```

---

## Step 7 — Terraform: SQL Warehouse (Compute)

### 7.1 `terraform/compute.tf`

A SQL Warehouse is the compute engine that dbt will use to run SQL queries against Unity Catalog tables.

```hcl
# --- SQL Warehouse ---
# AWS analogy: This is your Athena Workgroup. It is a named, serverless SQL compute endpoint
# that auto-scales and auto-suspends when idle (just like Athena — you pay per query).
#
# dbt and BI tools connect to this endpoint to execute SQL.
resource "databricks_sql_endpoint" "this" {
  name             = "${var.project_id}-${var.environment}-warehouse"
  cluster_size     = "2X-Small"   # Smallest available — sufficient for POC
  auto_stop_mins   = 10           # Suspend after 10 minutes of idle (saves cost)
  warehouse_type   = "SERVERLESS" # No cluster management

  tags {
    custom_tags {
      key   = "project"
      value = var.project_id
    }
  }
}
```

### 7.2 Apply

```bash
terraform apply
```

**Verify**:
```bash
databricks sql-warehouses list --profile hcpoc
```

---

## Step 7.5 — Terraform: Outputs

### `terraform/outputs.tf`

```hcl
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
```

Apply one more time to see the outputs:
```bash
terraform apply
terraform output
```

---

## Step 8 — Ingest FHIR Data into ADLS Gen2 (Python)

Now we shift from infrastructure to data. We will pull clinical data from a **free public FHIR API** and upload it to ADLS Gen2.

### 8.1 About the FHIR API

We are using the **HAPI FHIR R4 public test server**: `https://hapi.fhir.org/baseR4`

This server contains **synthetic** (not real) clinical data. FHIR (Fast Healthcare Interoperability Resources) is a healthcare data standard. Resources are returned as JSON objects. The three resources we will pull:

| FHIR Resource | What it is | Clinical meaning |
|---|---|---|
| **Patient** | A person receiving care | Demographics: name, DOB, gender, address |
| **Condition** | A clinical diagnosis | ICD codes: "Type 2 Diabetes", "Hypertension" |
| **Encounter** | A clinical visit | When a patient visited a facility and why |

### 8.2 `ingestion/requirements.txt`

```
requests>=2.31.0
azure-storage-file-datalake>=12.14.0
azure-identity>=1.15.0
```

### 8.3 `ingestion/ingest_fhir.py`

```python
"""
Ingest FHIR R4 resources from the public HAPI FHIR server and upload to ADLS Gen2.

This script:
1. Queries the HAPI FHIR R4 API for Patient, Condition, and Encounter resources.
2. Writes each resource as a newline-delimited JSON (NDJSON) file.
3. Uploads the files to the "bronze" container in ADLS Gen2.

Authentication: Uses DefaultAzureCredential, which picks up your `az login` session.
AWS analogy: This is like a Glue Python Shell job that calls an external API and writes to S3.
"""

import json
import os
import sys
from datetime import datetime, timezone

import requests
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient


# --- Configuration ---
FHIR_BASE_URL = "https://hapi.fhir.org/baseR4"
RESOURCE_TYPES = ["Patient", "Condition", "Encounter"]
PAGE_SIZE = 100  # Number of resources to fetch per type (keep small for POC)

# These come from Terraform outputs or environment variables
STORAGE_ACCOUNT_NAME = os.environ.get("STORAGE_ACCOUNT_NAME", "sthcpocdev")
CONTAINER_NAME = "bronze"


def fetch_fhir_resources(resource_type: str, count: int = PAGE_SIZE) -> list[dict]:
    """
    Fetch FHIR resources from the public HAPI FHIR server.

    FHIR APIs return a 'Bundle' — a wrapper object containing an array of resources
    under the 'entry' key. Each entry has a 'resource' field with the actual clinical data.

    AWS analogy: This is like calling an external REST API from a Glue job.
    """
    url = f"{FHIR_BASE_URL}/{resource_type}"
    params = {"_count": count, "_format": "json"}

    print(f"Fetching {count} {resource_type} resources from {url}...")
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    bundle = response.json()
    entries = bundle.get("entry", [])
    resources = [entry["resource"] for entry in entries]

    print(f"  Retrieved {len(resources)} {resource_type} resources.")
    return resources


def write_ndjson(resources: list[dict], filepath: str) -> None:
    """
    Write resources as newline-delimited JSON (NDJSON).

    NDJSON is one JSON object per line. This is the standard format for bulk data loading
    into data lakes because each line can be parsed independently (unlike a JSON array
    which must be read entirely into memory).

    AWS analogy: This is the same NDJSON format that Athena and Glue expect when reading JSON from S3.
    """
    with open(filepath, "w") as f:
        for resource in resources:
            f.write(json.dumps(resource) + "\n")
    print(f"  Wrote {len(resources)} records to {filepath}")


def upload_to_adls(local_path: str, remote_dir: str, filename: str) -> None:
    """
    Upload a local file to ADLS Gen2.

    Uses DefaultAzureCredential — this automatically picks up the token from `az login`.
    AWS analogy: This is like boto3's s3.upload_file() using credentials from ~/.aws/credentials.
    """
    credential = DefaultAzureCredential()
    service_client = DataLakeServiceClient(
        account_url=f"https://{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net",
        credential=credential,
    )
    file_system_client = service_client.get_file_system_client(CONTAINER_NAME)
    directory_client = file_system_client.get_directory_client(remote_dir)
    file_client = directory_client.get_file_client(filename)

    with open(local_path, "rb") as f:
        file_client.upload_data(f, overwrite=True)

    print(f"  Uploaded to abfss://{CONTAINER_NAME}@{STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{remote_dir}/{filename}")


def main():
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    for resource_type in RESOURCE_TYPES:
        # 1. Fetch from FHIR API
        resources = fetch_fhir_resources(resource_type)

        if not resources:
            print(f"  No {resource_type} resources found, skipping.")
            continue

        # 2. Write to local NDJSON file
        local_filename = f"{resource_type.lower()}_{timestamp}.ndjson"
        local_path = f"/tmp/{local_filename}"
        write_ndjson(resources, local_path)

        # 3. Upload to ADLS Gen2 Bronze container
        # Directory structure: bronze/{resource_type}/filename.ndjson
        remote_dir = resource_type.lower()
        upload_to_adls(local_path, remote_dir, local_filename)

        # 4. Cleanup local file
        os.remove(local_path)

    print("\nIngestion complete.")


if __name__ == "__main__":
    main()
```

### 8.4 Run the ingestion

```bash
cd ingestion

# Install dependencies
pip install -r requirements.txt

# Set the storage account name (from Terraform output)
export STORAGE_ACCOUNT_NAME=$(cd ../terraform && terraform output -raw storage_account_name)

# Run the ingestion
python ingest_fhir.py
```

**Verify the data landed**:
```bash
# List files in the bronze container
az storage fs file list \
  --file-system bronze \
  --account-name sthcpocdev \
  --auth-mode login \
  --output table
```

You should see files like:
```
Name                                    IsDirectory    ContentLength
--------------------------------------  -------------  ---------------
patient/patient_20260219T040000Z.ndjson  False          45230
condition/condition_20260219T040000Z.ndjson  False      31456
encounter/encounter_20260219T040000Z.ndjson  False      28912
```

---

## Step 9 — Create Bronze Delta Tables in Databricks

Now we tell Databricks to read the raw NDJSON files and create Delta tables in Unity Catalog.

### 9.1 `scripts/create_bronze_tables.sql`

This SQL script is run in the Databricks SQL editor or via the Databricks CLI. It creates **external Delta tables** that point to the NDJSON files in ADLS Gen2.

```sql
-- ============================================================
-- Create Bronze Delta Tables from raw NDJSON in ADLS Gen2
-- ============================================================
-- AWS analogy: This is equivalent to running CREATE EXTERNAL TABLE in Athena
-- pointing to S3 paths. The key difference is that we are creating Delta tables
-- (with a transaction log) rather than plain Hive tables over raw files.
-- ============================================================

-- Use our catalog and schema
USE CATALOG healthcare_poc;
USE SCHEMA bronze;

-- 1. Patients
-- Read all NDJSON files from the patients directory and create a managed Delta table.
-- COPY INTO is Databricks' idempotent bulk loader — it tracks which files have been loaded
-- and skips them on re-run (like an incremental Glue crawler, but deterministic).
CREATE TABLE IF NOT EXISTS patients
USING DELTA
AS SELECT * FROM read_files(
  'abfss://bronze@${STORAGE_ACCOUNT}/patient/',
  format => 'json',
  multiLine => false
);

-- 2. Conditions
CREATE TABLE IF NOT EXISTS conditions
USING DELTA
AS SELECT * FROM read_files(
  'abfss://bronze@${STORAGE_ACCOUNT}/condition/',
  format => 'json',
  multiLine => false
);

-- 3. Encounters
CREATE TABLE IF NOT EXISTS encounters
USING DELTA
AS SELECT * FROM read_files(
  'abfss://bronze@${STORAGE_ACCOUNT}/encounter/',
  format => 'json',
  multiLine => false
);

-- Verify
SELECT 'patients' AS table_name, COUNT(*) AS row_count FROM patients
UNION ALL
SELECT 'conditions', COUNT(*) FROM conditions
UNION ALL
SELECT 'encounters', COUNT(*) FROM encounters;
```

### 9.2 Run via Databricks CLI

You can run SQL statements through the Databricks CLI using the SQL Warehouse:

```bash
# Get the warehouse ID
WAREHOUSE_ID=$(cd terraform && terraform output -raw sql_warehouse_id)
STORAGE_ACCOUNT=$(cd terraform && terraform output -raw storage_account_name)

# Replace the placeholder in the SQL script
sed "s/\${STORAGE_ACCOUNT}/${STORAGE_ACCOUNT}.dfs.core.windows.net/g" \
  scripts/create_bronze_tables.sql > /tmp/bronze_tables.sql

# Execute via Databricks SQL (you can also paste this into the Databricks SQL editor in the UI)
databricks sql execute \
  --warehouse-id "$WAREHOUSE_ID" \
  --statement "$(cat /tmp/bronze_tables.sql)" \
  --profile hcpoc
```

Alternatively, open the **Databricks SQL Editor** in your browser, connect to your warehouse, and paste the SQL script directly.

### 9.3 Verify the tables

```bash
# List tables in the bronze schema
databricks tables list healthcare_poc.bronze --profile hcpoc
```

Or run a quick query:
```sql
SELECT * FROM healthcare_poc.bronze.patients LIMIT 5;
```

---

## Step 10 — dbt Project: Silver Transformations

Now we use **dbt** to transform the messy raw Bronze tables into clean, typed Silver tables.

### 10.1 `dbt_project/dbt_project.yml`

```yaml
name: 'healthcare_poc'
version: '1.0.0'
config-version: 2

profile: 'healthcare_poc'

model-paths: ["models"]
test-paths: ["tests"]

models:
  healthcare_poc:
    silver:
      +materialized: table
      +schema: silver
```

### 10.2 `dbt_project/profiles.yml`

This configures dbt to connect to your Databricks SQL Warehouse. **Add this to `.gitignore`** if it contains tokens.

```yaml
healthcare_poc:
  target: dev
  outputs:
    dev:
      type: databricks
      # The host is your workspace URL (without https://)
      host: "{{ env_var('DBT_DATABRICKS_HOST') }}"
      # The HTTP path is the SQL Warehouse endpoint
      http_path: "{{ env_var('DBT_DATABRICKS_HTTP_PATH') }}"
      catalog: healthcare_poc
      schema: silver
      # Use a personal access token for auth
      token: "{{ env_var('DBT_DATABRICKS_TOKEN') }}"
```

To get the HTTP path for your SQL Warehouse:
```bash
# Via Databricks CLI
databricks sql-warehouses get "$WAREHOUSE_ID" --profile hcpoc | jq -r '.odbc_params.path'
```

To generate a personal access token:
```bash
# Generate a PAT via Databricks CLI
databricks tokens create --lifetime-seconds 86400 --comment "dbt POC" --profile hcpoc
```

Set the environment variables:
```bash
export DBT_DATABRICKS_HOST=$(cd terraform && terraform output -raw databricks_workspace_url)
export DBT_DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/$(cd terraform && terraform output -raw sql_warehouse_id)"
export DBT_DATABRICKS_TOKEN="<paste-token-from-above>"
```

### 10.3 `dbt_project/models/silver/stg_patients.sql`

```sql
/*
    Silver Patients Model
    =====================
    Flattens the raw FHIR Patient JSON into a clean, typed relational table.

    FHIR Patient resources have deeply nested structures (arrays of names, addresses, etc).
    This model extracts the most commonly needed fields for analytics.

    AWS analogy: This is equivalent to a dbt model you would run via Athena —
    the SQL reads from a source table and materializes a new table.
    The difference is the source is a Delta table (not raw Parquet), and the
    target is also a Delta table in Unity Catalog (not a Hive table in Glue).
*/

WITH source AS (
    SELECT * FROM {{ source('bronze', 'patients') }}
),

parsed AS (
    SELECT
        id AS patient_id,
        -- FHIR stores names as an array of objects. Extract the first (official) name.
        name[0].family AS last_name,
        name[0].given[0] AS first_name,
        gender,
        birthDate AS date_of_birth,
        -- Extract address fields (FHIR addresses are also arrays)
        address[0].city AS city,
        address[0].state AS state,
        address[0].postalCode AS postal_code,
        address[0].country AS country,
        -- Metadata
        meta.lastUpdated AS last_updated,
        CURRENT_TIMESTAMP() AS _loaded_at
    FROM source
    WHERE id IS NOT NULL
)

SELECT * FROM parsed
```

### 10.4 `dbt_project/models/silver/stg_conditions.sql`

```sql
/*
    Silver Conditions Model
    =======================
    Flattens FHIR Condition resources into a clean diagnoses table.

    A Condition represents a clinical diagnosis (e.g., "Type 2 Diabetes Mellitus").
    It links to a Patient via the `subject.reference` field, which contains a relative
    URL like "Patient/12345" — we extract just the ID.
*/

WITH source AS (
    SELECT * FROM {{ source('bronze', 'conditions') }}
),

parsed AS (
    SELECT
        id AS condition_id,
        -- Extract patient ID from the FHIR reference (format: "Patient/12345")
        REPLACE(subject.reference, 'Patient/', '') AS patient_id,
        -- The clinical code (e.g., ICD-10 or SNOMED CT)
        code.coding[0].system AS coding_system,
        code.coding[0].code AS condition_code,
        code.coding[0].display AS condition_display,
        -- Clinical status (active, resolved, etc.)
        clinicalStatus.coding[0].code AS clinical_status,
        -- Verification status (confirmed, provisional, etc.)
        verificationStatus.coding[0].code AS verification_status,
        -- When was this condition recorded
        recordedDate AS recorded_date,
        CURRENT_TIMESTAMP() AS _loaded_at
    FROM source
    WHERE id IS NOT NULL
)

SELECT * FROM parsed
```

### 10.5 `dbt_project/models/silver/stg_encounters.sql`

```sql
/*
    Silver Encounters Model
    =======================
    Flattens FHIR Encounter resources into a clean visits/admissions table.

    An Encounter represents a clinical interaction — an office visit, an ER visit,
    a hospital admission, etc. It links to a Patient and has a status lifecycle
    (planned → arrived → in-progress → finished).
*/

WITH source AS (
    SELECT * FROM {{ source('bronze', 'encounters') }}
),

parsed AS (
    SELECT
        id AS encounter_id,
        -- Extract patient ID from the FHIR reference
        REPLACE(subject.reference, 'Patient/', '') AS patient_id,
        -- Encounter classification
        class.code AS encounter_class,        -- e.g., "AMB" (ambulatory), "IMP" (inpatient)
        class.display AS encounter_class_name,
        status AS encounter_status,           -- e.g., "finished", "in-progress"
        -- Encounter type (reason for visit)
        type[0].coding[0].code AS encounter_type_code,
        type[0].coding[0].display AS encounter_type_display,
        -- Time period
        period.start AS encounter_start,
        period.end AS encounter_end,
        CURRENT_TIMESTAMP() AS _loaded_at
    FROM source
    WHERE id IS NOT NULL
)

SELECT * FROM parsed
```

### 10.6 `dbt_project/models/silver/schema.yml`

This file defines the sources (Bronze tables) and tests for the Silver models.

```yaml
version: 2

sources:
  - name: bronze
    catalog: healthcare_poc
    schema: bronze
    tables:
      - name: patients
      - name: conditions
      - name: encounters

models:
  - name: stg_patients
    description: "Cleaned and flattened patient demographics from FHIR Patient resources"
    columns:
      - name: patient_id
        description: "Unique FHIR resource ID for the patient"
        tests:
          - unique
          - not_null

      - name: gender
        description: "Patient gender (male, female, other, unknown)"
        tests:
          - accepted_values:
              values: ['male', 'female', 'other', 'unknown']

  - name: stg_conditions
    description: "Cleaned diagnoses/conditions from FHIR Condition resources"
    columns:
      - name: condition_id
        description: "Unique FHIR resource ID for the condition"
        tests:
          - unique
          - not_null

      - name: patient_id
        description: "FK to stg_patients"
        tests:
          - not_null

  - name: stg_encounters
    description: "Cleaned clinical encounters from FHIR Encounter resources"
    columns:
      - name: encounter_id
        description: "Unique FHIR resource ID for the encounter"
        tests:
          - unique
          - not_null

      - name: patient_id
        description: "FK to stg_patients"
        tests:
          - not_null
```

### 10.7 Run dbt

```bash
cd dbt_project

# Check the connection
dbt debug

# Run the models (creates Silver tables)
dbt run

# Run the tests (validates data quality)
dbt test
```

**Expected output**:
```
Running with dbt=1.8.0
Found 3 models, 8 tests, 3 sources

Concurrency: 1 threads (target='dev')

1 of 3 START sql table model silver.stg_patients .......................... [RUN]
1 of 3 OK created sql table model silver.stg_patients ..................... [OK in 4.2s]
2 of 3 START sql table model silver.stg_conditions ........................ [RUN]
2 of 3 OK created sql table model silver.stg_conditions ................... [OK in 3.8s]
3 of 3 START sql table model silver.stg_encounters ........................ [RUN]
3 of 3 OK created sql table model silver.stg_encounters ................... [OK in 3.5s]

Finished running 3 table models in 15.2s.
Completed successfully.
```

---

## Step 11 — Run & Validate End-to-End

### 11.1 Validate the Silver tables

Create a validation script or run these queries in the Databricks SQL editor:

### `scripts/validate.sql`

```sql
-- ============================================================
-- Validate Silver Tables
-- ============================================================

USE CATALOG healthcare_poc;

-- 1. Row counts
SELECT 'silver.stg_patients' AS table_name, COUNT(*) AS rows FROM silver.stg_patients
UNION ALL
SELECT 'silver.stg_conditions', COUNT(*) FROM silver.stg_conditions
UNION ALL
SELECT 'silver.stg_encounters', COUNT(*) FROM silver.stg_encounters;

-- 2. Sample patients
SELECT patient_id, first_name, last_name, gender, date_of_birth, state
FROM silver.stg_patients
LIMIT 10;

-- 3. Top conditions by frequency
SELECT condition_display, COUNT(*) AS occurrences
FROM silver.stg_conditions
GROUP BY condition_display
ORDER BY occurrences DESC
LIMIT 10;

-- 4. Encounters per patient (simple analytics query)
SELECT
    p.patient_id,
    p.first_name,
    p.last_name,
    COUNT(e.encounter_id) AS total_encounters
FROM silver.stg_patients p
LEFT JOIN silver.stg_encounters e ON p.patient_id = e.patient_id
GROUP BY p.patient_id, p.first_name, p.last_name
ORDER BY total_encounters DESC
LIMIT 10;

-- 5. Patients with a specific condition (clinical trial eligibility pattern)
SELECT
    p.patient_id,
    p.first_name,
    p.last_name,
    p.gender,
    p.date_of_birth,
    c.condition_display,
    c.clinical_status
FROM silver.stg_patients p
JOIN silver.stg_conditions c ON p.patient_id = c.patient_id
WHERE c.clinical_status = 'active'
ORDER BY c.condition_display;
```

### 11.2 End-to-end run script

For convenience, here is a single script that runs the full pipeline:

### `scripts/run_pipeline.sh`

```bash
#!/bin/bash
set -euo pipefail

echo "============================================"
echo "Healthcare Databricks POC — Full Pipeline"
echo "============================================"

# 0. Load Terraform outputs
cd "$(dirname "$0")/../terraform"
export STORAGE_ACCOUNT_NAME=$(terraform output -raw storage_account_name)
export DBT_DATABRICKS_HOST=$(terraform output -raw databricks_workspace_url)
WAREHOUSE_ID=$(terraform output -raw sql_warehouse_id)
export DBT_DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/${WAREHOUSE_ID}"

# Ensure DBT_DATABRICKS_TOKEN is set
if [ -z "${DBT_DATABRICKS_TOKEN:-}" ]; then
  echo "ERROR: Set DBT_DATABRICKS_TOKEN before running this script."
  echo "  Generate one with: databricks tokens create --lifetime-seconds 86400 --profile hcpoc"
  exit 1
fi

# 1. Ingest FHIR data
echo ""
echo "--- Step 1: Ingest FHIR data into ADLS Gen2 ---"
cd ../ingestion
pip install -q -r requirements.txt
python ingest_fhir.py

# 2. Create Bronze Delta tables
echo ""
echo "--- Step 2: Create Bronze Delta tables ---"
cd ../scripts
STORAGE_DFS="${STORAGE_ACCOUNT_NAME}.dfs.core.windows.net"
sed "s/\${STORAGE_ACCOUNT}/${STORAGE_DFS}/g" create_bronze_tables.sql > /tmp/bronze_tables.sql
echo "Run the SQL in /tmp/bronze_tables.sql via the Databricks SQL editor"
echo "  (Automated execution via CLI requires the Databricks SQL Statement Execution API)"

# 3. Run dbt
echo ""
echo "--- Step 3: dbt run (Silver transformations) ---"
cd ../dbt_project
dbt run

# 4. dbt test
echo ""
echo "--- Step 4: dbt test (data quality) ---"
dbt test

echo ""
echo "============================================"
echo "Pipeline complete! Validate via:"
echo "  - Databricks SQL editor: run scripts/validate.sql"
echo "  - CLI: databricks sql execute --warehouse-id $WAREHOUSE_ID"
echo "============================================"
```

Make it executable:
```bash
chmod +x scripts/run_pipeline.sh
```

---

## Step 12 — Cleanup

**IMPORTANT**: Azure resources cost money. Tear everything down when you are done.

### 12.1 Destroy all Terraform resources

```bash
cd terraform

# Preview what will be destroyed
terraform plan -destroy

# Destroy everything
terraform destroy
```

Type `yes` when prompted. This will delete:
- The Databricks workspace (and everything inside it)
- The Storage Account (and all data)
- The Resource Group
- All RBAC assignments

### 12.2 Verify nothing is left

```bash
# Check that the resource group is gone
az group show --name rg-hcpoc-dev 2>&1 | grep -q "ResourceGroupNotFound" && echo "Clean!" || echo "Still exists"
```

---

## Troubleshooting

### "AuthorizationPermissionMismatch" when uploading to ADLS Gen2

**Cause**: Your RBAC role assignment has not propagated yet.
**Fix**: Wait 5 minutes after `terraform apply` and retry. Azure RBAC propagation is eventually consistent.

### "CATALOG_NOT_FOUND: healthcare_poc"

**Cause**: The Unity Catalog metastore may not be assigned to your workspace.
**Fix**: Go to the Databricks Account Console → Workspaces → your workspace → assign the metastore for your region. On newer Databricks accounts, this happens automatically for Premium workspaces.

### "SQL warehouse ... is not running"

**Cause**: The SQL Warehouse auto-suspended after 10 minutes of idle.
**Fix**: It auto-starts when you send a query. The first query after suspension takes ~30 seconds to warm up.

### dbt "Runtime Error: ... does not exist"

**Cause**: The Bronze tables have not been created yet (Step 9), or the catalog/schema names do not match.
**Fix**: Verify the tables exist:
```bash
databricks tables list healthcare_poc.bronze --profile hcpoc
```

### Terraform "Provider produced inconsistent result"

**Cause**: The Databricks provider depends on the workspace being created first, but Terraform tries to configure it during `plan`.
**Fix**: On the very first `apply`, you may need to apply in two stages:
```bash
# Stage 1: Create Azure resources only
terraform apply -target=azurerm_databricks_workspace.this -target=azurerm_resource_group.this -target=azurerm_storage_account.datalake -target=azurerm_storage_container.bronze

# Stage 2: Create Databricks resources
terraform apply
```

### "Access Connector not found" or "Storage Credential failed"

**Cause**: The Access Connector Managed Identity has not been granted RBAC on the storage account yet.
**Fix**: Ensure the `azurerm_role_assignment.connector_storage` resource was applied successfully. Check:
```bash
az role assignment list --scope $(terraform output -raw storage_account_id) --output table
```

---

## Summary: What You Have Built

| Layer | Technology | What it does |
|---|---|---|
| **Infrastructure** | Terraform | Provisions all Azure resources declaratively |
| **Authentication** | Azure CLI + Entra ID | SSO login, Service Principals, Managed Identities |
| **Storage** | ADLS Gen2 | Hierarchical data lake storage (Bronze container) |
| **Ingestion** | Python + FHIR API | Pulls real clinical data into the data lake |
| **Table Format** | Delta Lake | ACID transactions, schema enforcement on Bronze tables |
| **Catalog** | Unity Catalog | Three-level namespace (catalog.schema.table) with governance |
| **Compute** | SQL Warehouse (Serverless) | Pay-per-query SQL engine for dbt and ad-hoc queries |
| **Transformation** | dbt (databricks adapter) | SQL-based Bronze → Silver transformations |
| **RBAC** | Azure RBAC + Unity Catalog grants | Least-privilege access for users and services |

This POC demonstrates every layer of a modern Azure data lakehouse — from IaC to ingestion to transformation — using real healthcare data patterns. It is intentionally small but architecturally complete.
