variable "project_id" {
  description = "Short project identifier, used in resource names"
  type        = string
  default     = "azuredbpoc"
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

variable "force_destroy_catalog" {
  description = "Allow Terraform to destroy Unity Catalog schemas and catalog even if they contain tables"
  type        = bool
  default     = false
}