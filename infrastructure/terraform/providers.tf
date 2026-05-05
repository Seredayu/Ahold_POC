terraform {
  required_version = ">= 1.5.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }
  backend "azurerm" {
    resource_group_name  = "rg-ahold-poc-tfstate"
    storage_account_name = "saholdpoctfstate"
    container_name       = "tfstate"
    key                  = "ahold-poc.tfstate"
  }
}

# Auth: requires Azure CLI login (run: az login) or ARM_* env vars.
# For Databricks accounts provider: set DATABRICKS_ACCOUNT_ID env var or use var.databricks_account_id.

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy = true
    }
  }
  subscription_id = var.subscription_id
}

# Stage 1: accounts-level Databricks provider (for metastore)
provider "databricks" {
  alias      = "accounts"
  host       = "https://accounts.azuredatabricks.net"
  account_id = var.databricks_account_id
}

# Stage 2: workspace-level provider (populated after Stage 1 apply)
provider "databricks" {
  alias = "workspace"
  host  = var.workspace_url
}
