terraform {
  required_providers {
    azurerm = { source = "hashicorp/azurerm", version = "~> 3.0" }
    databricks = { source = "databricks/databricks", version = "~> 1.0" }
  }
  backend "azurerm" {
    resource_group_name  = "rg-ahold-poc-tfstate"
    storage_account_name = "saholdpoctfstate"
    container_name       = "tfstate"
    key                  = "ahold-poc.tfstate"
  }
}

provider "azurerm" { features {} }

resource "azurerm_resource_group" "main" {
  name     = var.resource_group_name
  location = var.location
}

resource "azurerm_storage_account" "datalake" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true  # ADLS Gen2
}

resource "azurerm_storage_container" "bronze" {
  name                  = "bronze"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "silver" {
  name                  = "silver"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "gold" {
  name                  = "gold"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "gold_edi_outbound" {
  name                  = "gold-edi-outbound"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}
