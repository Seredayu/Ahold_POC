terraform {
  required_providers {
    azurerm = {
      source = "hashicorp/azurerm"
    }
    databricks = {
      source                = "databricks/databricks"
      configuration_aliases = [databricks.accounts, databricks.workspace]
    }
  }
}

data "azurerm_client_config" "current" {}

resource "azurerm_databricks_workspace" "main" {
  name                = "${var.prefix}-dbw"
  resource_group_name = var.resource_group_name
  location            = var.location
  sku                 = "premium"

  custom_parameters {
    virtual_network_id                                   = var.vnet_id
    public_subnet_name                                   = var.public_subnet_name
    private_subnet_name                                  = var.private_subnet_name
    public_subnet_network_security_group_association_id  = var.public_nsg_association_id
    private_subnet_network_security_group_association_id = var.private_nsg_association_id
    no_public_ip                                         = true
  }
}

resource "azurerm_key_vault" "main" {
  name                = "${var.prefix}-kv"
  location            = var.location
  resource_group_name = var.resource_group_name
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = var.key_vault_sku

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = azurerm_databricks_workspace.main.storage_account_identity[0].principal_id

    secret_permissions = ["Get", "List"]
  }

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id

    secret_permissions = ["Get", "List", "Set", "Delete", "Purge", "Recover"]
  }
}

resource "azurerm_role_assignment" "databricks_storage" {
  scope                = var.storage_account_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_workspace.main.storage_account_identity[0].principal_id
}

resource "databricks_metastore" "main" {
  provider      = databricks.accounts
  name          = "${var.prefix}-metastore"
  region        = var.location
  storage_root  = "abfss://gold@${var.storage_account_name}.dfs.core.windows.net/metastore"
  force_destroy = true
}

resource "databricks_metastore_assignment" "main" {
  provider     = databricks.accounts
  metastore_id = databricks_metastore.main.id
  workspace_id = azurerm_databricks_workspace.main.workspace_id
}

resource "databricks_catalog" "bronze" {
  provider   = databricks.workspace
  name       = "bronze"
  comment    = "Append-only CDC landing zone from SAP ECC"
  depends_on = [databricks_metastore_assignment.main]
}

resource "databricks_catalog" "silver" {
  provider   = databricks.workspace
  name       = "silver"
  comment    = "DLT-transformed, validated data"
  depends_on = [databricks_metastore_assignment.main]
}

resource "databricks_catalog" "gold" {
  provider   = databricks.workspace
  name       = "gold"
  comment    = "Analytical aggregates and engine outputs"
  depends_on = [databricks_metastore_assignment.main]
}

resource "databricks_catalog" "feature_store" {
  provider   = databricks.workspace
  name       = "feature_store"
  comment    = "Reusable ML features"
  depends_on = [databricks_metastore_assignment.main]
}

resource "databricks_cluster_policy" "freshness_poc" {
  provider = databricks.workspace
  name     = "freshness-poc-job-cluster"
  definition = jsonencode({
    "spark_version"         = { "type" = "fixed", "value" = "14.3.x-scala2.12" }
    "node_type_id"          = { "type" = "fixed", "value" = "Standard_DS3_v2" }
    "autoscale.min_workers" = { "type" = "fixed", "value" = 2 }
    "autoscale.max_workers" = { "type" = "fixed", "value" = 8 }
    "data_security_mode"    = { "type" = "fixed", "value" = "SINGLE_USER" }
  })
  depends_on = [databricks_metastore_assignment.main]
}

resource "databricks_secret_scope" "kv" {
  provider = databricks.workspace
  name     = "kv-secrets"

  keyvault_metadata {
    resource_id = azurerm_key_vault.main.id
    dns_name    = azurerm_key_vault.main.vault_uri
  }

  depends_on = [azurerm_key_vault.main]
}
