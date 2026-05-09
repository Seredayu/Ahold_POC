resource "azurerm_resource_group" "main" {
  name     = var.resource_group_name
  location = var.location
}

module "networking" {
  source = "./modules/networking"

  prefix                         = var.prefix
  resource_group_name            = azurerm_resource_group.main.name
  location                       = var.location
  expressroute_circuit_id        = var.expressroute_circuit_id
  expressroute_authorization_key = var.expressroute_authorization_key
}

module "storage" {
  source = "./modules/storage"

  prefix               = var.prefix
  resource_group_name  = azurerm_resource_group.main.name
  location             = var.location
  storage_account_name = var.storage_account_name
  private_subnet_id    = module.networking.databricks_private_subnet_id
}

module "databricks" {
  source = "./modules/databricks"

  providers = {
    azurerm              = azurerm
    databricks.accounts  = databricks.accounts
    databricks.workspace = databricks.workspace
  }

  prefix                     = var.prefix
  resource_group_name        = azurerm_resource_group.main.name
  location                   = var.location
  vnet_id                    = module.networking.vnet_id
  public_subnet_name         = module.networking.databricks_public_subnet_name
  private_subnet_name        = module.networking.databricks_private_subnet_name
  public_nsg_association_id  = module.networking.public_nsg_association_id
  private_nsg_association_id = module.networking.private_nsg_association_id
  storage_account_id         = module.storage.storage_account_id
  storage_account_name       = module.storage.storage_account_name
  databricks_account_id      = var.databricks_account_id
}

module "lakeflow" {
  source = "./modules/lakeflow"

  providers = {
    databricks.workspace = databricks.workspace
  }

  prefix               = var.prefix
  cluster_policy_id    = module.databricks.cluster_policy_id
  secret_scope_name    = module.databricks.secret_scope_name
  storage_account_name = module.storage.storage_account_name

  depends_on = [module.databricks]
}

module "frontend" {
  source = "./modules/frontend"

  prefix              = var.prefix
  resource_group_name = azurerm_resource_group.main.name
}

module "api" {
  source = "./modules/api"

  prefix               = var.prefix
  resource_group_name  = azurerm_resource_group.main.name
  location             = var.location
  databricks_host      = var.databricks_host
  databricks_token     = var.databricks_token
  databricks_http_path = var.databricks_http_path
  swa_hostname         = var.swa_hostname != "" ? var.swa_hostname : module.frontend.hostname

  depends_on = [module.frontend]
}
