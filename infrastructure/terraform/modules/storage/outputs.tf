output "storage_account_id" {
  value = azurerm_storage_account.datalake.id
}

output "storage_account_name" {
  value = azurerm_storage_account.datalake.name
}

output "dfs_endpoint" {
  value = azurerm_storage_account.datalake.primary_dfs_endpoint
}

output "primary_access_key" {
  value     = azurerm_storage_account.datalake.primary_access_key
  sensitive = true
}
