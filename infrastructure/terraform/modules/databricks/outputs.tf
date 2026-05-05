output "workspace_url" {
  value = azurerm_databricks_workspace.main.workspace_url
}

output "workspace_id" {
  value = azurerm_databricks_workspace.main.workspace_id
}

output "metastore_id" {
  value = databricks_metastore.main.id
}

output "cluster_policy_id" {
  value = databricks_cluster_policy.freshness_poc.id
}

output "key_vault_id" {
  value = azurerm_key_vault.main.id
}

output "key_vault_uri" {
  value = azurerm_key_vault.main.vault_uri
}

output "secret_scope_name" {
  value = databricks_secret_scope.kv.name
}

output "managed_identity_principal_id" {
  value = azurerm_databricks_workspace.main.storage_account_identity[0].principal_id
}
