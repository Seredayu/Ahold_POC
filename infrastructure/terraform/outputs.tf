output "workspace_url" {
  description = "Databricks workspace URL — copy to terraform.tfvars as workspace_url for Stage 2"
  value       = module.databricks.workspace_url
}

output "storage_dfs_endpoint" {
  value = module.storage.dfs_endpoint
}

output "key_vault_uri" {
  value = module.databricks.key_vault_uri
}

output "metastore_id" {
  value = module.databricks.metastore_id
}

output "lakeflow_pipeline_id" {
  value = module.lakeflow.pipeline_id
}

output "acr_login_server" {
  value = module.api.acr_login_server
}

output "acr_name" {
  value = module.api.acr_name
}

output "aca_fqdn" {
  description = "Set as E2E_API_BASE in GitHub Actions E2E job (prefix with https://)"
  value       = module.api.aca_fqdn
}

output "swa_hostname" {
  description = "Set as swa_hostname in terraform.tfvars for ALLOW_ORIGINS wiring on second apply"
  value       = module.frontend.hostname
}

output "swa_api_key" {
  description = "Store as AZURE_STATIC_WEB_APPS_API_TOKEN GitHub secret"
  value       = module.frontend.api_key
  sensitive   = true
}
