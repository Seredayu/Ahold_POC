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
