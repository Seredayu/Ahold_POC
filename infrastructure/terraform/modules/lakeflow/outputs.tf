output "pipeline_id" {
  value = databricks_pipeline.lakeflow_sap_bronze.id
}

output "connection_name" {
  value = databricks_connection.sap_ecc.name
}
