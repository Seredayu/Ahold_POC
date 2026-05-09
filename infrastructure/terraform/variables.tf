variable "subscription_id" {
  type = string
}

variable "location" {
  type    = string
  default = "westeurope"
}

variable "prefix" {
  type        = string
  default     = "ahpoc"
  description = "Short prefix for resource names, 2-6 chars"
}

variable "resource_group_name" {
  type    = string
  default = "rg-ahold-freshness-poc"
}

variable "storage_account_name" {
  type    = string
  default = "saholdpocdata"
}

variable "expressroute_circuit_id" {
  type      = string
  sensitive = true
}

variable "expressroute_authorization_key" {
  type      = string
  sensitive = true
}

variable "databricks_account_id" {
  type      = string
  sensitive = true
}

variable "workspace_url" {
  type        = string
  default     = ""
  description = "Leave empty for Stage 1. After Stage 1 apply completes, set this to terraform output workspace_url value."
}

variable "databricks_host" {
  type        = string
  sensitive   = true
  description = "Databricks workspace hostname (e.g. adb-xxx.azuredatabricks.net)"
}

variable "databricks_token" {
  type        = string
  sensitive   = true
  description = "Databricks personal access token for SQL warehouse"
}

variable "databricks_http_path" {
  type        = string
  sensitive   = true
  description = "Databricks SQL warehouse HTTP path (e.g. /sql/1.0/warehouses/abc123)"
}

variable "swa_hostname" {
  type        = string
  default     = ""
  description = "Set to frontend SWA hostname after first apply, then re-apply to set ALLOW_ORIGINS"
}
