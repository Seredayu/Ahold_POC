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
