variable "prefix" {
  type = string
}

variable "resource_group_name" {
  type = string
}

variable "location" {
  type = string
}

variable "databricks_host" {
  type      = string
  sensitive = true
}

variable "databricks_token" {
  type      = string
  sensitive = true
}

variable "databricks_http_path" {
  type      = string
  sensitive = true
}

variable "swa_hostname" {
  type        = string
  default     = ""
  description = "Azure Static Web Apps hostname — set after frontend module applied. Leave empty on first apply."
}
