variable "prefix" {
  type = string
}

variable "resource_group_name" {
  type = string
}

variable "location" {
  type    = string
  default = "westeurope"
}

variable "storage_account_name" {
  type        = string
  description = "Must be globally unique, 3-24 lowercase alphanumeric chars"
}

variable "private_subnet_id" {
  type        = string
  description = "Databricks private subnet ID for private endpoint"
}
