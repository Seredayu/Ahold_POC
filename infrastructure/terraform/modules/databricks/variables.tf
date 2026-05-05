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

variable "vnet_id" {
  type = string
}

variable "public_subnet_name" {
  type = string
}

variable "private_subnet_name" {
  type = string
}

variable "public_nsg_association_id" {
  type = string
}

variable "private_nsg_association_id" {
  type = string
}

variable "storage_account_id" {
  type = string
}

variable "storage_account_name" {
  type = string
}

variable "databricks_account_id" {
  type      = string
  sensitive = true
}

variable "key_vault_sku" {
  type    = string
  default = "standard"
}
