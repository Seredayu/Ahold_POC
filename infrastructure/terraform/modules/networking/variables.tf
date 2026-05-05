variable "prefix" {
  description = "Short prefix for all resource names"
  type        = string
}

variable "resource_group_name" {
  type = string
}

variable "location" {
  type    = string
  default = "westeurope"
}

variable "vnet_address_space" {
  type    = string
  default = "10.0.0.0/16"
}

variable "databricks_public_cidr" {
  type    = string
  default = "10.0.1.0/24"
}

variable "databricks_private_cidr" {
  type    = string
  default = "10.0.2.0/24"
}

variable "gateway_cidr" {
  type    = string
  default = "10.0.3.0/27"
}

variable "expressroute_circuit_id" {
  type      = string
  sensitive = true
}

variable "expressroute_authorization_key" {
  type      = string
  sensitive = true
}
