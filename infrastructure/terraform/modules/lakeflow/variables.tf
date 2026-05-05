variable "prefix" {
  type = string
}

variable "cluster_policy_id" {
  type = string
}

variable "secret_scope_name" {
  type = string
}

variable "bronze_catalog" {
  type    = string
  default = "bronze"
}

variable "storage_account_name" {
  type = string
}
