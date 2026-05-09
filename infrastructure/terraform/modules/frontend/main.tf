terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

resource "azurerm_static_site" "frontend" {
  name                = "${var.prefix}-swa"
  resource_group_name = var.resource_group_name
  location            = "westeurope"
  sku_tier            = "Free"
  sku_size            = "Free"
}
