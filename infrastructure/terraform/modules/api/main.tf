terraform {
  required_providers {
    azurerm = {
      source = "hashicorp/azurerm"
    }
  }
}

data "azurerm_client_config" "current" {}

resource "azurerm_container_registry" "main" {
  name                = "${var.prefix}acr"
  resource_group_name = var.resource_group_name
  location            = var.location
  sku                 = "Basic"
  admin_enabled       = false
}

resource "azurerm_container_app_environment" "main" {
  name                = "${var.prefix}-cae"
  location            = var.location
  resource_group_name = var.resource_group_name
}

resource "azurerm_container_app" "api" {
  name                         = "${var.prefix}-api"
  container_app_environment_id = azurerm_container_app_environment.main.id
  resource_group_name          = var.resource_group_name
  revision_mode                = "Single"

  identity {
    type = "SystemAssigned"
  }

  ingress {
    external_enabled = true
    target_port      = 8000
    transport        = "http"
    traffic_weight {
      latest_revision = true
      percentage      = 100
    }
  }

  secret {
    name  = "databricks-host"
    value = var.databricks_host
  }

  secret {
    name  = "databricks-token"
    value = var.databricks_token
  }

  secret {
    name  = "databricks-http-path"
    value = var.databricks_http_path
  }

  template {
    min_replicas = 1
    max_replicas = 3

    container {
      name   = "api"
      image  = "${azurerm_container_registry.main.login_server}/${var.prefix}-api:latest"
      cpu    = 0.5
      memory = "1Gi"

      env {
        name        = "DATABRICKS_HOST"
        secret_name = "databricks-host"
      }

      env {
        name        = "DATABRICKS_TOKEN"
        secret_name = "databricks-token"
      }

      env {
        name        = "DATABRICKS_HTTP_PATH"
        secret_name = "databricks-http-path"
      }

      env {
        name  = "ALLOW_ORIGINS"
        value = var.swa_hostname != "" ? "https://${var.swa_hostname}" : "http://localhost:5173"
      }

      liveness_probe {
        path           = "/health"
        port           = 8000
        transport      = "HTTP"
        initial_delay  = 10
        period_seconds = 30
      }

      readiness_probe {
        path           = "/health"
        port           = 8000
        transport      = "HTTP"
        initial_delay  = 5
        period_seconds = 10
      }
    }
  }
}

resource "azurerm_role_assignment" "aca_acr_pull" {
  scope                = azurerm_container_registry.main.id
  role_definition_name = "AcrPull"
  principal_id         = azurerm_container_app.api.identity[0].principal_id

  depends_on = [azurerm_container_app.api]
}
