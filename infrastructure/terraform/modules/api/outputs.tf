output "acr_login_server" {
  description = "Container registry login server — used in GitHub Actions workflows"
  value       = azurerm_container_registry.main.login_server
}

output "acr_name" {
  description = "Container registry name — used for az acr build"
  value       = azurerm_container_registry.main.name
}

output "aca_fqdn" {
  description = "Container app FQDN — E2E_API_BASE for integration tests"
  value       = azurerm_container_app.api.latest_revision_fqdn
}

output "aca_identity_principal_id" {
  description = "ACA system-assigned identity — grant Key Vault access if needed"
  value       = azurerm_container_app.api.identity[0].principal_id
}

output "aca_name" {
  description = "Container App name — used in GitHub Actions az containerapp update commands"
  value       = azurerm_container_app.api.name
}
