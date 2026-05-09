output "hostname" {
  description = "SWA default hostname — use in ALLOW_ORIGINS and AAD redirect URI"
  value       = azurerm_static_site.frontend.default_host_name
}

output "api_key" {
  description = "SWA deployment token — store as AZURE_STATIC_WEB_APPS_API_TOKEN GitHub secret"
  value       = azurerm_static_site.frontend.api_key
  sensitive   = true
}
