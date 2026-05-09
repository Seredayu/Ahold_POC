# Phase 5: Azure Deployment + E2E Testing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deploy FastAPI backend to Azure Container Apps and React field app to Azure Static Web Apps, wired to the existing Databricks SQL warehouse, with GitHub Actions CI/CD and E2E smoke tests.

**Architecture:** Terraform extends existing `infrastructure/terraform/` with two new modules — `api` (ACR + Container App Environment + Container App) and `frontend` (Static Web Apps). GitHub Actions CI/CD builds the Docker image via `az acr build` (no local Docker required), deploys to ACA, and deploys the React build to SWA. Databricks credentials flow through GitHub Actions secrets → ACA built-in secret store. E2E tests hit the live ACA endpoint over HTTPS.

**Tech Stack:** Terraform (azurerm ≥3.x), GitHub Actions, Azure Container Registry (Basic), Azure Container Apps, Azure Static Web Apps (Free tier), Python pytest + httpx (E2E), Node 20 + Vite (frontend build)

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `infrastructure/terraform/modules/api/main.tf` | Create | ACR + ACA environment + ACA container app |
| `infrastructure/terraform/modules/api/variables.tf` | Create | Input vars for api module |
| `infrastructure/terraform/modules/api/outputs.tf` | Create | ACR login server, ACA FQDN, ACA identity |
| `infrastructure/terraform/modules/frontend/main.tf` | Create | Azure Static Web Apps resource |
| `infrastructure/terraform/modules/frontend/variables.tf` | Create | Input vars for frontend module |
| `infrastructure/terraform/modules/frontend/outputs.tf` | Create | SWA hostname + deployment token |
| `infrastructure/terraform/main.tf` | Modify | Wire api + frontend modules |
| `infrastructure/terraform/variables.tf` | Modify | Add databricks_host, databricks_token, databricks_http_path |
| `infrastructure/terraform/outputs.tf` | Modify | Add ACR, ACA, SWA outputs |
| `.github/workflows/deploy-api.yml` | Create | Build Docker + push to ACR + update ACA on master push |
| `.github/workflows/deploy-frontend.yml` | Create | Build React + deploy to SWA on master push |
| `tests/e2e/__init__.py` | Create | Empty |
| `tests/e2e/conftest.py` | Create | `api_base_url` fixture from `E2E_API_BASE` env var |
| `tests/e2e/test_e2e_exception_review.py` | Create | 5 smoke tests against live ACA endpoint |

---

## Task 1: Terraform — API Module (ACR + Container App)

**Files:**
- Create: `infrastructure/terraform/modules/api/main.tf`
- Create: `infrastructure/terraform/modules/api/variables.tf`
- Create: `infrastructure/terraform/modules/api/outputs.tf`

- [ ] **Step 1: Create `infrastructure/terraform/modules/api/variables.tf`**

```hcl
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
```

- [ ] **Step 2: Create `infrastructure/terraform/modules/api/main.tf`**

```hcl
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
```

- [ ] **Step 3: Create `infrastructure/terraform/modules/api/outputs.tf`**

```hcl
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
  value = azurerm_container_app.api.name
}
```

- [ ] **Step 4: Verify module files exist**

```bash
ls infrastructure/terraform/modules/api/
# Expected: main.tf  outputs.tf  variables.tf
```

- [ ] **Step 5: Commit**

```bash
git add infrastructure/terraform/modules/api/
git commit -m "feat: add Terraform api module (ACR + Container App environment + Container App)"
```

---

## Task 2: Terraform — Frontend Module (Static Web Apps)

**Files:**
- Create: `infrastructure/terraform/modules/frontend/main.tf`
- Create: `infrastructure/terraform/modules/frontend/variables.tf`
- Create: `infrastructure/terraform/modules/frontend/outputs.tf`

- [ ] **Step 1: Create `infrastructure/terraform/modules/frontend/variables.tf`**

```hcl
variable "prefix" {
  type = string
}

variable "resource_group_name" {
  type = string
}
```

Note: Azure Static Web Apps is only available in `eastus2`, `centralus`, `westus2`, `westeurope`, `eastasia`, `eastus`. `westeurope` is already the project's default location — no change needed.

- [ ] **Step 2: Create `infrastructure/terraform/modules/frontend/main.tf`**

```hcl
terraform {
  required_providers {
    azurerm = {
      source = "hashicorp/azurerm"
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
```

- [ ] **Step 3: Create `infrastructure/terraform/modules/frontend/outputs.tf`**

```hcl
output "hostname" {
  description = "SWA default hostname — use in ALLOW_ORIGINS and AAD redirect URI"
  value       = azurerm_static_site.frontend.default_host_name
}

output "api_key" {
  description = "SWA deployment token — store as AZURE_STATIC_WEB_APPS_API_TOKEN GitHub secret"
  value       = azurerm_static_site.frontend.api_key
  sensitive   = true
}
```

- [ ] **Step 4: Commit**

```bash
git add infrastructure/terraform/modules/frontend/
git commit -m "feat: add Terraform frontend module (Azure Static Web Apps)"
```

---

## Task 3: Wire Modules into Root Terraform + Add Variables

**Files:**
- Modify: `infrastructure/terraform/main.tf`
- Modify: `infrastructure/terraform/variables.tf`
- Modify: `infrastructure/terraform/outputs.tf`

- [ ] **Step 1: Add Databricks secret variables to `infrastructure/terraform/variables.tf`**

Append to end of file:

```hcl
variable "databricks_host" {
  type        = string
  sensitive   = true
  description = "Databricks workspace hostname (e.g. adb-xxx.azuredatabricks.net)"
}

variable "databricks_token" {
  type        = string
  sensitive   = true
  description = "Databricks personal access token for SQL warehouse"
}

variable "databricks_http_path" {
  type        = string
  sensitive   = true
  description = "Databricks SQL warehouse HTTP path (e.g. /sql/1.0/warehouses/abc123)"
}

variable "swa_hostname" {
  type        = string
  default     = ""
  description = "Set to frontend SWA hostname after first apply, then re-apply to set ALLOW_ORIGINS"
}
```

- [ ] **Step 2: Add api and frontend modules to `infrastructure/terraform/main.tf`**

Append to end of file:

```hcl
module "frontend" {
  source = "./modules/frontend"

  prefix              = var.prefix
  resource_group_name = azurerm_resource_group.main.name
}

module "api" {
  source = "./modules/api"

  prefix               = var.prefix
  resource_group_name  = azurerm_resource_group.main.name
  location             = var.location
  databricks_host      = var.databricks_host
  databricks_token     = var.databricks_token
  databricks_http_path = var.databricks_http_path
  swa_hostname         = var.swa_hostname != "" ? var.swa_hostname : module.frontend.hostname

  depends_on = [module.frontend]
}
```

- [ ] **Step 3: Add outputs to `infrastructure/terraform/outputs.tf`**

Append to end of file:

```hcl
output "acr_login_server" {
  value = module.api.acr_login_server
}

output "acr_name" {
  value = module.api.acr_name
}

output "aca_fqdn" {
  description = "Set as E2E_API_BASE in GitHub Actions E2E job (prefix with https://)"
  value       = module.api.aca_fqdn
}

output "swa_hostname" {
  description = "Set as swa_hostname in terraform.tfvars for ALLOW_ORIGINS wiring on second apply"
  value       = module.frontend.hostname
}

output "swa_api_key" {
  description = "Store as AZURE_STATIC_WEB_APPS_API_TOKEN GitHub secret"
  value       = module.frontend.api_key
  sensitive   = true
}
```

- [ ] **Step 4: Add `terraform.tfvars.example`**

Create `infrastructure/terraform/terraform.tfvars.example`:

```hcl
subscription_id      = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
databricks_host      = "adb-xxxxxxxxxxxx.azuredatabricks.net"
databricks_token     = "dapixxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
databricks_http_path = "/sql/1.0/warehouses/xxxxxxxxxxxxxxxx"
swa_hostname         = ""  # leave empty on first apply; fill in after apply, then re-apply
```

- [ ] **Step 5: Apply Terraform (human step)**

Run from `infrastructure/terraform/` with credentials set:

```bash
# Copy example and fill in values
cp terraform.tfvars.example terraform.tfvars
# Edit terraform.tfvars with real values

terraform init
terraform plan -out=tfplan
terraform apply tfplan
```

Expected outputs:
```
acr_login_server = "ahpocacr.azurecr.io"
acr_name         = "ahpocacr"
aca_fqdn         = "ahpoc-api.somehash.westeurope.azurecontainerapps.io"
swa_hostname     = "random-name.azurestaticapps.net"
swa_api_key      = <sensitive>
```

**Second apply** (after filling in `swa_hostname` from first apply output):

```bash
# Update terraform.tfvars: set swa_hostname = "<output from above>"
terraform apply  # updates ALLOW_ORIGINS env var on ACA
```

- [ ] **Step 6: Set GitHub Actions secrets (human step)**

Go to GitHub → Seredayu/Ahold_POC → Settings → Secrets and variables → Actions.

Add these secrets:
| Name | Value |
|------|-------|
| `AZURE_CREDENTIALS` | JSON from `az ad sp create-for-rbac --name "github-actions-ahpoc" --role contributor --scopes /subscriptions/<sub-id>/resourceGroups/rg-ahold-freshness-poc --sdk-auth` |
| `ACR_NAME` | `ahpocacr` (from terraform output) |
| `ACA_NAME` | `ahpoc-api` |
| `AZURE_RESOURCE_GROUP` | `rg-ahold-freshness-poc` |
| `AZURE_STATIC_WEB_APPS_API_TOKEN` | from `terraform output -raw swa_api_key` |
| `E2E_API_BASE` | `https://<aca_fqdn from terraform output>` |

- [ ] **Step 7: Commit Terraform changes**

```bash
git add infrastructure/terraform/main.tf infrastructure/terraform/variables.tf infrastructure/terraform/outputs.tf infrastructure/terraform/terraform.tfvars.example
git commit -m "feat: wire api and frontend Terraform modules into root, add deployment outputs"
```

---

## Task 4: GitHub Actions — API CI/CD

**Files:**
- Create: `.github/workflows/deploy-api.yml`

- [ ] **Step 1: Create `.github/workflows/deploy-api.yml`**

```yaml
name: Deploy API

on:
  push:
    branches: [master]
    paths:
      - "src/api/**"
      - "src/medallion/gold/**"
      - "Dockerfile"
      - "requirements.txt"

jobs:
  build-and-deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Azure login
        uses: azure/login@v2
        with:
          creds: ${{ secrets.AZURE_CREDENTIALS }}

      - name: Build and push image to ACR
        run: |
          az acr build \
            --registry ${{ secrets.ACR_NAME }} \
            --image ahold-freshness-api:${{ github.sha }} \
            --image ahold-freshness-api:latest \
            .

      - name: Update Container App revision
        run: |
          az containerapp update \
            --name ${{ secrets.ACA_NAME }} \
            --resource-group ${{ secrets.AZURE_RESOURCE_GROUP }} \
            --image ${{ secrets.ACR_NAME }}.azurecr.io/ahold-freshness-api:${{ github.sha }}

      - name: Verify health endpoint
        run: |
          ACA_FQDN=$(az containerapp show \
            --name ${{ secrets.ACA_NAME }} \
            --resource-group ${{ secrets.AZURE_RESOURCE_GROUP }} \
            --query "properties.configuration.ingress.fqdn" \
            --output tsv)
          echo "ACA_FQDN=$ACA_FQDN" >> $GITHUB_ENV
          # Wait up to 60s for new revision to be ready
          for i in $(seq 1 12); do
            STATUS=$(curl -sf "https://${ACA_FQDN}/health" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('status',''))" 2>/dev/null || echo "")
            if [ "$STATUS" = "ok" ]; then
              echo "Health check passed"
              exit 0
            fi
            echo "Attempt $i: not ready yet, waiting 5s..."
            sleep 5
          done
          echo "Health check failed after 60s"
          exit 1
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/deploy-api.yml
git commit -m "feat: add GitHub Actions workflow to build and deploy FastAPI to Azure Container Apps"
```

---

## Task 5: GitHub Actions — Frontend CI/CD

**Files:**
- Create: `.github/workflows/deploy-frontend.yml`

- [ ] **Step 1: Create `.github/workflows/deploy-frontend.yml`**

```yaml
name: Deploy Frontend

on:
  push:
    branches: [master]
    paths:
      - "frontend/**"
      - "staticwebapp.config.json"

jobs:
  build-and-deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Setup Node.js
        uses: actions/setup-node@v4
        with:
          node-version: "20"
          cache: "npm"
          cache-dependency-path: frontend/package-lock.json

      - name: Install dependencies
        working-directory: frontend
        run: npm ci

      - name: Run Vitest
        working-directory: frontend
        run: npm test

      - name: Build
        working-directory: frontend
        env:
          VITE_API_BASE: ${{ secrets.E2E_API_BASE }}
          VITE_POLLING_INTERVAL_MS: "15000"
        run: npm run build

      - name: Deploy to Azure Static Web Apps
        uses: Azure/static-web-apps-deploy@v1
        with:
          azure_static_web_apps_api_token: ${{ secrets.AZURE_STATIC_WEB_APPS_API_TOKEN }}
          repo_token: ${{ secrets.GITHUB_TOKEN }}
          action: "upload"
          app_location: "frontend"
          output_location: "dist"
          skip_app_build: true
```

Note: `skip_app_build: true` because we ran `npm run build` in the previous step (gives us test gate before deploy).

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/deploy-frontend.yml
git commit -m "feat: add GitHub Actions workflow to build and deploy React to Azure Static Web Apps"
```

---

## Task 6: E2E Integration Tests

**Files:**
- Create: `tests/e2e/__init__.py`
- Create: `tests/e2e/conftest.py`
- Create: `tests/e2e/test_e2e_exception_review.py`

These tests run against the live ACA endpoint. They require `E2E_API_BASE` env var (e.g. `https://ahpoc-api.somehash.westeurope.azurecontainerapps.io`) and a real `gold.replenishment.exception_queue` table with at least one `ESCALATE` row.

- [ ] **Step 1: Create `tests/e2e/__init__.py`**

```python
```

(empty file)

- [ ] **Step 2: Create `tests/e2e/conftest.py`**

```python
import os
import pytest
import httpx


def pytest_configure(config):
    """Skip all e2e tests if E2E_API_BASE is not set."""
    if not os.environ.get("E2E_API_BASE"):
        pytest.skip("E2E_API_BASE not set — skipping e2e tests", allow_module_level=True)


@pytest.fixture(scope="session")
def api_base() -> str:
    base = os.environ["E2E_API_BASE"].rstrip("/")
    return base


@pytest.fixture(scope="session")
def manager_id() -> str:
    return os.environ.get("E2E_MANAGER_ID", "e2e-test-manager")


@pytest.fixture(scope="session")
def client(api_base) -> httpx.Client:
    with httpx.Client(base_url=api_base, timeout=30.0) as c:
        yield c
```

- [ ] **Step 3: Create `tests/e2e/test_e2e_exception_review.py`**

```python
"""
E2E smoke tests against live Azure Container Apps endpoint.

Prerequisites:
  - E2E_API_BASE env var set to ACA FQDN (https://...)
  - gold.replenishment.exception_queue table exists with >= 1 ESCALATE row
  - ACA has valid DATABRICKS_HOST / DATABRICKS_TOKEN / DATABRICKS_HTTP_PATH

Run:
  E2E_API_BASE=https://ahpoc-api.hash.westeurope.azurecontainerapps.io pytest tests/e2e/ -v
"""
import pytest


class TestHealthEndpoint:
    def test_health_returns_ok(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "timestamp" in data


class TestExceptionListEndpoint:
    def test_list_pending_returns_200(self, client):
        resp = client.get("/exceptions/?status=PENDING")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_pending_items_have_required_fields(self, client):
        resp = client.get("/exceptions/?status=PENDING")
        assert resp.status_code == 200
        items = resp.json()
        if not items:
            pytest.skip("No PENDING exceptions in queue — seed data first")
        item = items[0]
        assert "exception_id" in item
        assert "unified_sku_id" in item
        assert "recommended_qty" in item
        assert "quantity_deviation_pct" in item
        assert item["status"] == "PENDING"
        assert "|" in item["exception_id"]  # werks|sku|loaded_at format

    def test_sorted_by_deviation_pct_descending(self, client):
        resp = client.get("/exceptions/?status=PENDING")
        items = resp.json()
        if len(items) < 2:
            pytest.skip("Need >= 2 PENDING items to verify sort order")
        deviations = [item["quantity_deviation_pct"] for item in items]
        assert deviations == sorted(deviations, reverse=True)


class TestApproveRejectWorkflow:
    def test_approve_exception(self, client, manager_id):
        """Approve first available PENDING exception, verify response."""
        pending = client.get("/exceptions/?status=PENDING").json()
        if not pending:
            pytest.skip("No PENDING exceptions to approve")

        exception_id = pending[0]["exception_id"]
        resp = client.post(
            f"/exceptions/{exception_id}/approve",
            json={"override_qty": 42, "override_reason": "E2E test approval"},
            headers={"X-Manager-Id": manager_id},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "APPROVED"
        assert data["manager_id"] == manager_id
        assert data["exception_id"] == exception_id
        assert "decision_timestamp" in data

    def test_approved_exception_no_longer_pending(self, client):
        """Previously approved exception should not appear in PENDING list."""
        pending = client.get("/exceptions/?status=PENDING").json()
        if not pending:
            pytest.skip("No PENDING exceptions available")

        exception_id = pending[0]["exception_id"]
        client.post(
            f"/exceptions/{exception_id}/approve",
            json={"override_qty": None, "override_reason": None},
            headers={"X-Manager-Id": "e2e-verifier"},
        )

        still_pending = client.get("/exceptions/?status=PENDING").json()
        pending_ids = [item["exception_id"] for item in still_pending]
        assert exception_id not in pending_ids

    def test_reject_exception(self, client, manager_id):
        """Reject first available PENDING exception, verify BLOCKED status."""
        pending = client.get("/exceptions/?status=PENDING").json()
        if not pending:
            pytest.skip("No PENDING exceptions to reject")

        exception_id = pending[0]["exception_id"]
        resp = client.post(
            f"/exceptions/{exception_id}/reject",
            headers={"X-Manager-Id": manager_id},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "BLOCKED"
        assert data["manager_id"] == manager_id
```

- [ ] **Step 4: Add E2E test step to API deploy workflow**

In `.github/workflows/deploy-api.yml`, append a new job after `build-and-deploy`:

```yaml
  e2e-smoke:
    runs-on: ubuntu-latest
    needs: build-and-deploy
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install test deps
        run: pip install pytest httpx

      - name: Run E2E smoke tests
        env:
          E2E_API_BASE: ${{ secrets.E2E_API_BASE }}
          E2E_MANAGER_ID: github-actions-e2e
        run: pytest tests/e2e/ -v --tb=short
```

- [ ] **Step 5: Commit**

```bash
git add tests/e2e/ .github/workflows/deploy-api.yml
git commit -m "feat: add E2E smoke tests + wire into API deploy workflow"
```

---

## Verification Checklist

After all tasks complete and Terraform is applied:

```bash
# 1. Terraform outputs
cd infrastructure/terraform
terraform output acr_login_server   # ahpocacr.azurecr.io
terraform output aca_fqdn           # ahpoc-api.hash.westeurope.azurecontainerapps.io
terraform output swa_hostname       # random-name.azurestaticapps.net

# 2. Trigger API deploy (push any change to master touching src/api/)
git commit --allow-empty -m "chore: trigger API deploy"
git push
# GitHub Actions → Deploy API → build-and-deploy → e2e-smoke

# 3. Manual health check
curl https://<aca_fqdn>/health
# Expected: {"status":"ok","timestamp":"2026-05-09T...Z"}

# 4. Manual exceptions list
curl https://<aca_fqdn>/exceptions/?status=PENDING
# Expected: JSON array (may be empty if no ESCALATE rows in DB)

# 5. React app
# Open https://<swa_hostname> — AAD login → ExceptionQueue renders → polling active
```

---

## Deployment Order (Critical)

1. `terraform apply` first apply (no `swa_hostname`) → get SWA hostname + ACA FQDN
2. Set GitHub Actions secrets (AZURE_CREDENTIALS, ACR_NAME, ACA_NAME, AZURE_RESOURCE_GROUP, AZURE_STATIC_WEB_APPS_API_TOKEN, E2E_API_BASE)
3. `terraform apply` second apply (with `swa_hostname` set) → ALLOW_ORIGINS updated on ACA
4. Push to master → API workflow builds image + deploys + runs E2E tests
5. Push to master → Frontend workflow builds React + deploys to SWA
6. Open SWA URL → verify AAD login → verify ExceptionQueue polls correctly

## GSTACK REVIEW REPORT

| Run | Date | Verdict |
|-----|------|---------|
| — | — | NO REVIEWS YET |
