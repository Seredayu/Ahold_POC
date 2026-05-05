# Phase 0 — Azure Infra + SAP Connectivity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Provision a fully deployable greenfield Azure + Databricks infrastructure and SAP ECC CDC ingestion pipeline that lands raw materials, inventory, and purchase order data into Bronze Unity Catalog Delta tables.

**Architecture:** Modular Terraform (networking → storage → databricks → lakeflow) applied in two stages because the Databricks workspace provider requires the workspace URL from Stage 1 before workspace-level resources can be created in Stage 2. Bronze Auto Loader Python classes subclass a shared base, add audit columns, and write append-only Delta.

**Tech Stack:** Terraform ≥ 1.5, AzureRM provider ~3.0, Databricks provider ~1.0, PySpark 3.5 (Spark 14.3 LTS), pytest, Databricks Asset Bundles (DABs) CLI.

---

## File Map

**New files — Terraform:**
- `infrastructure/terraform/providers.tf` — AzureRM + Databricks provider config
- `infrastructure/terraform/outputs.tf` — workspace URL, storage endpoint, metastore ID
- `infrastructure/terraform/terraform.tfvars.example` — placeholder values, safe to commit
- `infrastructure/terraform/modules/networking/main.tf`
- `infrastructure/terraform/modules/networking/variables.tf`
- `infrastructure/terraform/modules/networking/outputs.tf`
- `infrastructure/terraform/modules/storage/main.tf`
- `infrastructure/terraform/modules/storage/variables.tf`
- `infrastructure/terraform/modules/storage/outputs.tf`
- `infrastructure/terraform/modules/databricks/main.tf`
- `infrastructure/terraform/modules/databricks/variables.tf`
- `infrastructure/terraform/modules/databricks/outputs.tf`
- `infrastructure/terraform/modules/lakeflow/main.tf`
- `infrastructure/terraform/modules/lakeflow/variables.tf`
- `infrastructure/terraform/modules/lakeflow/outputs.tf`

**Modified files — Terraform:**
- `infrastructure/terraform/main.tf` — rewrite to call modules (remove old inline resources)
- `infrastructure/terraform/variables.tf` — expand with all required variables

**New files — Python:**
- `src/medallion/bronze/autoloader.py` — base class
- `src/medallion/bronze/sap_ecc_materials.py`
- `src/medallion/bronze/sap_ecc_inventory.py`
- `src/medallion/bronze/sap_ecc_open_orders.py`
- `tests/unit/test_autoloader.py`
- `tests/unit/test_sap_ecc_materials.py`
- `tests/unit/test_sap_ecc_inventory.py`
- `tests/unit/test_sap_ecc_open_orders.py`
- `tests/conftest.py` — shared Spark session fixture

**New files — Databricks Bundles:**
- `databricks.yml`
- `resources/pipelines/bronze_materials.yml`
- `resources/pipelines/bronze_inventory.yml`
- `resources/pipelines/bronze_open_orders.yml`
- `resources/jobs/lakeflow_trigger.yml`

**Modified:**
- `.gitignore` — add Terraform state, tfvars, pycache

---

## Task 1: Bootstrap — gitignore, providers, tfvars.example

**Files:**
- Modify: `.gitignore`
- Create: `infrastructure/terraform/providers.tf`
- Create: `infrastructure/terraform/terraform.tfvars.example`
- Create: `infrastructure/terraform/modules/networking/` (empty dirs)
- Create: `infrastructure/terraform/modules/storage/`
- Create: `infrastructure/terraform/modules/databricks/`
- Create: `infrastructure/terraform/modules/lakeflow/`

- [ ] **Step 1: Update .gitignore**

Add to `.gitignore` (create if it doesn't exist):

```gitignore
# Terraform
*.tfvars
!*.tfvars.example
.terraform/
.terraform.lock.hcl
terraform.tfstate
terraform.tfstate.backup
crash.log
override.tf
override.tf.json

# Python
__pycache__/
*.pyc
*.pyo
.pytest_cache/
.env
.venv/
```

- [ ] **Step 2: Create providers.tf**

```hcl
# infrastructure/terraform/providers.tf

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }
  backend "azurerm" {
    resource_group_name  = "rg-ahold-poc-tfstate"
    storage_account_name = "saholdpoctfstate"
    container_name       = "tfstate"
    key                  = "ahold-poc.tfstate"
  }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy = true
    }
  }
  subscription_id = var.subscription_id
}

# Stage 1: accounts-level Databricks provider (for metastore)
provider "databricks" {
  alias      = "accounts"
  host       = "https://accounts.azuredatabricks.net"
  account_id = var.databricks_account_id
}

# Stage 2: workspace-level provider (populated after Stage 1 apply)
provider "databricks" {
  alias = "workspace"
  host  = var.workspace_url
}
```

- [ ] **Step 3: Create terraform.tfvars.example**

```hcl
# infrastructure/terraform/terraform.tfvars.example
# Copy to terraform.tfvars and fill in real values. NEVER commit terraform.tfvars.

subscription_id                = "00000000-0000-0000-0000-000000000000"
location                       = "westeurope"
prefix                         = "ahpoc"
resource_group_name            = "rg-ahold-freshness-poc"
storage_account_name           = "saholdpocdata"
expressroute_circuit_id        = "/subscriptions/00000000/resourceGroups/rg-network/providers/Microsoft.Network/expressRouteCircuits/er-ahold"
expressroute_authorization_key = "00000000-0000-0000-0000-000000000000"
databricks_account_id          = "00000000-0000-0000-0000-000000000000"
workspace_url                  = ""   # populated after Stage 1 apply
```

- [ ] **Step 4: Create module directories**

```bash
mkdir -p infrastructure/terraform/modules/networking
mkdir -p infrastructure/terraform/modules/storage
mkdir -p infrastructure/terraform/modules/databricks
mkdir -p infrastructure/terraform/modules/lakeflow
```

- [ ] **Step 5: Commit**

```bash
git add .gitignore infrastructure/terraform/providers.tf infrastructure/terraform/terraform.tfvars.example infrastructure/terraform/modules/
git commit -m "chore: bootstrap terraform module structure and gitignore"
```

---

## Task 2: Networking Module

**Files:**
- Create: `infrastructure/terraform/modules/networking/main.tf`
- Create: `infrastructure/terraform/modules/networking/variables.tf`
- Create: `infrastructure/terraform/modules/networking/outputs.tf`

- [ ] **Step 1: Write variables.tf**

```hcl
# infrastructure/terraform/modules/networking/variables.tf

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
```

- [ ] **Step 2: Write main.tf**

```hcl
# infrastructure/terraform/modules/networking/main.tf

resource "azurerm_virtual_network" "main" {
  name                = "${var.prefix}-vnet"
  address_space       = [var.vnet_address_space]
  location            = var.location
  resource_group_name = var.resource_group_name
}

resource "azurerm_subnet" "databricks_public" {
  name                 = "databricks-public"
  resource_group_name  = var.resource_group_name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = [var.databricks_public_cidr]

  delegation {
    name = "databricks"
    service_delegation {
      name = "Microsoft.Databricks/workspaces"
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/join/action",
        "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
        "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action",
      ]
    }
  }
}

resource "azurerm_subnet" "databricks_private" {
  name                 = "databricks-private"
  resource_group_name  = var.resource_group_name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = [var.databricks_private_cidr]

  delegation {
    name = "databricks"
    service_delegation {
      name = "Microsoft.Databricks/workspaces"
      actions = [
        "Microsoft.Network/virtualNetworks/subnets/join/action",
        "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
        "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action",
      ]
    }
  }
}

resource "azurerm_subnet" "gateway" {
  name                 = "GatewaySubnet"
  resource_group_name  = var.resource_group_name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = [var.gateway_cidr]
}

resource "azurerm_network_security_group" "databricks_public" {
  name                = "${var.prefix}-nsg-public"
  location            = var.location
  resource_group_name = var.resource_group_name
}

resource "azurerm_network_security_group" "databricks_private" {
  name                = "${var.prefix}-nsg-private"
  location            = var.location
  resource_group_name = var.resource_group_name
}

resource "azurerm_subnet_network_security_group_association" "databricks_public" {
  subnet_id                 = azurerm_subnet.databricks_public.id
  network_security_group_id = azurerm_network_security_group.databricks_public.id
}

resource "azurerm_subnet_network_security_group_association" "databricks_private" {
  subnet_id                 = azurerm_subnet.databricks_private.id
  network_security_group_id = azurerm_network_security_group.databricks_private.id
}

resource "azurerm_public_ip" "gateway" {
  name                = "${var.prefix}-er-gw-pip"
  location            = var.location
  resource_group_name = var.resource_group_name
  allocation_method   = "Static"
  sku                 = "Standard"
}

resource "azurerm_virtual_network_gateway" "expressroute" {
  name                = "${var.prefix}-er-gw"
  location            = var.location
  resource_group_name = var.resource_group_name
  type                = "ExpressRoute"
  sku                 = "Standard"

  ip_configuration {
    name                          = "default"
    public_ip_address_id          = azurerm_public_ip.gateway.id
    private_ip_address_allocation = "Dynamic"
    subnet_id                     = azurerm_subnet.gateway.id
  }
}

resource "azurerm_virtual_network_gateway_connection" "expressroute" {
  name                       = "${var.prefix}-er-conn"
  location                   = var.location
  resource_group_name        = var.resource_group_name
  type                       = "ExpressRoute"
  virtual_network_gateway_id = azurerm_virtual_network_gateway.expressroute.id
  express_route_circuit_id   = var.expressroute_circuit_id
  authorization_key          = var.expressroute_authorization_key
}
```

- [ ] **Step 3: Write outputs.tf**

```hcl
# infrastructure/terraform/modules/networking/outputs.tf

output "vnet_id" {
  value = azurerm_virtual_network.main.id
}

output "vnet_name" {
  value = azurerm_virtual_network.main.name
}

output "databricks_public_subnet_id" {
  value = azurerm_subnet.databricks_public.id
}

output "databricks_public_subnet_name" {
  value = azurerm_subnet.databricks_public.name
}

output "databricks_private_subnet_id" {
  value = azurerm_subnet.databricks_private.id
}

output "databricks_private_subnet_name" {
  value = azurerm_subnet.databricks_private.name
}

output "public_nsg_association_id" {
  value = azurerm_subnet_network_security_group_association.databricks_public.id
}

output "private_nsg_association_id" {
  value = azurerm_subnet_network_security_group_association.databricks_private.id
}
```

- [ ] **Step 4: Validate the networking module in isolation**

```bash
cd infrastructure/terraform
terraform init -backend=false
terraform validate
```

Expected: `Success! The configuration is valid.`

- [ ] **Step 5: Commit**

```bash
git add infrastructure/terraform/modules/networking/
git commit -m "feat(infra): add networking module — VNet, subnets, ExpressRoute gateway"
```

---

## Task 3: Storage Module

**Files:**
- Create: `infrastructure/terraform/modules/storage/main.tf`
- Create: `infrastructure/terraform/modules/storage/variables.tf`
- Create: `infrastructure/terraform/modules/storage/outputs.tf`

- [ ] **Step 1: Write variables.tf**

```hcl
# infrastructure/terraform/modules/storage/variables.tf

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
```

- [ ] **Step 2: Write main.tf**

```hcl
# infrastructure/terraform/modules/storage/main.tf

resource "azurerm_storage_account" "datalake" {
  name                     = var.storage_account_name
  resource_group_name      = var.resource_group_name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true

  network_rules {
    default_action             = "Deny"
    virtual_network_subnet_ids = [var.private_subnet_id]
    bypass                     = ["AzureServices"]
  }
}

resource "azurerm_storage_container" "bronze" {
  name                  = "bronze"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "silver" {
  name                  = "silver"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "gold" {
  name                  = "gold"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "gold_edi_outbound" {
  name                  = "gold-edi-outbound"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

resource "azurerm_private_endpoint" "datalake_dfs" {
  name                = "${var.prefix}-storage-pe"
  location            = var.location
  resource_group_name = var.resource_group_name
  subnet_id           = var.private_subnet_id

  private_service_connection {
    name                           = "${var.prefix}-storage-psc"
    private_connection_resource_id = azurerm_storage_account.datalake.id
    subresource_names              = ["dfs"]
    is_manual_connection           = false
  }
}
```

- [ ] **Step 3: Write outputs.tf**

```hcl
# infrastructure/terraform/modules/storage/outputs.tf

output "storage_account_id" {
  value = azurerm_storage_account.datalake.id
}

output "storage_account_name" {
  value = azurerm_storage_account.datalake.name
}

output "dfs_endpoint" {
  value = azurerm_storage_account.datalake.primary_dfs_endpoint
}

output "primary_access_key" {
  value     = azurerm_storage_account.datalake.primary_access_key
  sensitive = true
}
```

- [ ] **Step 4: Validate**

```bash
cd infrastructure/terraform
terraform validate
```

Expected: `Success! The configuration is valid.`

- [ ] **Step 5: Commit**

```bash
git add infrastructure/terraform/modules/storage/
git commit -m "feat(infra): add storage module — ADLS Gen2, containers, private endpoint"
```

---

## Task 4: Databricks Module

**Files:**
- Create: `infrastructure/terraform/modules/databricks/main.tf`
- Create: `infrastructure/terraform/modules/databricks/variables.tf`
- Create: `infrastructure/terraform/modules/databricks/outputs.tf`

- [ ] **Step 1: Write variables.tf**

```hcl
# infrastructure/terraform/modules/databricks/variables.tf

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
```

- [ ] **Step 2: Write main.tf**

```hcl
# infrastructure/terraform/modules/databricks/main.tf

data "azurerm_client_config" "current" {}

resource "azurerm_databricks_workspace" "main" {
  name                = "${var.prefix}-dbw"
  resource_group_name = var.resource_group_name
  location            = var.location
  sku                 = "premium"

  custom_parameters {
    virtual_network_id                                   = var.vnet_id
    public_subnet_name                                   = var.public_subnet_name
    private_subnet_name                                  = var.private_subnet_name
    public_subnet_network_security_group_association_id  = var.public_nsg_association_id
    private_subnet_network_security_group_association_id = var.private_nsg_association_id
    no_public_ip                                         = true
  }
}

resource "azurerm_key_vault" "main" {
  name                = "${var.prefix}-kv"
  location            = var.location
  resource_group_name = var.resource_group_name
  tenant_id           = data.azurerm_client_config.current.tenant_id
  sku_name            = var.key_vault_sku

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = azurerm_databricks_workspace.main.storage_account_identity[0].principal_id

    secret_permissions = ["Get", "List"]
  }

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id

    secret_permissions = ["Get", "List", "Set", "Delete", "Purge", "Recover"]
  }
}

resource "azurerm_role_assignment" "databricks_storage" {
  scope                = var.storage_account_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_workspace.main.storage_account_identity[0].principal_id
}

resource "databricks_metastore" "main" {
  provider      = databricks.accounts
  name          = "${var.prefix}-metastore"
  region        = var.location
  storage_root  = "abfss://gold@${var.storage_account_name}.dfs.core.windows.net/metastore"
  force_destroy = true
}

resource "databricks_metastore_assignment" "main" {
  provider     = databricks.accounts
  metastore_id = databricks_metastore.main.id
  workspace_id = azurerm_databricks_workspace.main.workspace_id
}

resource "databricks_catalog" "bronze" {
  provider   = databricks.workspace
  name       = "bronze"
  comment    = "Append-only CDC landing zone from SAP ECC"
  depends_on = [databricks_metastore_assignment.main]
}

resource "databricks_catalog" "silver" {
  provider   = databricks.workspace
  name       = "silver"
  comment    = "DLT-transformed, validated data"
  depends_on = [databricks_metastore_assignment.main]
}

resource "databricks_catalog" "gold" {
  provider   = databricks.workspace
  name       = "gold"
  comment    = "Analytical aggregates and engine outputs"
  depends_on = [databricks_metastore_assignment.main]
}

resource "databricks_catalog" "feature_store" {
  provider   = databricks.workspace
  name       = "feature_store"
  comment    = "Reusable ML features"
  depends_on = [databricks_metastore_assignment.main]
}

resource "databricks_cluster_policy" "freshness_poc" {
  provider = databricks.workspace
  name     = "freshness-poc-job-cluster"
  definition = jsonencode({
    "spark_version"         = { "type" = "fixed", "value" = "14.3.x-scala2.12" }
    "node_type_id"          = { "type" = "fixed", "value" = "Standard_DS3_v2" }
    "autoscale.min_workers" = { "type" = "fixed", "value" = 2 }
    "autoscale.max_workers" = { "type" = "fixed", "value" = 8 }
    "data_security_mode"    = { "type" = "fixed", "value" = "SINGLE_USER" }
  })
  depends_on = [databricks_metastore_assignment.main]
}

resource "databricks_secret_scope" "kv" {
  provider = databricks.workspace
  name     = "kv-secrets"

  keyvault_metadata {
    resource_id = azurerm_key_vault.main.id
    dns_name    = azurerm_key_vault.main.vault_uri
  }

  depends_on = [azurerm_key_vault.main]
}
```

- [ ] **Step 3: Add required_providers block for alias config in module**

```hcl
# Add at the top of infrastructure/terraform/modules/databricks/main.tf (before data block):

terraform {
  required_providers {
    azurerm = {
      source = "hashicorp/azurerm"
    }
    databricks = {
      source                = "databricks/databricks"
      configuration_aliases = [databricks.accounts, databricks.workspace]
    }
  }
}
```

- [ ] **Step 4: Write outputs.tf**

```hcl
# infrastructure/terraform/modules/databricks/outputs.tf

output "workspace_url" {
  value = azurerm_databricks_workspace.main.workspace_url
}

output "workspace_id" {
  value = azurerm_databricks_workspace.main.workspace_id
}

output "metastore_id" {
  value = databricks_metastore.main.id
}

output "cluster_policy_id" {
  value = databricks_cluster_policy.freshness_poc.id
}

output "key_vault_id" {
  value = azurerm_key_vault.main.id
}

output "key_vault_uri" {
  value = azurerm_key_vault.main.vault_uri
}

output "secret_scope_name" {
  value = databricks_secret_scope.kv.name
}

output "managed_identity_principal_id" {
  value = azurerm_databricks_workspace.main.storage_account_identity[0].principal_id
}
```

- [ ] **Step 5: Validate**

```bash
cd infrastructure/terraform && terraform validate
```

Expected: `Success! The configuration is valid.`

- [ ] **Step 6: Commit**

```bash
git add infrastructure/terraform/modules/databricks/
git commit -m "feat(infra): add databricks module — workspace, Unity Catalog, Key Vault, cluster policy"
```

---

## Task 5: Lakeflow Module

**Files:**
- Create: `infrastructure/terraform/modules/lakeflow/main.tf`
- Create: `infrastructure/terraform/modules/lakeflow/variables.tf`
- Create: `infrastructure/terraform/modules/lakeflow/outputs.tf`

- [ ] **Step 1: Write variables.tf**

```hcl
# infrastructure/terraform/modules/lakeflow/variables.tf

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
```

- [ ] **Step 2: Write main.tf**

```hcl
# infrastructure/terraform/modules/lakeflow/main.tf

terraform {
  required_providers {
    databricks = {
      source                = "databricks/databricks"
      configuration_aliases = [databricks.workspace]
    }
  }
}

resource "databricks_connection" "sap_ecc" {
  provider        = databricks.workspace
  name            = "sap-ecc-connection"
  connection_type = "SAP_ERP"
  comment         = "SAP ECC 6.0 RFC connection for Lakeflow Connect CDC"

  options = {
    host         = "{{secrets/${var.secret_scope_name}/sap-ecc-host}}"
    systemNumber = "{{secrets/${var.secret_scope_name}/sap-ecc-sysnr}}"
    clientId     = "{{secrets/${var.secret_scope_name}/sap-ecc-client}}"
    user         = "{{secrets/${var.secret_scope_name}/sap-ecc-username}}"
    password     = "{{secrets/${var.secret_scope_name}/sap-ecc-password}}"
  }
}

resource "databricks_pipeline" "lakeflow_sap_bronze" {
  provider = databricks.workspace
  name     = "${var.prefix}-lakeflow-sap-bronze"
  target   = var.bronze_catalog
  channel  = "PREVIEW"

  cluster {
    policy_id = var.cluster_policy_id
    label     = "default"
  }

  ingestion_definition {
    connection_name = databricks_connection.sap_ecc.name

    table {
      source_schema       = "SAP_ECC"
      source_table        = "MARA"
      destination_catalog = var.bronze_catalog
      destination_schema  = "sap"
      destination_table   = "materials_mara"
    }

    table {
      source_schema       = "SAP_ECC"
      source_table        = "MARC"
      destination_catalog = var.bronze_catalog
      destination_schema  = "sap"
      destination_table   = "materials_marc"
    }

    table {
      source_schema       = "SAP_ECC"
      source_table        = "MSEG"
      destination_catalog = var.bronze_catalog
      destination_schema  = "sap"
      destination_table   = "inventory_mseg"
    }

    table {
      source_schema       = "SAP_ECC"
      source_table        = "MKPF"
      destination_catalog = var.bronze_catalog
      destination_schema  = "sap"
      destination_table   = "inventory_mkpf"
    }

    table {
      source_schema       = "SAP_ECC"
      source_table        = "EKKO"
      destination_catalog = var.bronze_catalog
      destination_schema  = "sap"
      destination_table   = "open_orders_ekko"
    }

    table {
      source_schema       = "SAP_ECC"
      source_table        = "EKPO"
      destination_catalog = var.bronze_catalog
      destination_schema  = "sap"
      destination_table   = "open_orders_ekpo"
    }
  }
}
```

- [ ] **Step 3: Write outputs.tf**

```hcl
# infrastructure/terraform/modules/lakeflow/outputs.tf

output "pipeline_id" {
  value = databricks_pipeline.lakeflow_sap_bronze.id
}

output "connection_name" {
  value = databricks_connection.sap_ecc.name
}
```

- [ ] **Step 4: Validate**

```bash
cd infrastructure/terraform && terraform validate
```

Expected: `Success! The configuration is valid.`

- [ ] **Step 5: Commit**

```bash
git add infrastructure/terraform/modules/lakeflow/
git commit -m "feat(infra): add lakeflow module — SAP ECC CDC connection and DLT pipeline"
```

---

## Task 6: Root Module — Wire Everything Together

**Files:**
- Modify: `infrastructure/terraform/main.tf` (full rewrite)
- Modify: `infrastructure/terraform/variables.tf` (expand)
- Create: `infrastructure/terraform/outputs.tf`

- [ ] **Step 1: Rewrite variables.tf**

```hcl
# infrastructure/terraform/variables.tf

variable "subscription_id" {
  type = string
}

variable "location" {
  type    = string
  default = "westeurope"
}

variable "prefix" {
  type        = string
  default     = "ahpoc"
  description = "Short prefix for resource names, 2-6 chars"
}

variable "resource_group_name" {
  type    = string
  default = "rg-ahold-freshness-poc"
}

variable "storage_account_name" {
  type    = string
  default = "saholdpocdata"
}

variable "expressroute_circuit_id" {
  type      = string
  sensitive = true
}

variable "expressroute_authorization_key" {
  type      = string
  sensitive = true
}

variable "databricks_account_id" {
  type      = string
  sensitive = true
}

variable "workspace_url" {
  type        = string
  default     = ""
  description = "Populated after Stage 1 apply. Set in terraform.tfvars before Stage 2."
}
```

- [ ] **Step 2: Rewrite main.tf**

```hcl
# infrastructure/terraform/main.tf

resource "azurerm_resource_group" "main" {
  name     = var.resource_group_name
  location = var.location
}

module "networking" {
  source = "./modules/networking"

  prefix                         = var.prefix
  resource_group_name            = azurerm_resource_group.main.name
  location                       = var.location
  expressroute_circuit_id        = var.expressroute_circuit_id
  expressroute_authorization_key = var.expressroute_authorization_key
}

module "storage" {
  source = "./modules/storage"

  prefix               = var.prefix
  resource_group_name  = azurerm_resource_group.main.name
  location             = var.location
  storage_account_name = var.storage_account_name
  private_subnet_id    = module.networking.databricks_private_subnet_id
}

module "databricks" {
  source = "./modules/databricks"

  providers = {
    azurerm            = azurerm
    databricks.accounts  = databricks.accounts
    databricks.workspace = databricks.workspace
  }

  prefix                     = var.prefix
  resource_group_name        = azurerm_resource_group.main.name
  location                   = var.location
  vnet_id                    = module.networking.vnet_id
  public_subnet_name         = module.networking.databricks_public_subnet_name
  private_subnet_name        = module.networking.databricks_private_subnet_name
  public_nsg_association_id  = module.networking.public_nsg_association_id
  private_nsg_association_id = module.networking.private_nsg_association_id
  storage_account_id         = module.storage.storage_account_id
  storage_account_name       = module.storage.storage_account_name
  databricks_account_id      = var.databricks_account_id
}

module "lakeflow" {
  source = "./modules/lakeflow"

  providers = {
    databricks.workspace = databricks.workspace
  }

  prefix               = var.prefix
  cluster_policy_id    = module.databricks.cluster_policy_id
  secret_scope_name    = module.databricks.secret_scope_name
  storage_account_name = module.storage.storage_account_name

  depends_on = [module.databricks]
}
```

- [ ] **Step 3: Write outputs.tf**

```hcl
# infrastructure/terraform/outputs.tf

output "workspace_url" {
  description = "Databricks workspace URL — copy to terraform.tfvars as workspace_url for Stage 2"
  value       = module.databricks.workspace_url
}

output "storage_dfs_endpoint" {
  value = module.storage.dfs_endpoint
}

output "key_vault_uri" {
  value = module.databricks.key_vault_uri
}

output "metastore_id" {
  value = module.databricks.metastore_id
}

output "lakeflow_pipeline_id" {
  value = module.lakeflow.pipeline_id
}
```

- [ ] **Step 4: Run terraform init and validate**

```bash
cd infrastructure/terraform
terraform init -backend=false
terraform validate
```

Expected: `Success! The configuration is valid.`

- [ ] **Step 5: Run terraform fmt**

```bash
terraform fmt -recursive
```

Expected: Lists any files reformatted, exits 0.

- [ ] **Step 6: Commit**

```bash
git add infrastructure/terraform/main.tf infrastructure/terraform/variables.tf infrastructure/terraform/outputs.tf
git commit -m "feat(infra): wire all terraform modules in root — ready for terraform apply"
```

---

## Task 7: Auto Loader Base Class (TDD)

**Files:**
- Create: `tests/conftest.py`
- Create: `tests/unit/test_autoloader.py`
- Create: `src/medallion/bronze/autoloader.py`

- [ ] **Step 1: Write conftest.py**

```python
# tests/conftest.py

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("ahold-poc-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
```

- [ ] **Step 2: Write failing tests**

```python
# tests/unit/test_autoloader.py

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, StringType, StructField, StructType

from src.medallion.bronze.autoloader import AutoLoaderBase


class _ConcreteLoader(AutoLoaderBase):
    def target_schema(self) -> StructType:
        return StructType([
            StructField("id", StringType(), True),
            StructField("value", FloatType(), True),
        ])


def test_target_table_name_formed_correctly(spark):
    loader = _ConcreteLoader(
        spark=spark,
        source_path="/tmp/src",
        checkpoint_path="/tmp/ckpt",
        catalog="bronze",
        schema="sap",
        table="materials",
    )
    assert loader.target_table == "bronze.sap.materials"


def test_add_audit_columns_adds_all_three(spark):
    loader = _ConcreteLoader(
        spark=spark,
        source_path="/tmp/src",
        checkpoint_path="/tmp/ckpt",
        catalog="bronze",
        schema="sap",
        table="materials",
    )
    df = spark.createDataFrame([("a", 1.0)], ["id", "value"])
    result = loader.add_audit_columns(df)

    assert "_ingested_at" in result.columns
    assert "_source_file" in result.columns
    assert "_cdc_operation" in result.columns


def test_add_audit_columns_ingested_at_is_not_null(spark):
    loader = _ConcreteLoader(
        spark=spark,
        source_path="/tmp/src",
        checkpoint_path="/tmp/ckpt",
        catalog="bronze",
        schema="sap",
        table="materials",
    )
    df = spark.createDataFrame([("a", 1.0)], ["id", "value"])
    result = loader.add_audit_columns(df)
    row = result.collect()[0]
    assert row["_ingested_at"] is not None


def test_target_schema_must_be_implemented(spark):
    with pytest.raises(TypeError):
        AutoLoaderBase(  # abstract — cannot instantiate
            spark=spark,
            source_path="/tmp/src",
            checkpoint_path="/tmp/ckpt",
            catalog="bronze",
            schema="sap",
            table="test",
        )
```

- [ ] **Step 3: Run tests — verify they fail**

```bash
pytest tests/unit/test_autoloader.py -v
```

Expected: `ImportError` or `ModuleNotFoundError` — `autoloader` does not exist yet.

- [ ] **Step 4: Implement AutoLoaderBase**

```python
# src/medallion/bronze/autoloader.py

from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


class AutoLoaderBase(ABC):
    def __init__(
        self,
        spark: SparkSession,
        source_path: str,
        checkpoint_path: str,
        catalog: str,
        schema: str,
        table: str,
    ):
        self.spark = spark
        self.source_path = source_path
        self.checkpoint_path = checkpoint_path
        self.target_table = f"{catalog}.{schema}.{table}"

    @abstractmethod
    def target_schema(self) -> StructType:
        ...

    def add_audit_columns(self, df: DataFrame) -> DataFrame:
        df = df.withColumn("_ingested_at", F.current_timestamp())
        if "_metadata" in df.columns:
            df = df.withColumn("_source_file", F.col("_metadata.file_path"))
            df = df.withColumn(
                "_cdc_operation",
                F.col("_metadata.file_modification_time").cast("string"),
            )
        else:
            df = df.withColumn("_source_file", F.lit(""))
            df = df.withColumn("_cdc_operation", F.lit(""))
        return df

    def read_stream(self) -> DataFrame:
        return (
            self.spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .schema(self.target_schema())
            .load(self.source_path)
        )

    def run(self) -> None:
        df = self.read_stream()
        df = self.add_audit_columns(df)
        (
            df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", self.checkpoint_path)
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
            .toTable(self.target_table)
            .awaitTermination()
        )
```

- [ ] **Step 5: Run tests — verify they pass**

```bash
pytest tests/unit/test_autoloader.py -v
```

Expected:
```
PASSED tests/unit/test_autoloader.py::test_target_table_name_formed_correctly
PASSED tests/unit/test_autoloader.py::test_add_audit_columns_adds_all_three
PASSED tests/unit/test_autoloader.py::test_add_audit_columns_ingested_at_is_not_null
PASSED tests/unit/test_autoloader.py::test_target_schema_must_be_implemented
4 passed in <5s
```

- [ ] **Step 6: Commit**

```bash
git add tests/conftest.py tests/unit/test_autoloader.py src/medallion/bronze/autoloader.py
git commit -m "feat(bronze): add AutoLoaderBase with audit columns — TDD"
```

---

## Task 8: SAP ECC Materials Pipeline (TDD)

**Files:**
- Create: `tests/unit/test_sap_ecc_materials.py`
- Create: `src/medallion/bronze/sap_ecc_materials.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/test_sap_ecc_materials.py

from pyspark.sql.types import (
    DateType, DecimalType, StringType, StructField, StructType,
)

from src.medallion.bronze.sap_ecc_materials import SAPECCMaterialsLoader


def test_loader_sets_correct_target_table(spark):
    loader = SAPECCMaterialsLoader(spark=spark)
    assert loader.target_table == "bronze.sap.materials"


def test_target_schema_contains_matnr(spark):
    loader = SAPECCMaterialsLoader(spark=spark)
    schema = loader.target_schema()
    field_names = [f.name for f in schema.fields]
    assert "MATNR" in field_names


def test_target_schema_contains_werks(spark):
    loader = SAPECCMaterialsLoader(spark=spark)
    schema = loader.target_schema()
    field_names = [f.name for f in schema.fields]
    assert "WERKS" in field_names


def test_target_schema_contains_shelf_life_fields(spark):
    loader = SAPECCMaterialsLoader(spark=spark)
    schema = loader.target_schema()
    field_names = [f.name for f in schema.fields]
    assert "MHDRZ" in field_names  # shelf life (days)
    assert "IPRKZ" in field_names  # period indicator for shelf life


def test_add_audit_columns_present_after_transform(spark):
    loader = SAPECCMaterialsLoader(spark=spark)
    df = spark.createDataFrame(
        [("000000000000001234", "NL01")],
        ["MATNR", "WERKS"],
    )
    result = loader.add_audit_columns(df)
    assert "_ingested_at" in result.columns
```

- [ ] **Step 2: Run tests — verify they fail**

```bash
pytest tests/unit/test_sap_ecc_materials.py -v
```

Expected: `ImportError` — `sap_ecc_materials` does not exist.

- [ ] **Step 3: Implement SAPECCMaterialsLoader**

```python
# src/medallion/bronze/sap_ecc_materials.py

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType, DecimalType, StringType, StructField, StructType,
)

from .autoloader import AutoLoaderBase

_DEFAULT_SOURCE = "abfss://bronze@{storage}.dfs.core.windows.net/sap/materials/"
_DEFAULT_CKPT   = "abfss://bronze@{storage}.dfs.core.windows.net/_checkpoints/sap_materials/"


class SAPECCMaterialsLoader(AutoLoaderBase):
    """Bronze Auto Loader for MARA + MARC material master CDC feed.

    Lands to bronze.sap.materials with audit columns.
    Key columns: MATNR (material number), WERKS (plant).
    """

    def __init__(
        self,
        spark: SparkSession,
        storage_account: str = "saholdpocdata",
        catalog: str = "bronze",
    ):
        super().__init__(
            spark=spark,
            source_path=_DEFAULT_SOURCE.format(storage=storage_account),
            checkpoint_path=_DEFAULT_CKPT.format(storage=storage_account),
            catalog=catalog,
            schema="sap",
            table="materials",
        )

    def target_schema(self) -> StructType:
        return StructType([
            StructField("MATNR", StringType(), True),   # Material number
            StructField("WERKS", StringType(), True),   # Plant
            StructField("MAKTX", StringType(), True),   # Material description
            StructField("MTART", StringType(), True),   # Material type
            StructField("MATKL", StringType(), True),   # Material group
            StructField("MEINS", StringType(), True),   # Base unit of measure
            StructField("MHDRZ", DecimalType(5, 0), True),  # Shelf life (days)
            StructField("IPRKZ", StringType(), True),   # Period indicator for shelf life
            StructField("MHDLP", DecimalType(5, 0), True),  # Min remaining shelf life
            StructField("LAEDA", DateType(), True),     # Last change date
            StructField("ERSDA", DateType(), True),     # Creation date
        ])
```

- [ ] **Step 4: Run tests — verify they pass**

```bash
pytest tests/unit/test_sap_ecc_materials.py -v
```

Expected:
```
PASSED tests/unit/test_sap_ecc_materials.py::test_loader_sets_correct_target_table
PASSED tests/unit/test_sap_ecc_materials.py::test_target_schema_contains_matnr
PASSED tests/unit/test_sap_ecc_materials.py::test_target_schema_contains_werks
PASSED tests/unit/test_sap_ecc_materials.py::test_target_schema_contains_shelf_life_fields
PASSED tests/unit/test_sap_ecc_materials.py::test_add_audit_columns_present_after_transform
5 passed in <5s
```

- [ ] **Step 5: Commit**

```bash
git add tests/unit/test_sap_ecc_materials.py src/medallion/bronze/sap_ecc_materials.py
git commit -m "feat(bronze): add SAPECCMaterialsLoader for MARA/MARC feed — TDD"
```

---

## Task 9: SAP ECC Inventory Pipeline (TDD)

**Files:**
- Create: `tests/unit/test_sap_ecc_inventory.py`
- Create: `src/medallion/bronze/sap_ecc_inventory.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/test_sap_ecc_inventory.py

from src.medallion.bronze.sap_ecc_inventory import SAPECCInventoryLoader


def test_loader_sets_correct_target_table(spark):
    loader = SAPECCInventoryLoader(spark=spark)
    assert loader.target_table == "bronze.sap.inventory"


def test_target_schema_contains_key_columns(spark):
    loader = SAPECCInventoryLoader(spark=spark)
    field_names = [f.name for f in loader.target_schema().fields]
    assert "MBLNR" in field_names  # material document number
    assert "ZEILE" in field_names  # line item


def test_target_schema_contains_movement_type(spark):
    loader = SAPECCInventoryLoader(spark=spark)
    field_names = [f.name for f in loader.target_schema().fields]
    assert "BWART" in field_names  # movement type


def test_target_schema_contains_quantity(spark):
    loader = SAPECCInventoryLoader(spark=spark)
    field_names = [f.name for f in loader.target_schema().fields]
    assert "MENGE" in field_names  # quantity
```

- [ ] **Step 2: Run tests — verify they fail**

```bash
pytest tests/unit/test_sap_ecc_inventory.py -v
```

Expected: `ImportError`.

- [ ] **Step 3: Implement SAPECCInventoryLoader**

```python
# src/medallion/bronze/sap_ecc_inventory.py

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType, DecimalType, StringType, StructField, StructType, TimestampType,
)

from .autoloader import AutoLoaderBase

_DEFAULT_SOURCE = "abfss://bronze@{storage}.dfs.core.windows.net/sap/inventory/"
_DEFAULT_CKPT   = "abfss://bronze@{storage}.dfs.core.windows.net/_checkpoints/sap_inventory/"


class SAPECCInventoryLoader(AutoLoaderBase):
    """Bronze Auto Loader for MSEG + MKPF goods movement CDC feed.

    Lands to bronze.sap.inventory with audit columns.
    Key columns: MBLNR (material document), ZEILE (line item).
    """

    def __init__(
        self,
        spark: SparkSession,
        storage_account: str = "saholdpocdata",
        catalog: str = "bronze",
    ):
        super().__init__(
            spark=spark,
            source_path=_DEFAULT_SOURCE.format(storage=storage_account),
            checkpoint_path=_DEFAULT_CKPT.format(storage=storage_account),
            catalog=catalog,
            schema="sap",
            table="inventory",
        )

    def target_schema(self) -> StructType:
        return StructType([
            StructField("MBLNR", StringType(), True),         # Material document number
            StructField("ZEILE", StringType(), True),         # Line item
            StructField("MATNR", StringType(), True),         # Material number
            StructField("WERKS", StringType(), True),         # Plant
            StructField("LGORT", StringType(), True),         # Storage location
            StructField("BWART", StringType(), True),         # Movement type
            StructField("MENGE", DecimalType(13, 3), True),   # Quantity
            StructField("MEINS", StringType(), True),         # Unit of measure
            StructField("BUDAT", DateType(), True),           # Posting date
            StructField("CPUDT", DateType(), True),           # Entry date
            StructField("CPUTM", TimestampType(), True),      # Entry time
        ])
```

- [ ] **Step 4: Run tests — verify they pass**

```bash
pytest tests/unit/test_sap_ecc_inventory.py -v
```

Expected: `4 passed in <5s`

- [ ] **Step 5: Commit**

```bash
git add tests/unit/test_sap_ecc_inventory.py src/medallion/bronze/sap_ecc_inventory.py
git commit -m "feat(bronze): add SAPECCInventoryLoader for MSEG/MKPF feed — TDD"
```

---

## Task 10: SAP ECC Open Orders Pipeline (TDD)

**Files:**
- Create: `tests/unit/test_sap_ecc_open_orders.py`
- Create: `src/medallion/bronze/sap_ecc_open_orders.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/test_sap_ecc_open_orders.py

from src.medallion.bronze.sap_ecc_open_orders import SAPECCOpenOrdersLoader


def test_loader_sets_correct_target_table(spark):
    loader = SAPECCOpenOrdersLoader(spark=spark)
    assert loader.target_table == "bronze.sap.open_orders"


def test_target_schema_contains_key_columns(spark):
    loader = SAPECCOpenOrdersLoader(spark=spark)
    field_names = [f.name for f in loader.target_schema().fields]
    assert "EBELN" in field_names  # purchase order number
    assert "EBELP" in field_names  # line item


def test_target_schema_contains_vendor(spark):
    loader = SAPECCOpenOrdersLoader(spark=spark)
    field_names = [f.name for f in loader.target_schema().fields]
    assert "LIFNR" in field_names  # vendor number


def test_target_schema_contains_delivery_date(spark):
    loader = SAPECCOpenOrdersLoader(spark=spark)
    field_names = [f.name for f in loader.target_schema().fields]
    assert "EINDT" in field_names  # delivery date


def test_target_schema_contains_quantity_and_value(spark):
    loader = SAPECCOpenOrdersLoader(spark=spark)
    field_names = [f.name for f in loader.target_schema().fields]
    assert "MENGE" in field_names   # order quantity
    assert "NETPR" in field_names   # net price
```

- [ ] **Step 2: Run tests — verify they fail**

```bash
pytest tests/unit/test_sap_ecc_open_orders.py -v
```

Expected: `ImportError`.

- [ ] **Step 3: Implement SAPECCOpenOrdersLoader**

```python
# src/medallion/bronze/sap_ecc_open_orders.py

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType, DecimalType, StringType, StructField, StructType,
)

from .autoloader import AutoLoaderBase

_DEFAULT_SOURCE = "abfss://bronze@{storage}.dfs.core.windows.net/sap/open_orders/"
_DEFAULT_CKPT   = "abfss://bronze@{storage}.dfs.core.windows.net/_checkpoints/sap_open_orders/"


class SAPECCOpenOrdersLoader(AutoLoaderBase):
    """Bronze Auto Loader for EKKO + EKPO purchase order CDC feed.

    Lands to bronze.sap.open_orders with audit columns.
    Key columns: EBELN (PO number), EBELP (line item).
    """

    def __init__(
        self,
        spark: SparkSession,
        storage_account: str = "saholdpocdata",
        catalog: str = "bronze",
    ):
        super().__init__(
            spark=spark,
            source_path=_DEFAULT_SOURCE.format(storage=storage_account),
            checkpoint_path=_DEFAULT_CKPT.format(storage=storage_account),
            catalog=catalog,
            schema="sap",
            table="open_orders",
        )

    def target_schema(self) -> StructType:
        return StructType([
            StructField("EBELN", StringType(), True),          # PO number
            StructField("EBELP", StringType(), True),          # PO line item
            StructField("MATNR", StringType(), True),          # Material number
            StructField("WERKS", StringType(), True),          # Plant
            StructField("LIFNR", StringType(), True),          # Vendor number
            StructField("MENGE", DecimalType(13, 3), True),    # Order quantity
            StructField("MEINS", StringType(), True),          # Unit of measure
            StructField("NETPR", DecimalType(11, 2), True),    # Net price
            StructField("WAERS", StringType(), True),          # Currency
            StructField("EINDT", DateType(), True),            # Delivery date
            StructField("BEDAT", DateType(), True),            # PO date
            StructField("EKORG", StringType(), True),          # Purchasing org
            StructField("EKGRP", StringType(), True),          # Purchasing group
        ])
```

- [ ] **Step 4: Run all unit tests — verify they all pass**

```bash
pytest tests/unit/ -v
```

Expected:
```
PASSED tests/unit/test_autoloader.py::test_target_table_name_formed_correctly
PASSED tests/unit/test_autoloader.py::test_add_audit_columns_adds_all_three
PASSED tests/unit/test_autoloader.py::test_add_audit_columns_ingested_at_is_not_null
PASSED tests/unit/test_autoloader.py::test_target_schema_must_be_implemented
PASSED tests/unit/test_sap_ecc_materials.py::test_loader_sets_correct_target_table
PASSED tests/unit/test_sap_ecc_materials.py::test_target_schema_contains_matnr
PASSED tests/unit/test_sap_ecc_materials.py::test_target_schema_contains_werks
PASSED tests/unit/test_sap_ecc_materials.py::test_target_schema_contains_shelf_life_fields
PASSED tests/unit/test_sap_ecc_materials.py::test_add_audit_columns_present_after_transform
PASSED tests/unit/test_sap_ecc_inventory.py::test_loader_sets_correct_target_table
PASSED tests/unit/test_sap_ecc_inventory.py::test_target_schema_contains_key_columns
PASSED tests/unit/test_sap_ecc_inventory.py::test_target_schema_contains_movement_type
PASSED tests/unit/test_sap_ecc_inventory.py::test_target_schema_contains_quantity
PASSED tests/unit/test_sap_ecc_open_orders.py::test_loader_sets_correct_target_table
PASSED tests/unit/test_sap_ecc_open_orders.py::test_target_schema_contains_key_columns
PASSED tests/unit/test_sap_ecc_open_orders.py::test_target_schema_contains_vendor
PASSED tests/unit/test_sap_ecc_open_orders.py::test_target_schema_contains_delivery_date
PASSED tests/unit/test_sap_ecc_open_orders.py::test_target_schema_contains_quantity_and_value
18 passed
```

- [ ] **Step 5: Commit**

```bash
git add tests/unit/test_sap_ecc_open_orders.py src/medallion/bronze/sap_ecc_open_orders.py
git commit -m "feat(bronze): add SAPECCOpenOrdersLoader for EKKO/EKPO feed — TDD"
```

---

## Task 11: Databricks Asset Bundles

**Files:**
- Create: `databricks.yml`
- Create: `resources/pipelines/bronze_materials.yml`
- Create: `resources/pipelines/bronze_inventory.yml`
- Create: `resources/pipelines/bronze_open_orders.yml`
- Create: `resources/jobs/lakeflow_trigger.yml`

- [ ] **Step 1: Create bundle root**

```bash
mkdir -p resources/pipelines resources/jobs
```

- [ ] **Step 2: Write databricks.yml**

```yaml
# databricks.yml

bundle:
  name: ahold-freshness-poc

artifacts:
  default:
    type: whl
    build: pip wheel src/ -w dist/
    path: dist/

resources:
  pipelines:
    bronze_materials:
      source: resources/pipelines/bronze_materials.yml
    bronze_inventory:
      source: resources/pipelines/bronze_inventory.yml
    bronze_open_orders:
      source: resources/pipelines/bronze_open_orders.yml
  jobs:
    lakeflow_trigger:
      source: resources/jobs/lakeflow_trigger.yml

variables:
  workspace_url:
    description: "Databricks workspace URL (from terraform output)"
  cluster_policy_id:
    description: "Cluster policy ID (from terraform output)"
  storage_account:
    description: "ADLS Gen2 storage account name"
    default: "saholdpocdata"
  schedule_enabled:
    description: "Set to true to unpause the daily schedule"
    default: "false"
  lakeflow_pipeline_id:
    description: "Terraform-managed Lakeflow Connect pipeline ID (from terraform output lakeflow_pipeline_id)"

targets:
  dev:
    mode: development
    workspace:
      host: ${var.workspace_url}
    variables:
      schedule_enabled: "false"
      storage_account: "saholdpocdata"

  staging:
    workspace:
      host: ${var.workspace_url}
    variables:
      schedule_enabled: "true"
      storage_account: "saholdpocdata"

  prod:
    mode: production
    workspace:
      host: ${var.workspace_url}
    variables:
      schedule_enabled: "true"
      storage_account: "saholdpocdata"
```

- [ ] **Step 3: Write bronze_materials.yml**

```yaml
# resources/pipelines/bronze_materials.yml

name: bronze-sap-materials-${bundle.target}
target: bronze
channel: CURRENT

clusters:
  - label: default
    policy_id: ${var.cluster_policy_id}
    autoscale:
      min_workers: 1
      max_workers: 4

libraries:
  - notebook:
      path: /Shared/freshness-poc/notebooks/bronze_materials_loader

configuration:
  storage_account: ${var.storage_account}
  bronze_catalog: bronze
```

- [ ] **Step 4: Write bronze_inventory.yml**

```yaml
# resources/pipelines/bronze_inventory.yml

name: bronze-sap-inventory-${bundle.target}
target: bronze
channel: CURRENT

clusters:
  - label: default
    policy_id: ${var.cluster_policy_id}
    autoscale:
      min_workers: 1
      max_workers: 4

libraries:
  - notebook:
      path: /Shared/freshness-poc/notebooks/bronze_inventory_loader

configuration:
  storage_account: ${var.storage_account}
  bronze_catalog: bronze
```

- [ ] **Step 5: Write bronze_open_orders.yml**

```yaml
# resources/pipelines/bronze_open_orders.yml

name: bronze-sap-open-orders-${bundle.target}
target: bronze
channel: CURRENT

clusters:
  - label: default
    policy_id: ${var.cluster_policy_id}
    autoscale:
      min_workers: 1
      max_workers: 4

libraries:
  - notebook:
      path: /Shared/freshness-poc/notebooks/bronze_open_orders_loader

configuration:
  storage_account: ${var.storage_account}
  bronze_catalog: bronze
```

- [ ] **Step 6: Write lakeflow_trigger.yml**

```yaml
# resources/jobs/lakeflow_trigger.yml

name: lakeflow-daily-bronze-${bundle.target}

schedule:
  quartz_cron_expression: "0 0 4 * * ?"
  timezone_id: "Europe/Amsterdam"
  pause_status: ${var.schedule_enabled == "true" ? "UNPAUSED" : "PAUSED"}

email_notifications:
  on_failure:
    - replenishment-oncall@ah.nl

tasks:
  - task_key: lakeflow_sap_cdc
    description: "Trigger Lakeflow Connect CDC pipeline — SAP ECC → Bronze (Terraform-managed)"
    pipeline_task:
      pipeline_id: ${var.lakeflow_pipeline_id}
    timeout_seconds: 3600

  - task_key: bronze_materials
    depends_on:
      - task_key: lakeflow_sap_cdc
    pipeline_task:
      pipeline_id: ${resources.pipelines.bronze_materials.id}
    timeout_seconds: 1800

  - task_key: bronze_inventory
    depends_on:
      - task_key: lakeflow_sap_cdc
    pipeline_task:
      pipeline_id: ${resources.pipelines.bronze_inventory.id}
    timeout_seconds: 1800

  - task_key: bronze_open_orders
    depends_on:
      - task_key: lakeflow_sap_cdc
    pipeline_task:
      pipeline_id: ${resources.pipelines.bronze_open_orders.id}
    timeout_seconds: 1800
```

- [ ] **Step 7: Commit**

```bash
git add databricks.yml resources/
git commit -m "feat(dabs): add Databricks Asset Bundle — bronze pipelines + lakeflow daily job"
```

---

## Task 12: Final Check + Push

- [ ] **Step 1: Run full test suite**

```bash
pytest tests/unit/ -v --tb=short
```

Expected: `18 passed`

- [ ] **Step 2: Run terraform fmt and validate**

```bash
cd infrastructure/terraform
terraform fmt -recursive -check
terraform validate
```

Expected: both exit 0.

- [ ] **Step 3: Verify gitignore is protecting secrets**

```bash
cd c:/Projects/Ahold_POC
git status
```

Confirm `terraform.tfvars` does NOT appear in untracked files (it's gitignored).

- [ ] **Step 4: Push to GitHub**

```bash
git push origin master
```

Expected: `master -> master`

---

## Deployment Sequence (when secrets are available)

Once ExpressRoute authorization key and SAP RFC credentials are in hand:

**Stage 1** — provision workspace:
```bash
cd infrastructure/terraform
cp terraform.tfvars.example terraform.tfvars
# fill in real values
terraform init
terraform apply -target=azurerm_resource_group.main \
                -target=module.networking \
                -target=module.storage \
                -target=module.databricks
terraform output workspace_url   # copy this value
```

**Stage 1.5** — populate Key Vault secrets (requires SAP RFC credentials from Basis team):
```bash
KV_NAME=$(terraform output -raw key_vault_uri | sed 's|https://||;s|\.vault\.azure\.net/||')
az keyvault secret set --vault-name $KV_NAME --name sap-ecc-host     --value "<SAP app server hostname>"
az keyvault secret set --vault-name $KV_NAME --name sap-ecc-sysnr    --value "<system number>"
az keyvault secret set --vault-name $KV_NAME --name sap-ecc-client   --value "<client number>"
az keyvault secret set --vault-name $KV_NAME --name sap-ecc-username --value "<RFC user>"
az keyvault secret set --vault-name $KV_NAME --name sap-ecc-password --value "<RFC password>"
az keyvault secret set --vault-name $KV_NAME --name sap-btp-base-url --value "<BTP AI Core URL>"
az keyvault secret set --vault-name $KV_NAME --name sap-btp-token    --value "<BTP token>"
```

**Stage 2** — add workspace_url to tfvars, then complete:
```bash
# set workspace_url = "<output from Stage 1>" in terraform.tfvars
terraform apply   # completes lakeflow module
```

**Stage 3** — deploy DABs:
```bash
databricks bundle deploy --target dev
databricks bundle run lakeflow_trigger --target dev
```

**Verify** against the 6 criteria in the spec `## Verification` section.
