# Phase 0 Design — Azure Infra + SAP Connectivity

**Date:** 2026-05-05
**Sprint phase:** 0 (Weeks 1–2)
**Status:** Approved

## Context

Phase 0 establishes the foundational infrastructure for the Ahold Delhaize Freshness Sprint POC. It must be complete before any data pipeline, model, or engine work can begin. Two workstreams run in parallel:

1. **Azure infrastructure** — fully deployable via Terraform, greenfield subscription
2. **SAP ECC connectivity** — Lakeflow Connect CDC landing raw data into Bronze Delta tables

SAP ECC is on-prem, reachable via an already-provisioned ExpressRoute circuit (carrier-side done; Azure VNet gateway still needed). Unity Catalog metastore does not yet exist and must be created.

## Architecture

```
SAP ECC (on-prem)
  → ExpressRoute circuit (provisioned)
    → VNet Gateway (to be created)
      → Lakeflow Connect CDC (DLT managed pipeline)
        → ADLS Gen2 bronze container (Delta, append-only)
          → Auto Loader (schema inference + evolution)
            → bronze.sap.* Unity Catalog tables
```

## Terraform Structure

Modular layout — four focused modules wired by a root module:

```
infrastructure/terraform/
  main.tf                    # root: instantiates all modules, passes outputs between them
  variables.tf               # global inputs (subscription_id, location, env tag)
  outputs.tf                 # workspace URL, storage endpoint, metastore ID
  terraform.tfvars.example   # safe-to-commit example values (no secrets)
  modules/
    networking/              # VNet, subnets, NSGs, ExpressRoute gateway + connection
    storage/                 # ADLS Gen2, containers, private endpoints, RBAC
    databricks/              # workspace, Unity Catalog, Key Vault, cluster policy, catalogs
    lakeflow/                # Lakeflow Connect DLT pipeline, SAP connection, CDC feeds
```

## Module Designs

### networking

**Purpose:** bridge the existing ExpressRoute circuit to a Databricks-ready VNet.

**Resources:**
- `azurerm_virtual_network` — `10.0.0.0/16`, westeurope
- `azurerm_subnet` × 3:
  - `databricks-public` — `10.0.1.0/24` (VNet injection, public plane)
  - `databricks-private` — `10.0.2.0/24` (VNet injection, private plane)
  - `GatewaySubnet` — `10.0.3.0/27` (required name for ExpressRoute gateway)
- `azurerm_virtual_network_gateway` — type `ExpressRoute`, Standard SKU
- `azurerm_virtual_network_gateway_connection` — links gateway to circuit
- NSGs on both Databricks subnets with Microsoft-required delegation rules

**Key inputs:** `expressroute_circuit_id`, `expressroute_authorization_key`, `vnet_address_space` (default `10.0.0.0/16`)

**Outputs:** `vnet_id`, `databricks_public_subnet_id`, `databricks_private_subnet_id`

**Out of scope (POC):** Azure Firewall, DDoS protection, hub-spoke peering

### storage

**Purpose:** ADLS Gen2 data lake with private access and Databricks RBAC.

**Resources:**
- `azurerm_storage_account` — ADLS Gen2 (`is_hns_enabled = true`), LRS, westeurope
- `azurerm_storage_container` × 4: `bronze`, `silver`, `gold`, `gold-edi-outbound`
- `azurerm_private_endpoint` — binds storage to `databricks-private` subnet
- `azurerm_role_assignment` — Databricks managed identity → `Storage Blob Data Contributor`

**Key inputs:** `storage_account_name`, `private_subnet_id`, `databricks_managed_identity_id`

**Outputs:** `storage_account_id`, `dfs_endpoint`, `container_ids`

### databricks

**Purpose:** Databricks Premium workspace, Unity Catalog metastore, secrets, cluster policy, and catalogs.

**Resources:**
- `azurerm_databricks_workspace` — Premium SKU, VNet-injected (uses networking outputs)
- `azurerm_key_vault` — stores SAP credentials, storage keys, BTP tokens; Databricks managed identity granted `get`/`list`
- `databricks_metastore` — Unity Catalog, westeurope, ADLS Gen2 root at `abfss://gold@${storage_module.storage_account_name}.dfs.core.windows.net/metastore`
- `databricks_metastore_assignment` — attaches metastore to workspace
- `databricks_catalog` × 4: `bronze`, `silver`, `gold`, `feature_store`
- `databricks_cluster_policy` — enforces `Standard_DS3_v2`, autoscale 2–8, Spark 14.3 LTS
- `databricks_secret_scope` — Key Vault-backed, for SAP/BTP credentials

**Key inputs:** `public_subnet_id`, `private_subnet_id`, `storage_account_id`, `key_vault_sku`

**Outputs:** `workspace_url`, `metastore_id`, `cluster_policy_id`, `key_vault_id`

### lakeflow

**Purpose:** Lakeflow Connect CDC pipeline pulling SAP ECC tables into Bronze.

**Resources:**
- `databricks_pipeline` (DLT) — Lakeflow Connect managed, triggered mode (not continuous), scheduled 04:00 AM via DABs job

**SAP ECC CDC feeds:**

| Feed | SAP tables | Bronze target | Key columns |
|------|-----------|---------------|-------------|
| Materials | MARA, MARC | `bronze.sap.materials` | MATNR, WERKS |
| Inventory movements | MSEG, MKPF | `bronze.sap.inventory` | MBLNR, ZEILE |
| Purchase orders | EKKO, EKPO | `bronze.sap.open_orders` | EBELN, EBELP |

SAP connection params (host, port, client, system number) read from Key Vault secret scope — never hardcoded.

**Key inputs:** `workspace_url`, `cluster_policy_id`, `secret_scope_name`, `bronze_catalog`

**Outputs:** `pipeline_id`, `landing_zone_path`

## Bronze Auto Loader Pipelines

```
src/medallion/bronze/
  autoloader.py              # base class: cloudFiles, schema inference, audit columns
  sap_ecc_materials.py       # subclass → bronze.sap.materials
  sap_ecc_inventory.py       # subclass → bronze.sap.inventory
  sap_ecc_open_orders.py     # subclass → bronze.sap.open_orders
```

**Base class behaviour:**
- Format: `cloudFiles` (Auto Loader), source format: `parquet`
- Schema inference with `cloudFiles.inferColumnTypes = true`
- Schema evolution: `mergeSchema = true`
- Audit columns appended to every record: `_ingested_at` (timestamp), `_source_file` (string), `_cdc_operation` (string: INSERT/UPDATE/DELETE)
- Write mode: append-only Delta

## Databricks Bundles (DABs)

```
databricks.yml               # bundle root
resources/
  pipelines/
    bronze_materials.yml
    bronze_inventory.yml
    bronze_open_orders.yml
  jobs/
    lakeflow_trigger.yml     # 04:00 AM daily, triggers Lakeflow + all bronze pipelines
```

**Targets:** `dev`, `staging`, `prod` — parameterised by storage path, cluster size, schedule toggle.

## Secrets & Credentials

All secrets stored in Azure Key Vault, never in code or tfvars:

| Secret name | Contents |
|------------|----------|
| `sap-ecc-host` | SAP application server hostname |
| `sap-ecc-client` | SAP client number |
| `sap-ecc-sysnr` | SAP system number |
| `sap-ecc-username` | RFC user (read-only) |
| `sap-ecc-password` | RFC user password |
| `sap-btp-base-url` | SAP BTP AI Core endpoint |
| `sap-btp-token` | BTP service token |

`terraform.tfvars.example` documents all variable names with placeholder values. Actual `terraform.tfvars` is `.gitignore`'d.

## Critical External Dependencies

These block SAP connectivity but NOT the Terraform infra work:

| Dependency | Owner | Lead time | Action |
|-----------|-------|-----------|--------|
| SAP RFC/BAPI access (`BAPI_PO_CREATE1` auth objects) | SAP Basis | 3 weeks | Raise ticket Week 1 |
| RFC read-only user for Lakeflow Connect | SAP Basis | 3 weeks | Include in same ticket |
| ExpressRoute authorization key | Network team | 1–2 days | Request immediately |

## Verification

End-to-end Phase 0 is verified when:
1. `terraform apply` completes with zero errors from a clean state
2. Databricks workspace loads at the output URL, Unity Catalog shows all 4 catalogs
3. Key Vault secrets are readable from a Databricks notebook via the secret scope
4. Lakeflow Connect pipeline shows SAP ECC tables in the connection preview (requires RFC access)
5. A manual pipeline run lands at least one record in `bronze.sap.materials` and Auto Loader reads it into the Delta table
6. `bronze.sap.materials`, `bronze.sap.inventory`, `bronze.sap.open_orders` are queryable in Unity Catalog with `_ingested_at` audit column populated
