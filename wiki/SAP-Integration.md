# SAP Integration

> Last updated: 2026-05-08 | Sources: [ahold_delhaize_inventory_optimization_architecture.md, Ahold Delhaize - 1.md]

## Summary

All SAP interaction follows the **Clean Core mandate**: zero modifications to SAP. Every action is executed via standard BAPIs routed through SAP BTP AI Core. Direct modification of SAP tables is never permitted.

## Clean Core Mandate

- Zero SAP customizations or modifications
- All write-backs via standard BAPIs only
- SAP Basis must grant RFC access and authorize BAPI authorization objects — **3-week approval lead time** (must start before Week 1 of sprint)

## BAPIs Used

| BAPI | Purpose | Triggered by |
|---|---|---|
| `BAPI_PO_CREATE1` | Auto-create Purchase Orders | Sweeper Engine (Engine 3) |
| `BAPI_GOODSMVT_CREATE` | Stock corrections (phantom stock) | Phantom Stock Engine (Engine 1) |

## SAP ECC 6.0 (Source + Target)

**Source data (ingestion)**:
- Modules: MM (Materials Mgmt), SD (Sales & Distribution), WM (Warehouse Mgmt), PP (Production), QM (Quality Mgmt)
- Key tables: MARA, MARC, MBEW, MSEG, LIKP/LIPS, EKKO/EKPO, VBRK/VBRP
- Extraction via SAP BODS (BODS as "Trusted Proxy"), RFC, ODP, IDOC

**Write-backs**:
- Auto-create POs via `BAPI_PO_CREATE1`
- Adjust safety stock levels
- Update MRP parameters
- Block obsolete SKUs

## SAP S/4HANA (Group Finance)

**Source data**: ACDOCA (Universal Journal), ACDOCP (Planning), BSEG (Accounting), CO-PA profitability, Cash Management

**Write-backs**:
- Forecast financials
- Update product P&L
- Waste provisioning entries
- Working capital optimization

Integration: OData APIs, SAP Gateway, CDS Views

## SAP BTP AI Core

All BAPI calls are routed through SAP BTP AI Core — this is the sanctioned integration path that maintains Clean Core compliance while enabling AI-driven automation.

## Symphony Gold Integration

Symphony Gold (Central & Eastern Europe) is not SAP but integrates at the same layer:
- Write-backs: Update shelf pricing via REST API (Electronic Shelf Labels), trigger markdown campaigns, reorder store layouts, send donations lists
- Read: POS transactions via REST APIs + SAP Event Mesh; master data via SFTP/File Exports

## EDI Integration

- **Standard**: EDI 850 (Purchase Order)
- **Supplier partner agreement**: 4–6 week procurement lead time — must initiate in Week 1
- **File path**: `gold.edi.outbound/` (Azure Blob Storage)
- **Transport**: Azure Logic App triggered by Blob Storage event → SFTP to supplier
- **Never** route EDI directly from Databricks to external SFTP

## Critical Pre-Sprint Dependencies

1. SAP Basis RFC access + `BAPI_PO_CREATE1` authorization objects — **3-week lead time**
2. EDI supplier partner agreement — **4–6 week lead time**
3. Rosetta Stone match rate validation against SAP MM60 exports (must be ≥ 95%)

## Related

[[Index]] [[Architecture-Overview]] [[Replenishment-Engine]] [[Data-Integration]]
