# Data Integration

> Last updated: 2026-05-08 | Sources: [ahold_delhaize_inventory_optimization_architecture.md, Ahold Delhaize - 1.md]

## Summary

Layer 2 of the architecture handles data movement from three source ERP systems into the Databricks Medallion Lakehouse. Each stream uses different ingestion technology due to the structural and regional constraints of each source system.

## Three Ingestion Streams

### Stream 1: SAP ECC 6.0 (Western Europe)
- **Tool**: SAP Data Services (BODS) as "Trusted Proxy"
- **Why**: SAP ECC uses legacy cluster and pooled tables that modern CDC tools (e.g., Lakeflow Connect) cannot read directly.
- **Methods**: RFC Connectors, Operational Data Provisioning (ODP), IDOC Extraction, Delta Extraction, BAPI Calls
- **Key tables extracted**: MARA (Materials), MARC (Plant Data), MBEW (Valuation), MSEG (Movements), LIKP/LIPS (Deliveries), EKKO/EKPO (Purchase Orders)
- **Schedule**: Daily at 02:00 AM CET (previous day sales from VBRK/VBRP, stock movements from MSEG)

### Stream 2: Symphony Gold (Central & Eastern Europe)
- **Model**: Hybrid ingestion
- **Real-time path**: REST APIs + SAP Event Mesh for POS transactions
- **Batch path**: SFTP/File Exports (CSV/JSON) for large master data synchronization
- **Data**: Real-time POS, store-level inventory, perishables expiry tracking, supplier lead times

### Stream 3: SAP S/4HANA (Group Finance)
- **Tool**: Lakeflow Connect (CDC)
- **Latency**: Sub-minute streaming via Change Data Capture
- **Purpose**: Keeps financial reporting (ACDOCA Universal Journal, CO-PA, planning) synchronized with operations
- **Integration**: OData APIs, SAP Gateway, CDS Views for write-backs

## Medallion Architecture (Layer 3)

### Bronze Layer (Append-Only, Raw)
All raw data lands here unchanged. Key tables:
- `raw_sap_mara`, `raw_sap_marc`, `raw_sap_mseg`
- `raw_symphony_pos`, `raw_symphony_inv`
- `raw_s4_acdoca`, `raw_s4_acdocp`
- `external_weather`, `external_holidays`, `external_events`

### Silver Layer (Cleansed + Unified)
DLT pipelines clean, deduplicate, and enrich with master data. Critical component: **Rosetta Stone**.
- `master_materials`, `master_stores`, `master_suppliers`
- `sales_transactions`, `inventory_movements`, `stock_levels`
- `purchase_orders`, `delivery_performance`, `quality_checks`
- `perishable_tracking`, `promotion_calendar`

### Gold Layer (Business-Ready)
Analytical aggregates ready for models and dashboards:
- `demand_forecast_daily`, `inventory_optimization`, `replenishment_plan`
- `waste_prediction`, `freshness_index`, `promotion_impact`
- `supplier_performance`, `anomaly_alerts`, `ml_feature_store`, `kpi_dashboards`
- `gold.replenishment.order_recommendations` (output of Freshness Engine)
- `gold.edi.outbound/` (EDI 850 files picked up by Azure Logic App)

## Rosetta Stone

The Rosetta Stone (`src/medallion/silver/rosetta_stone/`) is the critical master data alignment layer. It maps:
- SAP ECC material numbers ↔ Symphony Gold item codes ↔ EAN barcodes

Output: `silver.master.unified_sku_registry`

**Blocking constraint**: Match rate must be ≥ 95% before model training can begin. Must be validated against SAP MM60 exports before committing to this target.

## Pipeline Technology

- **DLT (Delta Live Tables)**: Declarative pipeline development, automatic data quality checks, self-healing with automatic retries, incremental processing.
- **Unity Catalog**: Cross-workspace governance, GDPR compliance, row/column-level security, audit logging, data masking for PII, data retention policies.
- **Orchestration**: Databricks Workflows (primary), n8n (optional for alerting).

## External Data Sources

Ingested alongside ERP data:
- Weather APIs (OpenWeather, Weather.com) — temperature, precipitation forecasts
- Event calendars — sporting events, concerts, public holidays
- Economic indicators — consumer confidence, inflation
- Competitor pricing — web scraping, third-party data
- Social media sentiment — Twitter, Instagram
- Foot traffic data — Google Places, SafeGraph

## Related

[[Index]] [[Architecture-Overview]] [[SAP-Integration]] [[Infrastructure]]
