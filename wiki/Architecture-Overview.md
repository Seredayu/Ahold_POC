# Architecture Overview

> Last updated: 2026-05-08 | Sources: [ahold_delhaize_inventory_optimization_architecture.md, Ahold Delhaize - 1.md]

## Summary

The Ahold Delhaize **8-Layer AI Inventory Optimization Architecture** bridges fragmented legacy ERP systems (SAP ECC 6.0, Symphony Gold, SAP S/4HANA) with a centralized AI intelligence layer on Databricks (Azure). It ensures AI-driven predictions respect physical logistics constraints and financial targets in real-time, targeting **€350M+ annual EBITDA improvement**.

The POC scope is Fresh Produce & Bakery at Albert Heijn NL, 50 pilot stores, with a **90% No-Touch ordering** goal and a hard **08:15 AM EDI 850 release** deadline.

## The 8 Layers

### Layer 1: Source Systems
- **SAP ECC 6.0** (Western Europe / AH NL): MM, SD, WM, PP, QM modules. Key tables: MARA, MARC, MBEW, MSEG, LIKP/LIPS, EKKO/EKPO.
- **Symphony Gold** (Central & Eastern Europe): POS Transactions, Store Inventory, Perishables expiry, Supplier lead times, Promotion Mgmt, Fresh Food Mgmt.
- **SAP S/4HANA** (Group Finance): Universal Journal (ACDOCA), CO-PA, Cash Management, Consolidation.

### Layer 2: Data Integration & Ingestion
Three distinct streams based on regional/technical requirements:
- **Stream 1 — SAP ECC 6.0:** SAP Data Services (BODS) as "Trusted Proxy" using RFC and Operational Data Provisioning (ODP) to handle legacy cluster/pooled tables that modern CDC tools cannot read.
- **Stream 2 — Symphony Gold:** Hybrid ingestion — real-time POS via REST APIs + SAP Event Mesh; large master data via SFTP/File Exports.
- **Stream 3 — SAP S/4HANA:** Lakeflow Connect CDC for sub-minute latency streaming to keep financial reporting synchronized with operations.

### Layer 3: Databricks Lakehouse Platform (Azure)
Medallion Architecture via Delta Live Tables (DLT):
- **Bronze** (append-only): raw_sap_mara, raw_sap_marc, raw_sap_mseg, raw_symphony_pos, raw_symphony_inv, raw_s4_acdoca, external_weather/holidays/events
- **Silver** (cleansed + Rosetta Stone): master_materials, master_stores, master_suppliers, sales_transactions, inventory_movements, stock_levels, purchase_orders, delivery_performance, quality_checks, perishable_tracking, promotion_calendar
- **Gold** (business ready): demand_forecast_daily, inventory_optimization, replenishment_plan, waste_prediction, freshness_index, promotion_impact, supplier_performance, anomaly_alerts, ml_feature_store, kpi_dashboards

**Unity Catalog** provides cross-region data lineage, GDPR compliance, and RBAC throughout.

### Layer 4: AI/ML Orchestration & Feature Engineering
**Databricks Feature Store** centralizes signals for consistency between training and inference:
- Time-Series: 7/14/30-day moving averages, day-of-week/month patterns, seasonality decomposition, trend indicators
- Freshness: days to expiry, shelf-life remaining %, temperature compliance, quality score (from QM)
- Promotion: active promotion flag, discount depth, promotion lift (historical), cross-promotion effects
- External: weather (temp, precip), local events/holidays, competitor pricing, foot traffic predictions
- Supply Chain: lead time variance, supplier reliability score, transit time predictions, warehouse capacity utilization

### Layer 5: ML Models & Prediction Engine (MLflow Model Registry)
- **Model 1 — Demand Forecasting**: Prophet (baseline) → LightGBM (production), with LSTM as deep option. Granularity: Store-SKU-Day. Outputs: P10/P50/P90 demand, forecast accuracy, confidence intervals.
- **Model 2 — Waste Prediction / Phantom Detection**: XGBoost Classifier + Random Forest. Targets waste probability, expected waste $, optimal markdown %. Triggers markdown alerts, donation triggers, reorder prevention.
- **Model 3 — Dynamic Pricing**: Reinforcement Learning (PPO/DQN) + Price Elasticity Model. Optimizes margin, inventory turnover, freshness preservation.
- **Model 4 — Replenishment Optimization**: Multi-Echelon Inventory Optimization (MEIO) with MILP (PuLP/CBC). Constraints: truck capacity, warehouse space, shelf-life limits, MOQs.
- **Model 5 — AI-Driven Supplier Selection**: Multi-criteria scoring — quality, lead time reliability, price competitiveness, sustainability.

### Layer 6: Business Logic & Decision Engine
Translates "mathematical ideals" into "operational realities":
- **Business Rules Engine (BRE)**: min/max stock levels by store, freshness thresholds, auto-order rules, markdown policies, store clustering, regional preferences.
- **Constraint Solver**: Linear Programming / multi-objective optimization reconciling AI orders with physical SAP EWM/TM limits (truck volume, warehouse labor, capacity).
- **Alert Engine** (n8n/Airflow): stockout risk, waste alerts, price change triggers, quality violations via Email/SMS/Teams/Slack/mobile/dashboard.

### Layer 7: Action & Write-Back Systems
- **SAP ECC**: Auto-create POs via `BAPI_PO_CREATE1`, adjust safety stock, update MRP parameters, block obsolete SKUs. Integration: RFC/BAPI, IDOC, Web Services.
- **Symphony Gold**: Update shelf pricing (Electronic Shelf Labels via REST API), trigger markdown campaigns, reorder store layouts, send donations list.
- **SAP S/4HANA**: Forecast financials, update product P&L, waste provisioning, working capital optimization. Integration: OData APIs, SAP Gateway, CDS Views.

EDI 850 files land in `gold.edi.outbound/`; Azure Logic App picks them up for SFTP delivery to suppliers. EDI is **never** routed directly from Databricks.

### Layer 8: User Interfaces & Analytics
- **Power BI** (operational): Store managers, category managers, supply chain, buyers. Real-time daily inventory positions, stockout alerts, waste tracking, replenishment queues.
- **SAP Analytics Cloud** (strategic): C-suite, Category VPs, Finance, Strategy. Waste trends, forecast accuracy, margin analysis, supplier scorecard, regional performance, ROI metrics.
- **React Field App** (store managers): Mobile-first, offline-capable. Management by Exception — approve markdowns, barcode scanning, real-time stock check, photo upload (quality issues), task management.

## Daily Processing Timeline

| Time (CET) | Activity |
|---|---|
| 02:00 AM | Data ingestion: SAP ECC sales/movements, Symphony POS/inventory, external weather |
| 02:15–03:00 AM | DLT processing: Bronze → Silver → Gold |
| 03:00–03:30 AM | ML inference: demand forecast, waste prediction, replenishment optimization |
| 03:30–04:00 AM | Business rules application: min/max, promotions, truck capacity, MOQs |
| 04:00–05:00 AM | Engine 1 (Phantom Stock) + Engine 2 (Freshness MILP) run |
| 05:00 AM | Engine 3 (Sweeper) begins monitoring |
| **08:15 AM** | **Hard deadline: EDI 850 release** |

## Key Architectural Decisions

1. **Unified Data Lakehouse** — single source of truth across Western + Central/Eastern Europe; eliminates silos between SAP ECC, Symphony, S/4HANA.
2. **Delta Live Tables** — declarative, self-healing pipelines with automatic data quality checks and incremental processing.
3. **MLflow Model Registry** — version control, A/B testing, Staging→Production workflow, full model lineage.
4. **Hybrid Write-Back** — 90% automation target (POC scope) via direct BAPI/EDI calls; remainder handled by exception queue in React app.
5. **Real-Time + Batch Hybrid** — batch daily forecasts/weekly supplier optimization; real-time stockout alerts, quality detection, price changes.
6. **Regional Deployment** — West Europe workspace (SAP ECC: NL, BE, LU); Central Europe workspace (Symphony: CZ, RO, etc.); Unity Catalog for cross-workspace sharing.

## Success KPIs

| Metric | Baseline | Target |
|---|---|---|
| Perishable waste | 3.5% | 2.0% (€200M+ savings) |
| On-shelf availability | 94% | 98% |
| Forecast MAPE (fresh produce) | — | < 15% |
| Forecast MAPE (packaged) | — | < 10% |
| Inventory days | 28 days | 24 days (€150M cash release) |
| Manual ordering time reduction | — | 70% |
| **Total EBITDA impact** | — | **€350M+** |

## Related

[[Index]] [[Data-Integration]] [[ML-Models]] [[Replenishment-Engine]] [[SAP-Integration]] [[Frontend]] [[Infrastructure]]
