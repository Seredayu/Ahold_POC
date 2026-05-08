# Freshness Sprint POC — Wiki Index

> Last updated: 2026-05-08 | Sources: [ahold_delhaize_inventory_optimization_architecture.md, Ahold Delhaize - 1.md]

## Pages

| Page | Description |
|---|---|
| [[Architecture-Overview]] | Full 8-layer solution architecture, daily timeline, KPIs, key decisions |
| [[Data-Integration]] | Layer 2–3: Ingestion streams, Medallion (Bronze/Silver/Gold), Rosetta Stone, external data |
| [[ML-Models]] | Layer 4–5: Feature Store, five ML engines, MLflow MLOps, SHAP explainability |
| [[Replenishment-Engine]] | Three autonomous engines (Phantom Stock, Freshness MILP, Sweeper), EDI 850 deadline |
| [[SAP-Integration]] | Clean Core mandate, BAPIs, ECC/S4HANA/Symphony write-backs, EDI, pre-sprint dependencies |
| [[Frontend]] | React Field App, Power BI, SAP Analytics Cloud, SHAP Waterfall component |
| [[Infrastructure]] | Azure + Databricks platform, security, monitoring, DevOps/MLOps, implementation roadmap |
| [[Watcher]] | File system daemon that auto-ingests research/ changes into wiki pages |

## Key Facts

- **POC scope**: Fresh Produce & Bakery, Albert Heijn NL, 50 pilot stores
- **No-Touch target**: 90% automated replenishment
- **Hard deadline**: 08:15 AM CET EDI 850 release daily
- **Expected ROI**: €350M+ annual EBITDA improvement
- **Platform**: Databricks Lakehouse on Azure
- **Source systems**: SAP ECC 6.0 (Western Europe), Symphony Gold (Central/Eastern Europe), SAP S/4HANA (Group Finance)

## Critical Pre-Sprint Dependencies

1. SAP Basis RFC access + `BAPI_PO_CREATE1` authorization — **3-week lead time**
2. EDI supplier partner agreement — **4–6 week lead time**
3. Rosetta Stone match rate ≥ 95% validated against SAP MM60 exports
