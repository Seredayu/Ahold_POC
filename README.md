# Ahold Delhaize — Autonomous Replenishment POC

**"The Freshness Sprint"** — an 8-layer AI-driven replenishment engine replacing manual Excel-based ordering for Fresh Produce & Bakery at Albert Heijn NL (pilot: 50 stores).

## Goal

Move from 100% manual replenishment to **90% No-Touch** automated ordering with an **08:15 AM EDI 850 release**, targeting:

- Spoilage: 3.5% → 2.0% (€200M waste reduction)
- Working capital: −4 days DIO (€150M released)
- Labor: −70% manual ordering time

## Architecture

```
Layer 1  Sources          SAP ECC 6.0 (WE) · Symphony Gold (CEE) · SAP EWM/TM · Weather/Events
Layer 2  Ingestion        Lakeflow Connect CDC · Ingestion Gateways (real-time, eliminates 24h lag)
Layer 3  Lakehouse        Medallion (Bronze/Silver/Gold) · Master Data Rosetta Stone
Layer 4  Feature Store    Freshness Decay Velocity · Promo Lift Coefficients
Layer 5  Models           Demand Sensing M1/M2 (P10/P50/P90) · Multi-Echelon M4
Layer 6  Orchestration    MILP Solver "Reality Filter" (TM + EWM constraints)
Layer 7  Action           BAPI/OData write-backs · EDI 850 generation → 08:15 AM
Layer 8  UI               React Field App · Management by Exception · SHAP explainability
```

## Three Priority Engines

| Engine | What it does | Key metric |
|--------|-------------|------------|
| **Phantom Stock Detector** | Identifies invisible stockouts (stock > 0 but sales stopped) | +4% OSA, precision ≥85% |
| **Continuous Freshness Orchestrator** | Blocks orders where Transit-to-Life ratio > 0.5; MILP quantity optimizer | Zero expiry violations |
| **Agentic AI Sweeper** | Monitors 05:00–08:15 AM window; self-corrects exceptions; triggers EDI | ≥90% No-Touch rate |

## Tech Stack

- **Compute:** Databricks (Unity Catalog, DLT, Feature Store, MLflow, Mosaic AI Agent Framework)
- **Storage:** Azure Data Lake Storage Gen2 — Bronze / Silver / Gold Delta Lake
- **Integration:** SAP BTP AI Core (BAPI orchestration), Azure Logic Apps (EDI 850 transport)
- **Frontend:** React 18 + TypeScript + FastAPI (Azure Container Apps)
- **Solver:** PuLP/CBC MILP (abstracted behind `SolverInterface` for OR-Tools upgrade path)
- **Models:** LightGBM + Prophet seasonal decomposition, XGBoost (phantom detection), Databricks Solution Accelerators

## Constraints

- **Clean Core mandate** — zero SAP modifications; all innovation side-by-side in cloud via BAPI/OData
- Sweeper agent is a **deterministic state machine** (not LLM-based) — purchase orders require full auditability
- SHAP explainability in the React UI is load-bearing for store manager trust, not optional polish

## POC Scope

| Item | Value |
|------|-------|
| Target stores | 50 Albert Heijn NL (A/B: 25 test, 25 control) |
| Category | Fresh Produce & Bakery |
| SKUs | ~2,000 |
| Duration | 12–16 weeks |
| EDI deadline | 08:15 AM daily |

## Repository Structure

```
src/
  ingestion/          # Lakeflow Connect configs, external signal ingestors
  medallion/
    bronze/           # Auto Loader pipelines
    silver/           # DLT pipelines + Rosetta Stone (Entity Resolution)
    gold/             # Analytical aggregates
  feature_store/      # Phantom stock signals, SKU/site daily features
  engines/
    phantom_stock/    # Engine 1 — XGBoost classifier
    freshness/        # Engine 2 — Transit-to-Life MILP solver
    sweeper/          # Engine 3 — State machine agent
  integration/
    bapi/             # SAP BAPI wrappers (BAPI_PO_CREATE1, GOODSMVT)
    edi/              # EDI 850 generator
    sap_btp/          # BTP AI Core client
  api/                # FastAPI backend for React app
  orchestration/      # Databricks Workflow definitions
frontend/             # React field app (exception queue, SHAP waterfall, freshness map)
notebooks/            # Exploratory + solution accelerator notebooks
tests/
  unit/
  integration/
  backtest/
infrastructure/
  terraform/          # Azure resource provisioning
  databricks/         # Workspace config, cluster policies
```

## Critical Path

1. **Week 1** — SAP RFC/BAPI access from SAP Basis *(longest lead time — start before sprint)*
2. **Week 4** — Rosetta Stone match rate ≥95% *(unblocks all models)*
3. **Week 8** — Request `BAPI_PO_CREATE1` SAP Security authorization *(3-week approval lead time)*
4. **Week 6** — Initiate EDI supplier partner agreement *(4–6 week procurement lead time)*

## Go/No-Go Gates

| Week | Gate | Pass Criteria |
|------|------|---------------|
| 5 | Data Foundation | Rosetta Stone ≥95%, DLT pipelines healthy |
| 8 | Engine 1 | Backtest precision ≥80% |
| 11 | Engine 2 + BAPI | BAPI write-back in SAP QA, 1 EDI supplier test confirmed |
| 14 | Integration | Full pipeline completes by 08:15 AM in staging |
| 16 | Final | No-Touch ≥85%, EDI met ≥90% of days, waste trending down |
