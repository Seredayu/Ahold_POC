# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is the **Ahold Delhaize Freshness Sprint POC** — an 8-layer AI-driven autonomous replenishment engine for Fresh Produce & Bakery at Albert Heijn NL (50 pilot stores). The goal is 90% No-Touch ordering with a hard **08:15 AM EDI 850 release** deadline.

Full architectural detail is in `investigation.md`. The `README.md` has the scope, go/no-go gates, and critical path.

## Architecture

The system is structured as a Medallion Lakehouse on Databricks (Azure) with three autonomous engines:

**Data flow:** SAP ECC CDC → Bronze (Delta Lake, append-only) → Silver (DLT pipelines + Rosetta Stone) → Gold (analytical aggregates) → Feature Store → Engines → BAPI/EDI write-backs

**Three engines run sequentially each morning starting 05:00 AM:**
1. `src/engines/phantom_stock/` — XGBoost classifier detecting invisible stockouts; outputs drive stock corrections via `BAPI_GOODSMVT_CREATE`
2. `src/engines/freshness/` — MILP solver (`replenishment_quantity_optimizer.py`) with Transit-to-Life constraint; outputs are `gold.replenishment.order_recommendations`
3. `src/engines/sweeper/` — Deterministic state machine (NOT LLM-based) that monitors pipeline completeness and triggers `BAPI_PO_CREATE1` + EDI 850 generation by 08:15 AM

**The Rosetta Stone** (`src/medallion/silver/rosetta_stone/`) maps SAP ECC material numbers ↔ Symphony Gold item codes ↔ EAN barcodes into `silver.master.unified_sku_registry`. This is the dependency for all downstream models — a match rate below 95% blocks model training.

## Key Constraints

- **Clean Core mandate**: zero SAP modifications. All SAP interaction via standard BAPIs (`BAPI_PO_CREATE1`, `BAPI_GOODSMVT_CREATE`) routed through SAP BTP AI Core. Never suggest modifying SAP tables directly.
- **Sweeper must be deterministic**: decision rules live in `src/engines/sweeper/decision_rules.py` as Pydantic models with explicit thresholds. Do not introduce LLM-based decisions into the Sweeper — purchase orders require full auditability.
- **MILP solver is abstracted** behind a `SolverInterface` base class in `src/engines/freshness/`. The POC uses PuLP/CBC; do not hardcode solver-specific APIs — keep the interface so OR-Tools/Gurobi can be swapped without touching Engine 2 logic.
- **SHAP explainability is load-bearing**: the React app's `ShapWaterfall.tsx` component is the primary store-manager trust mechanism. It must render for every exception in the queue.

## Stack Decisions

- **Databricks**: DLT for Silver pipelines, Feature Store for reusable signals, MLflow for model registry, Mosaic AI Agent Framework for Sweeper tooling, Unity Catalog throughout
- **Models**: LightGBM primary demand model with Prophet seasonal decomposition as input features; XGBoost for phantom detection; Databricks Demand Forecasting Solution Accelerator as the hierarchical forecasting base
- **MILP**: PuLP with CBC solver (open-source, runs natively in Databricks Python environment)
- **EDI transport**: Azure Logic Apps triggered by Blob Storage event — EDI 850 files land in `gold.edi.outbound/`, Logic App picks them up. Never route EDI directly from Databricks to external SFTP.
- **Frontend**: React 18 + TypeScript + Vite, FastAPI backend on Azure Container Apps, Azure Static Web Apps hosting with Azure AD SSO

## Critical Dependencies (start before sprint Week 1)

- SAP Basis must grant RFC access and authorize `BAPI_PO_CREATE1` authorization objects — 3-week approval lead time
- EDI supplier partner agreement — 4–6 week procurement lead time; initiate in Week 1
- Rosetta Stone match rate must be validated against SAP MM60 exports before committing to ≥95% target
## Output style 
No preambule.
Tool result first. 
No explanation of actions. 
Stop.

