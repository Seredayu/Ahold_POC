# Replenishment Engine

> Last updated: 2026-05-08 | Sources: [ahold_delhaize_inventory_optimization_architecture.md, Ahold Delhaize - 1.md]

## Summary

Three autonomous engines run sequentially each morning starting 05:00 AM, with a hard **08:15 AM EDI 850 release** deadline. The goal is **90% No-Touch ordering** for Fresh Produce & Bakery at Albert Heijn NL (50 pilot stores).

## Engine 1: Phantom Stock Detector

- **Location**: `src/engines/phantom_stock/`
- **Algorithm**: XGBoost classifier detecting invisible stockouts (items that appear in-stock in SAP but are actually unavailable on-shelf)
- **Write-back**: Stock corrections via `BAPI_GOODSMVT_CREATE`
- **Purpose**: Ensures replenishment decisions are made on accurate inventory levels

## Engine 2: Freshness Optimizer (MILP)

- **Location**: `src/engines/freshness/`
- **Core file**: `replenishment_quantity_optimizer.py`
- **Algorithm**: Mixed Integer Linear Programming (MILP) via PuLP with CBC solver (open-source, runs natively in Databricks Python environment)
- **Key constraint**: **Transit-to-Life** — ordered quantity's shelf life must exceed transit time; stale deliveries are blocked
- **Hard constraints**: truck capacity, warehouse space, shelf-life limits (days to expiry), minimum order quantities (MOQs)
- **Objectives**: Minimize waste, maximize sales, reduce logistics costs, ensure freshness
- **Output**: `gold.replenishment.order_recommendations`
- **Solver abstraction**: `SolverInterface` base class — never hardcode PuLP-specific APIs; OR-Tools/Gurobi must be swappable without touching engine logic

## Engine 3: Sweeper

- **Location**: `src/engines/sweeper/`
- **Type**: Deterministic state machine (NOT LLM-based)
- **Decision rules**: `src/engines/sweeper/decision_rules.py` — Pydantic models with explicit thresholds. Full auditability required for purchase orders.
- **Responsibilities**:
  - Monitor pipeline completeness across all upstream engines
  - Trigger `BAPI_PO_CREATE1` for auto-approved orders
  - Generate EDI 850 files to `gold.edi.outbound/`
  - Ensure 08:15 AM hard deadline is met
- **Tooling**: Databricks Mosaic AI Agent Framework for Sweeper tooling

## EDI 850 Delivery

- EDI 850 files are written to `gold.edi.outbound/` (Azure Blob Storage)
- Azure Logic App picks them up via Blob Storage event trigger
- Logic App delivers to supplier SFTP
- **Never** route EDI directly from Databricks to external SFTP

## Business Rules Layer

The Business Rules Engine (Layer 6) translates MILP outputs into executable orders:
- Apply min/max stock levels per store
- Adjust for ongoing promotions (avoid over-ordering during promo periods)
- Respect truck capacity constraints
- Enforce supplier minimum order quantities
- Apply regional preferences and store clustering (A/B/C)

## No-Touch vs. Exception Queue

| Status | Action |
|---|---|
| 90% target: auto-approved | Sweeper triggers BAPI + EDI directly |
| ~10%: exceptions | Routed to React app queue for store-manager review |

Exceptions surface via SHAP waterfall (`ShapWaterfall.tsx`) so managers understand the AI's reasoning before approving or overriding.

## Related

[[Index]] [[ML-Models]] [[SAP-Integration]] [[Frontend]] [[Architecture-Overview]]
