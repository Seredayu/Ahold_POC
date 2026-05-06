# Session Handoff — 2026-05-06

## Where We Are

### Completed and merged to master
| Phase | Status | Key output |
|---|---|---|
| Phase 0 | ✅ merged | Azure infra, Lakeflow CDC, bronze pipelines, `databricks.yml` |
| Phase 1 | ✅ merged | Medallion Bronze/Silver/Gold + Rosetta Stone SKU mapping |
| Phase 2A | ✅ merged (feature branch) | Feature Store: `velocity_features`, `stock_features`, `collapse_signals`, `FeatureStoreBase`, DABs `feature_store_refresh` job |
| Phase 2B | ✅ merged to master (PR #1) | Phantom Stock Detector: `label_generator`, `classifier` (XGBoost + SHAP), `bapi_client` (BAPI_GOODSMVT_CREATE), `feature_pipeline`, 7 unit tests, DABs jobs |

### In progress right now — Phase 3 brainstorming
The brainstorming skill was invoked for Phase 3. Design decisions were agreed and **Phase 3A spec was written but NOT yet committed**. Phase 3B spec was NOT yet written.

---

## Phase 3 Design Decisions (all approved by user)

**Decomposition:** Two sub-projects — 3A (Demand Models) then 3B (Freshness Orchestrator + BAPI)

**Demand model stack (full):**
- M1 — LightGBM + Prophet seasonal decomposition, 104 weeks training history, 3 horizon targets (7d/14d/28d)
- M2 — Multiplicative lift correction: promo (from SAP SD `silver.promo.calendar`) + weather (Open-Meteo API free tier, Amsterdam coords)
- M4 — LightGBM quantile regression α=0.1/0.5/0.9 → P10/P50/P90 + `uncertainty_spread`

**Transit-to-Life (TTL) handling — category-configurable:**
- Fresh Produce: hard block (qty = 0, `ttl_status = TTL_BLOCKED`)
- Bakery: soft cap (qty = min(qty, max_sellable), `ttl_status = TTL_CAPPED`)

**Auto-approval gates for BAPI_PO_CREATE1 (BOTH must pass):**
- Confidence gate: `uncertainty_spread / p50 < 0.30`
- Value gate: `recommended_qty × unit_cost < €500`
- Result: `AUTO_APPROVE` | `PENDING_REVIEW` | `BLOCKED`

**Unified morning pipeline** (replaces all separate per-phase jobs — enforced dependencies, no wall-clock gaps):
```
morning_pipeline  — daily 05:00 AM Amsterdam
│
├── feature_store_refresh
│
├── phantom_stock_score        (depends_on: feature_store_refresh)
│   ├── score_batch
│   ├── write_alerts
│   └── bapi_writeback
│
├── demand_forecast_score      (depends_on: feature_store_refresh — parallel with phantom)
│   ├── ingest_weather
│   ├── score_m1
│   ├── score_m2
│   └── score_m4
│
└── freshness_score            (depends_on: demand_forecast_score)
    ├── milp_solve
    ├── write_recommendations
    └── bapi_po_create
```

Weekly training jobs (separate, no deadline pressure):
- `demand_forecast_train` — Sunday 01:00 AM (`train_m1 → train_m2 → train_m4`)
- `phantom_stock_train` — Sunday 03:00 AM (existing)

---

## Files Written This Session (NOT YET COMMITTED)

### Written, needs commit + push:
- `docs/superpowers/specs/2026-05-06-phase3a-demand-models-design.md` — COMPLETE, not committed

### Not yet written:
- `docs/superpowers/specs/2026-05-06-phase3b-freshness-orchestrator-design.md`

---

## Phase 3A File Map (from spec)

| File | Action |
|---|---|
| `src/engines/demand/prophet_features.py` | Create |
| `src/engines/demand/m1_model.py` | Create |
| `src/engines/demand/weather_ingest.py` | Create |
| `src/engines/demand/m2_model.py` | Create |
| `src/engines/demand/m4_model.py` | Create |
| `tests/unit/test_demand_models.py` | Create (6 tests) |
| `resources/jobs/demand_forecast_train.yml` | Create |
| `setup.py` | Modify (+6 demand_* entry points) |

## Phase 3B File Map (to be designed)

| File | Action |
|---|---|
| `src/engines/freshness/pulp_solver.py` | Expand existing stub |
| `src/engines/freshness/ttl_rules.py` | Create (category-configurable TTL logic) |
| `src/engines/freshness/po_client.py` | Create (BAPI_PO_CREATE1, extends BAPIClient pattern) |
| `src/engines/freshness/feature_pipeline.py` | Create (load M4 + stock + supplier master) |
| `tests/unit/test_freshness_engine.py` | Create |
| `resources/jobs/morning_pipeline.yml` | Create (unified job replacing all per-phase jobs) |
| `resources/jobs/demand_forecast_train.yml` | Create |
| `setup.py` | Modify (+5 freshness_* entry points) |
| `databricks.yml` | Modify (register morning_pipeline + demand_forecast_train) |

---

## Immediate Next Steps (resume here)

1. **Commit + push `docs/superpowers/specs/2026-05-06-phase3a-demand-models-design.md`** (already written at that path)
2. **Write `docs/superpowers/specs/2026-05-06-phase3b-freshness-orchestrator-design.md`** covering:
   - SolverInterface + SolverInput + OrderRecommendation types
   - TTL logic (category-configurable, thresholds as Pydantic config)
   - Auto-approval gates (confidence + value, both required)
   - `gold.replenishment.order_recommendations` schema
   - `BAPI_PO_CREATE1` write-back (BAPIClient extension)
   - `gold.replenishment.po_audit` append table
   - Full `morning_pipeline.yml` unified job definition
   - Go/No-Go gate: BAPI write-back confirmed in SAP QA + 1 EDI supplier test
3. **Ask user to review both specs**
4. **Invoke `superpowers:writing-plans`** for Phase 3A (argument: spec at `docs/superpowers/specs/2026-05-06-phase3a-demand-models-design.md`)
5. **Then invoke `superpowers:writing-plans`** for Phase 3B

---

## Key Constraints (never violate)

- **Clean Core**: zero SAP modifications — BAPI/OData only
- **SolverInterface abstraction**: keep PuLP/CBC swappable to OR-Tools/Gurobi — never hardcode solver APIs
- **Sweeper is deterministic**: no LLM decisions in state machine (Phase 4)
- **SHAP is load-bearing**: React ShapWaterfall must render per exception — store manager trust mechanism
- **08:15 AM EDI deadline**: all pipeline tasks must complete before this
- **No wall-clock timing assumptions**: use Databricks job `depends_on`, not scheduled start gaps

---

## GitHub State

- Repo: https://github.com/Seredayu/Ahold_POC
- Default branch: `master`
- All Phase 0–2B code merged and pushed
- GH_TOKEN used this session: revoke at https://github.com/settings/tokens (token was shared in chat — revoke it)
- Worktrees: all cleaned up, only main repo remains
