# Session Handoff — 2026-05-06

---

## 1. What Was Accomplished This Session

### Phase 2B — Phantom Stock Detector (COMPLETE ✅)

Resumed from a previous session that had hit context limits mid-Task #28. All 5 tasks completed, reviewed (spec + code quality), fixed, and merged to `master` via PR #1.

| Task | Description | Result |
|---|---|---|
| #27 | `label_generator.py` + 3 label tests | Done in prior session |
| #28 | `classifier.py` expansion (XGBoost predict_proba, SHAP, per-row SHAP values) | Fixed `predict_proba` vs `predict` bug; added per-row SHAP JSON column |
| #29 | `bapi_client.py` + BAPIError + retry | Fixed `RequestException` bypass bug, empty-body 204 guard, `response is None` mixed-failure guard |
| #30 | `feature_pipeline.py` + entry points | Removed dead `spark` parameter from `load_features()` |
| #31 | DABs jobs + `setup.py` + `databricks.yml` | `phantom_stock_train.yml`, `phantom_stock_score.yml`, 11 console_scripts |

Final review found 3 more issues fixed before merge:
- `BELOW_THRESHOLD` rows were leaking into `gold.phantom_stock.alerts` → filtered out
- `_entry_bapi_writeback` had no row-count guard → added 500-row sanity cap
- Per-row SHAP values missing from `predict()` → added `shap.TreeExplainer` + JSON column for React `ShapWaterfall.tsx`

Branch `feature/phase2b-phantom-stock` pushed, PR #1 created with GH_TOKEN, merged, local worktrees cleaned up.

### Phase 3 — Brainstorming (IN PROGRESS 🔄)

Ran `superpowers:brainstorming` skill. Explored codebase, asked 4 clarifying questions, agreed on full design. Phase 3A spec written and committed. Phase 3B spec NOT YET WRITTEN.

---

## 2. Exact Decisions Made and Why

| Decision | Choice | Reason |
|---|---|---|
| Demand model stack | M1 + M2 + M4 (full) | User chose option C — full production-grade stack for the POC |
| Training history | 104 weeks (2 years) | Captures two seasonal cycles; better for Fresh Produce YoY comparisons |
| M2 signals | Promo + Weather (Open-Meteo free tier) | User chose option C — both signals for maximum accuracy |
| TTL handling | Category-configurable | Fresh Produce = hard block (zero tolerance); Bakery = soft cap (day-old discount) |
| PO auto-approval | Both gates required | Confidence gate (uncertainty_spread/p50 < 0.30) AND value gate (< €500); either failure → PENDING_REVIEW |
| Phase 3 structure | Two sub-projects: 3A then 3B | Mirrors Phase 2A→2B pattern; demand models feed MILP just as feature store feeds classifier |
| Job orchestration | Single unified `morning_pipeline.yml` | User rejected wall-clock timing gaps ("why do you sleep?"); enforced `depends_on` chains eliminate race conditions |

### Unified morning pipeline (enforced dependency order):
```
morning_pipeline — daily 05:00 AM Amsterdam
├── feature_store_refresh
├── phantom_stock_score        (depends_on: feature_store_refresh)
│   ├── score_batch / write_alerts / bapi_writeback
└── demand_forecast_score      (depends_on: feature_store_refresh — parallel with phantom)
    ├── ingest_weather / score_m1 / score_m2 / score_m4
    └── freshness_score        (depends_on: score_m4)
        ├── milp_solve / write_recommendations / bapi_po_create
```

Weekly training (separate jobs, no deadline):
- `demand_forecast_train` — Sunday 01:00 AM
- `phantom_stock_train` — Sunday 03:00 AM (existing)

---

## 3. Files Created / Modified This Session

### Phase 2B (all committed to master via PR #1):
| File | Action |
|---|---|
| `src/engines/phantom_stock/label_generator.py` | Created |
| `src/engines/phantom_stock/classifier.py` | Rewritten — `predict_proba`, per-row SHAP, empty DF guard, `SparkSession.getActiveSession()` |
| `src/engines/phantom_stock/bapi_client.py` | Created — retry, `RequestException` catch, `response is None` guard, 10s timeout |
| `src/engines/phantom_stock/feature_pipeline.py` | Created — `load_features()`, `_entry_score_batch/write_alerts/bapi_writeback` |
| `tests/unit/test_phantom_stock.py` | Created — 7 tests, `predict_proba` mocks fixed to `[[neg, pos]]` shape |
| `resources/jobs/phantom_stock_train.yml` | Created |
| `resources/jobs/phantom_stock_score.yml` | Created |
| `setup.py` | Created — 11 console_scripts |
| `databricks.yml` | Modified — +2 phantom jobs |

### Phase 3 brainstorming (committed to master directly):
| File | Action |
|---|---|
| `docs/superpowers/specs/2026-05-06-phase3a-demand-models-design.md` | Created ✅ committed |
| `docs/superpowers/specs/2026-05-06-phase3b-freshness-orchestrator-design.md` | NOT YET WRITTEN |
| `docs/session-handoff.md` | Created ✅ committed (this file) |

### Previously missing, caught and pushed this session:
| File | Action |
|---|---|
| `docs/superpowers/plans/2026-05-05-phase1-medallion-rosetta-stone.md` | Was untracked — committed and pushed |

---

## 4. Open Questions and Blockers

### Design gaps (Phase 3B spec not yet written):
- TTL threshold values not yet set — what is the minimum acceptable TTL ratio per category? (e.g. Fresh Produce: 0.5 = at least 50% of shelf life remaining after transit)
- `unit_cost` source — where does this come from in the SAP estate? (likely `silver.sap.material_master` MBEW table)
- `transit_days` source — supplier master or SAP purchasing info records (PIR)?
- `truck_capacity_units` — is this per route or per SKU/supplier combination?
- `silver.promo.calendar` — does this table exist yet, or does it need to be created from SAP SD condition records in Phase 3A?

### Infrastructure blockers (must be in flight since Week 1):
- SAP RFC/BAPI authorization for `BAPI_PO_CREATE1` — 3-week approval lead time; must be done
- EDI supplier partner agreement — 4–6 week lead time; must be started for Phase 4
- Open-Meteo API — free tier, no auth needed; unblocked

### Go/No-Go gates:
- **Week 8 gate** (before Phase 3 starts): Engine 1 backtest precision ≥ 80% — must pass
- **Week 10 gate**: M1 MAE ≤ 15% of mean daily demand per SKU (`m1_mae_pct` MLflow metric)
- **Week 11 gate**: BAPI_PO_CREATE1 write-back confirmed in SAP QA + 1 EDI supplier test

---

## 5. Exact Starting Prompt for Next Session

Paste this verbatim to resume:

```
Resume Phase 3 design from docs/session-handoff.md.

Context:
- Phase 2B Phantom Stock Detector is complete and merged to master (PR #1)
- Phase 3 brainstorming is in progress — all design decisions approved (see handoff)
- Phase 3A spec is written at docs/superpowers/specs/2026-05-06-phase3a-demand-models-design.md

Immediate tasks:
1. Write docs/superpowers/specs/2026-05-06-phase3b-freshness-orchestrator-design.md covering:
   - SolverInterface (existing stub in src/engines/freshness/) + SolverInput + OrderRecommendation types
   - Category-configurable TTL logic (Fresh Produce = hard block, Bakery = soft cap)
   - Auto-approval gates: confidence (uncertainty_spread/p50 < 0.30) AND value (qty × unit_cost < €500)
   - gold.replenishment.order_recommendations schema
   - BAPI_PO_CREATE1 write-back (extends BAPIClient pattern from Phase 2B)
   - gold.replenishment.po_audit append table
   - Full morning_pipeline.yml unified job (see handoff for dependency graph)
   - Go/No-Go gate: BAPI write-back in SAP QA + 1 EDI supplier test confirmed
2. Commit and push both specs to master
3. Ask user to review both specs
4. Invoke superpowers:writing-plans for Phase 3A (spec: docs/superpowers/specs/2026-05-06-phase3a-demand-models-design.md)
5. Then invoke superpowers:writing-plans for Phase 3B

Key constraints (never violate):
- SolverInterface must stay abstract — PuLP/CBC swappable to OR-Tools/Gurobi
- No wall-clock timing — all DABs tasks use depends_on
- SHAP explainability mandatory for React ShapWaterfall per exception
- 08:15 AM EDI hard deadline
- Clean Core — zero SAP modifications
```
