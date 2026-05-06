# Phase 3B — Freshness Orchestrator Design

**Phase:** 3B — Weeks 10–11  
**Builds on:** Phase 3A Demand Models (`feature_store.demand.m4_probabilistic`), Phase 2A Feature Store (`feature_store.stock.sku_site_positions`), Phase 2B Phantom Stock (`BAPIClient` pattern)  
**Consumed by:** Phase 4 React app exception queue (`gold.replenishment.order_recommendations`), EDI 850 pipeline (Sweeper), SAP QA (`BAPI_PO_CREATE1`)

---

## Goal

Run the MILP replenishment solver daily, apply category-configurable TTL policy, evaluate auto-approval gates, write `gold.replenishment.order_recommendations`, and post approved POs to SAP via `BAPI_PO_CREATE1`. Everything wired into the unified `morning_pipeline.yml` that replaces all previous ad-hoc job files. Hard EDI deadline: **08:15 AM Amsterdam** — the pipeline must complete `bapi_po_create` before the Sweeper triggers EDI 850 generation.

---

## Architecture

### Existing Stubs (extend, do not replace)

`src/engines/freshness/solver_interface.py` already defines `SolverInterface`, `SolverInput`, and `OrderRecommendation`. These are extended with new fields; the abstract `solve()` contract is unchanged so the PuLP/CBC → OR-Tools/Gurobi swap path is preserved.

`src/engines/freshness/replenishment_quantity_optimizer.py` already implements `PuLPCBCSolver`. The TTL check currently hardcoded at `transit_to_life > 0.5` is replaced by a call to `TtlPolicy.apply()` so threshold and behaviour are category-configurable.

---

## Extended Data Types

### Extended `SolverInput`

Add to the existing `SolverInput` dataclass:

| Field | Type | Description |
|---|---|---|
| `category` | `str` | Product category — drives TTL policy (`"FRESH_PRODUCE"`, `"BAKERY"`, etc.) |
| `unit_cost` | `float` | Cost per unit (€) — used for value gate |
| `demand_p10` | `float` | P10 demand from M4 — needed for `uncertainty_spread` = P90 − P10 |

### Extended `OrderRecommendation`

Add to the existing `OrderRecommendation` dataclass:

| Field | Type | Description |
|---|---|---|
| `approval_status` | `str` | `AUTO_APPROVED`, `PENDING_REVIEW`, or `BLOCKED` |
| `approval_reason` | `str \| None` | Null if `AUTO_APPROVED`; populated for `PENDING_REVIEW`/`BLOCKED` |
| `confidence_ratio` | `float` | `uncertainty_spread / p50`; NaN if `p50 == 0` |
| `order_value` | `float` | `recommended_qty × unit_cost` (€) |
| `ttl_policy_applied` | `str` | `"HARD_BLOCK"`, `"SOFT_CAP"`, or `"PASS"` |
| `day_old_discount` | `bool` | `True` when Bakery soft cap triggered (qty reduced, not blocked) |
| `promo_lift` | `float` | Promo multiplier from M2 (passed through for ShapWaterfall) |
| `weather_lift` | `float` | Weather multiplier from M2 (passed through for ShapWaterfall) |

---

## TTL Policy

### File: `src/engines/freshness/ttl_policy.py`

Category-configurable Transit-to-Life constraint. Each category has a threshold and a behaviour:

```python
@dataclass
class CategoryTtlConfig:
    threshold: float   # transit_to_life_ratio limit
    hard_block: bool   # True = zero units; False = soft cap (qty scaled down)
```

Default configs (overridable via Databricks job parameter or unit test injection):

| Category | Threshold | Behaviour |
|---|---|---|
| `FRESH_PRODUCE` | `0.50` | Hard block — any order where `transit_to_life_ratio > 0.50` is set to `qty=0`, `blocked=True`, `solver_status="BLOCKED_TRANSIT_TO_LIFE"` |
| `BAKERY` | `0.50` | Soft cap — when `transit_to_life_ratio > 0.50`, qty is scaled to `recommended_qty × (1 − transit_to_life_ratio)`; `day_old_discount=True`; order is **not** blocked (day-old bread still saleable) |
| `DEFAULT` | `0.60` | Hard block — all unconfigured categories fall back to this |

### `apply_ttl_policy(recommendation, solver_input, config)` function

Returns a mutated `OrderRecommendation` (does not modify in-place — returns a new instance). Called inside `PuLPCBCSolver.solve()` after the LP solve, replacing the existing hardcoded `transit_to_life > 0.5` check.

---

## Approval Gate

### File: `src/engines/freshness/approval_gate.py`

Both gates must pass for `AUTO_APPROVED`. Either failure → `PENDING_REVIEW`.

| Gate | Condition for pass | Failure reason text |
|---|---|---|
| Confidence gate | `uncertainty_spread / p50 < 0.30` | `"HIGH_UNCERTAINTY: ratio={:.3f}"` |
| Value gate | `recommended_qty × unit_cost < 500.0` | `"HIGH_VALUE: €{:.2f}"` |
| Blocked | `recommendation.blocked == True` | (already `BLOCKED`; gate not evaluated) |

```python
class ApprovalGate:
    CONFIDENCE_THRESHOLD = 0.30
    VALUE_THRESHOLD = 500.0

    def evaluate(self, recommendation: OrderRecommendation, solver_input: SolverInput) -> OrderRecommendation:
        # Returns mutated copy with approval_status + approval_reason populated
        # Blocked recommendations pass through unchanged (status already BLOCKED)
        # uncertainty_spread = solver_input.demand_p90 - solver_input.demand_p10
        # confidence_ratio = uncertainty_spread / solver_input.demand_p50 (0.0 if demand_p50 == 0)
```

---

## Output Tables

### `gold.replenishment.order_recommendations` (full overwrite daily)

Written by `_entry_write_recommendations()`.

| Column | Type | Description |
|---|---|---|
| `werks` | `string` | SAP plant code |
| `unified_sku_id` | `string` | Rosetta Stone SKU ID |
| `recommended_qty` | `int` | Units to order (0 if blocked) |
| `transit_to_life_ratio` | `double` | `transit_days / shelf_life_days` |
| `approval_status` | `string` | `AUTO_APPROVED`, `PENDING_REVIEW`, `BLOCKED` |
| `approval_reason` | `string` | Null if `AUTO_APPROVED` |
| `confidence_ratio` | `double` | `uncertainty_spread / p50` |
| `order_value` | `double` | `recommended_qty × unit_cost` (€) |
| `ttl_policy_applied` | `string` | `HARD_BLOCK`, `SOFT_CAP`, `PASS` |
| `day_old_discount` | `boolean` | Bakery soft-cap flag |
| `solver_status` | `string` | PuLP status (`Optimal`, `BLOCKED_TRANSIT_TO_LIFE`, etc.) |
| `p10` | `double` | M4 P10 demand |
| `p50` | `double` | M4 P50 demand |
| `p90` | `double` | M4 P90 demand |
| `uncertainty_spread` | `double` | P90 − P10 |
| `promo_lift` | `double` | M2 promo multiplier (ShapWaterfall input) |
| `weather_lift` | `double` | M2 weather multiplier (ShapWaterfall input) |
| `_computed_at` | `timestamp` | Write timestamp |

**Primary keys:** `werks`, `unified_sku_id`

---

### `gold.replenishment.po_audit` (append-only)

Written by `_entry_bapi_po_create()` after each BAPI call. Never overwritten — full historical record.

| Column | Type | Description |
|---|---|---|
| `werks` | `string` | |
| `unified_sku_id` | `string` | |
| `recommended_qty` | `int` | Quantity sent to SAP |
| `approval_status` | `string` | Status at time of call |
| `bapi_status` | `string` | `ok` or `error` |
| `po_number` | `string` | SAP PO number returned by `BAPI_PO_CREATE1` (null on error) |
| `bapi_message` | `string` | Error text (null on success) |
| `_created_at` | `timestamp` | Append timestamp |

---

## PO Client

### File: `src/engines/freshness/po_client.py`

Extends the Phase 2B `BAPIClient` class with a `create_purchase_order()` method for `BAPI_PO_CREATE1`.

```python
from engines.phantom_stock.bapi_client import BAPIClient, BAPIError

class POClient(BAPIClient):
    def create_purchase_order(
        self,
        werks: str,
        unified_sku_id: str,
        quantity: int,
    ) -> dict:
        # POST to SAP BTP AI Core endpoint for BAPI_PO_CREATE1
        # Payload: {"POHEADER": {"COMP_CODE": werks, ...}, "POITEM": [{"MATERIAL": unified_sku_id, "QUANTITY": quantity}]}
        # Returns: {"PO_NUMBER": "4500001234", ...}
        # Raises BAPIError on HTTP error or missing PO_NUMBER in response
```

Inherits the retry logic, `RequestException` catch, and 10-second timeout from `BAPIClient`. The `BAPIError` exception class is reused unchanged.

**Sanity cap:** `_PO_CREATE_SANITY_CAP = 200` — if `AUTO_APPROVED` row count exceeds this, abort with `RuntimeError` before any BAPI calls. (Lower than phantom's 500 cap because POs have direct financial impact.)

---

## Freshness Pipeline

### File: `src/engines/freshness/freshness_pipeline.py`

Three entry point functions called as separate DABs tasks:

#### `_entry_freshness_milp_solve()`

1. Read `feature_store.demand.m4_probabilistic` (P10/P50/P90/uncertainty_spread per SKU/site)
2. Read `feature_store.demand.m2_corrected` (promo_lift, weather_lift per SKU/site)
3. Read `feature_store.stock.sku_site_positions` (current_stock, shelf_life_days)
4. Read `silver.master.unified_sku_registry` (unit_cost, transit_days, min_order_qty, max_order_qty, category, truck_capacity_units)
5. Join all four on `(werks, unified_sku_id)`; drop rows with any null in required solver fields
6. Build `list[SolverInput]`; instantiate `PuLPCBCSolver`; call `solver.solve()`
7. For each `OrderRecommendation`: apply `ApprovalGate.evaluate()`
8. Write result to `gold.replenishment.solver_output` (intermediate Delta table, overwrite) for handoff to next task

#### `_entry_write_recommendations()`

1. Read `gold.replenishment.solver_output`
2. Join back M2 lift columns (promo_lift, weather_lift) and M4 quantile columns (p10/p50/p90/uncertainty_spread)
3. Write full schema to `gold.replenishment.order_recommendations` (Delta overwrite)
4. Log MLflow metrics: `auto_approved_count`, `pending_review_count`, `blocked_count`, `total_order_value_eur`

#### `_entry_bapi_po_create()`

1. Read `gold.replenishment.order_recommendations`
2. Filter to `approval_status == "AUTO_APPROVED"` and `recommended_qty > 0`
3. Sanity cap check: if row count > `_PO_CREATE_SANITY_CAP` → raise `RuntimeError`
4. Collect rows; iterate; call `POClient.create_purchase_order()` per row
5. Build audit records (ok/error); append to `gold.replenishment.po_audit`
6. Log MLflow metrics: `po_created_count`, `po_error_count`
7. MLflow artifact: `po_audit.json` (same pattern as Phase 2B `bapi_audit.json`)

---

## Testing

7 unit tests in `tests/unit/test_freshness_orchestrator.py`:

1. `test_ttl_hard_block_fresh_produce` — `transit_days=4, shelf_life_days=6` (ratio 0.67 > 0.50) → `blocked=True`, `recommended_qty=0`, `ttl_policy_applied="HARD_BLOCK"`
2. `test_ttl_pass_fresh_produce` — `transit_days=2, shelf_life_days=6` (ratio 0.33 < 0.50) → `blocked=False`, `ttl_policy_applied="PASS"`
3. `test_ttl_soft_cap_bakery` — `transit_days=5, shelf_life_days=8` (ratio 0.625 ∈ (0.50, 0.70]) → `day_old_discount=True`, `recommended_qty` reduced, `blocked=False`
4. `test_approval_gate_auto_approved` — `uncertainty_spread=20, p50=100` (ratio 0.20 < 0.30) + `qty=10, unit_cost=30.0` (value €300 < €500) → `approval_status="AUTO_APPROVED"`
5. `test_approval_gate_pending_high_uncertainty` — `uncertainty_spread=40, p50=100` (ratio 0.40 ≥ 0.30) → `approval_status="PENDING_REVIEW"`, reason contains `"HIGH_UNCERTAINTY"`
6. `test_approval_gate_pending_high_value` — `uncertainty_spread=10, p50=100` (ratio OK) + `qty=20, unit_cost=30.0` (value €600 ≥ €500) → `approval_status="PENDING_REVIEW"`, reason contains `"HIGH_VALUE"`
7. `test_pulp_solver_produces_optimal_recommendation` — single `SolverInput` with `demand_p90=100, current_stock=20, shelf_life_days=10, transit_days=2, category="FRESH_PRODUCE"` → `solver_status="Optimal"`, `recommended_qty >= 80`

---

## DABs Jobs

### `morning_pipeline.yml` (daily 05:00 AM Amsterdam, replaces all prior ad-hoc jobs)

Full dependency graph — no wall-clock timing, all ordering enforced by `depends_on`:

```
feature_store_refresh
├── phantom_stock_score_batch       (depends_on: feature_store_refresh)
│   └── phantom_stock_write_alerts  (depends_on: phantom_stock_score_batch)
│       └── phantom_stock_bapi_writeback (depends_on: phantom_stock_write_alerts)
└── ingest_weather                  (depends_on: feature_store_refresh)
    └── score_m1                    (depends_on: ingest_weather)
        └── score_m2                (depends_on: score_m1)
            └── score_m4            (depends_on: score_m2)
                └── freshness_milp_solve (depends_on: score_m4)
                    └── freshness_write_recommendations (depends_on: freshness_milp_solve)
                        └── freshness_bapi_po_create (depends_on: freshness_write_recommendations)
```

`phantom_stock_score_batch` and `ingest_weather` both depend only on `feature_store_refresh` and therefore run in parallel.

Cron schedule: `"0 0 5 * * ?"` (Quartz, 05:00 AM daily Amsterdam = UTC+2 in summer → `"0 0 3 * * ?"` UTC; configure via Databricks timezone setting `Europe/Amsterdam`).

Task timeouts: `feature_store_refresh` 3600s; phantom tasks 1800s each; demand scoring tasks 1800s each; freshness tasks 3600s each (MILP can be slow at scale).

### Weekly training jobs (unchanged structure, separate files)

- `demand_forecast_train.yml` — Sunday 01:00 AM (existing, created in Phase 3A)
- `phantom_stock_train.yml` — Sunday 03:00 AM (existing, created in Phase 2B)

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `src/engines/freshness/solver_interface.py` | Modify | Extend `SolverInput` + `OrderRecommendation` with new fields |
| `src/engines/freshness/replenishment_quantity_optimizer.py` | Modify | Replace hardcoded TTL check with `apply_ttl_policy()` call |
| `src/engines/freshness/ttl_policy.py` | Create | `CategoryTtlConfig` + `apply_ttl_policy()` |
| `src/engines/freshness/approval_gate.py` | Create | `ApprovalGate.evaluate()` — confidence + value gates |
| `src/engines/freshness/freshness_pipeline.py` | Create | Three entry points: milp_solve, write_recommendations, bapi_po_create |
| `src/engines/freshness/po_client.py` | Create | `POClient(BAPIClient)` — `create_purchase_order()` for `BAPI_PO_CREATE1` |
| `tests/unit/test_freshness_orchestrator.py` | Create | 7 unit tests |
| `resources/jobs/morning_pipeline.yml` | Create | Unified 11-task morning pipeline |
| `setup.py` | Modify | Add 3 `freshness_*` console_scripts |
| `databricks.yml` | Modify | Replace phantom + demand job references with `morning_pipeline` |

---

## Error Handling

- **MILP infeasible:** `solver_status = "Infeasible"`; `recommended_qty = 0`; `approval_status = "PENDING_REVIEW"`; `approval_reason = "SOLVER_INFEASIBLE"` — written to `order_recommendations`; no BAPI call attempted
- **POClient BAPIError:** Row written to `po_audit` with `bapi_status="error"`, `bapi_message` set; pipeline continues to next row; error count logged to MLflow; does **not** raise — partial success is acceptable
- **Sanity cap breach:** `RuntimeError` raised; entire `bapi_po_create` task fails; DABs marks task `FAILED`; no POs created; `po_audit` is not written for that run
- **M4 table missing:** `_entry_freshness_milp_solve()` raises `AnalysisException`; task fails; `write_recommendations` and `bapi_po_create` are blocked via `depends_on`

---

## Go/No-Go Gate (Week 11)

Two conditions must both be confirmed before Phase 3B is considered production-ready:

1. **SAP QA write-back**: At least one `BAPI_PO_CREATE1` call returns a real PO number in the SAP QA system — confirmed in `gold.replenishment.po_audit` with `bapi_status="ok"` and non-null `po_number`
2. **EDI supplier test**: At least one EDI 850 document successfully transmitted to a pilot supplier via the Azure Logic Apps / SFTP route — confirmed by Sweeper log in `gold.sweeper.run_log`

Either condition failing blocks Phase 4 (React exception queue UI) from going live.
