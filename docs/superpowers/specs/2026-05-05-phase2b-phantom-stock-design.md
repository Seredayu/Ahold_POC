# Phase 2B Design — Engine 1: Phantom Stock Detector

**Date:** 2026-05-05
**Sprint phase:** 2B (Weeks 6–8)
**Status:** Approved

## Context

Engine 1 detects "phantom stockouts" — SKUs where SAP reports positive stock but sales have stopped, indicating the product is physically missing or misplaced. The classifier triggers stock corrections via `BAPI_GOODSMVT_CREATE` through SAP BTP AI Core. Target: precision ≥ 85%, +4% On-Shelf Availability (OSA) improvement.

Depends on Phase 2A Feature Store being live. The Week 8 Go/No-Go gate is backtest precision ≥ 80% before proceeding to Phase 3.

## Architecture

```
Weekly (Sunday 03:00 AM):
  phantom-stock-train
    ├─ generate_labels     gold.inventory.daily_positions + gold.sales.velocity → heuristic labels
    └─ train_model         XGBoost → MLflow registry "phantom_stock_detector/Production"

Daily (06:00 AM, after feature-store-refresh):
  phantom-stock-score
    ├─ score_batch         FEU feature lookup → PhantomStockClassifier.predict()
    ├─ write_alerts        → gold.phantom_stock.alerts (full overwrite)
    └─ bapi_writeback      score ≥ 0.95 → BAPI_GOODSMVT_CREATE (auto-correct)
                           0.85–0.95  → gold.phantom_stock.review_queue (manager review)
                           < 0.85     → no action
```

## File Map

```
src/engines/phantom_stock/
  classifier.py              ← expand existing stub (add feature schema, SHAP logging)
  label_generator.py         ← generate_labels() pure function (new)
  feature_pipeline.py        ← load_features() via FEU score_batch() (new)
  bapi_client.py             ← BAPIClient.post_goods_movement() via SAP BTP AI Core (new)
tests/unit/
  test_phantom_stock.py      ← 6 tests (new)
resources/jobs/
  phantom_stock_train.yml    ← DABs job: weekly Sunday 03:00 AM (new)
  phantom_stock_score.yml    ← DABs job: daily 06:00 AM (new)
```

## Label Generation

### Heuristic: velocity-collapse

```python
def generate_labels(
    inventory_df: DataFrame,   # gold.inventory.daily_positions
    velocity_df: DataFrame,    # gold.sales.velocity
) -> DataFrame:
    """
    Label (werks, unified_sku_id) as phantom if:
      stock_qty > 0 AND sales_7d = 0 AND sales_28d > 5

    Returns DataFrame with columns: werks, unified_sku_id, is_phantom (int 0/1), label_date
    """
```

- `stock_qty > 0`: system shows stock available
- `sales_7d = 0`: no sales in the past 7 days
- `sales_28d > 5`: had real recent sales history (filters out slow-movers and new listings)

Labels are generated for today's snapshot only. Training uses 90 days of historical snapshots by replaying the heuristic over daily position + velocity history.

`label_generator.py` is a pure function with no Spark session dependency — takes DataFrames, returns a DataFrame. Fully unit-testable with `spark.createDataFrame()`.

## Feature Pipeline (Inference)

```python
# feature_pipeline.py
def load_features(spark: SparkSession) -> DataFrame:
    """
    Load all three feature tables from FEU and join on (werks, unified_sku_id).
    Returns a single DataFrame ready for PhantomStockClassifier.predict().
    """
    fs = FeatureEngineeringClient()
    velocity = fs.read_table("feature_store.velocity.sku_site_velocity")
    stock = fs.read_table("feature_store.stock.sku_site_positions")
    collapse = fs.read_table("feature_store.phantom.collapse_signals")
    return (
        velocity
        .join(stock.drop("uom", "_computed_at"), on=["werks", "unified_sku_id"], how="inner")
        .join(collapse.drop("_computed_at"), on=["werks", "unified_sku_id"], how="inner")
    )
```

Feature columns consumed by XGBoost (8 total):
`sales_7d`, `sales_14d`, `sales_28d`, `sales_90d`, `stock_qty`, `days_of_cover`, `velocity_collapse_ratio`, `days_since_last_sale`

`shelf_life_days` is carried through to `gold.phantom_stock.alerts` for context but excluded from model input (it is a property of the SKU, not a signal of stockout).

## Classifier Expansion

Expand existing `PhantomStockClassifier` stub in `classifier.py`:

1. Add `FEATURE_COLUMNS` class constant — ordered list of 8 feature column names
2. Add `SHAP` logging to `train()` — log SHAP summary plot as MLflow artifact for explainability
3. Update `predict()` to accept a Spark DataFrame (not Pandas) — use `.toPandas()` internally, return results as Spark DataFrame for downstream writes
4. Add `action` column derivation in `predict()`:
   - score ≥ 0.95 → `"AUTO_CORRECTED"`
   - 0.85 ≤ score < 0.95 → `"PENDING_REVIEW"`
   - score < 0.85 → `"BELOW_THRESHOLD"`

## Output Tables

### `gold.phantom_stock.alerts`

Full overwrite daily. Consumed by Phase 4 React app exception queue.

| Column | Type | Notes |
|--------|------|-------|
| `werks` | string | |
| `unified_sku_id` | string | |
| `phantom_score` | double | raw model probability |
| `is_phantom` | boolean | score ≥ 0.85 |
| `action` | string | AUTO_CORRECTED / PENDING_REVIEW / BELOW_THRESHOLD |
| `shelf_life_days` | int | context only, not model input |
| `_scored_at` | timestamp | |

### `gold.phantom_stock.review_queue`

Append-only inserts (Delta `mode="append"`). Rows where `action = PENDING_REVIEW`. The Phase 4 React app writes resolutions back as a separate append with `_resolution` populated — never updates existing rows, preserving full audit history.

| Column | Type | Notes |
|--------|------|-------|
| `werks` | string | |
| `unified_sku_id` | string | |
| `phantom_score` | double | |
| `_scored_at` | timestamp | |
| `_resolved_at` | timestamp | NULL on insert; populated when manager acts (Phase 4) |
| `_resolution` | string | NULL on insert; APPROVED / REJECTED written by Phase 4 app |

## BAPI Write-back

### `bapi_client.py`

```python
class BAPIClient:
    """
    Posts goods movement corrections to SAP ECC via SAP BTP AI Core HTTP endpoint.
    Credentials read from Databricks Secrets: scope "sap-btp", key "ai-core-token".
    Every call logged to MLflow as a run artifact for full auditability.
    """

    def __init__(self, endpoint_url: str, token: str):
        self._endpoint = endpoint_url
        self._token = token

    def post_goods_movement(
        self,
        werks: str,
        unified_sku_id: str,
        movement_type: str = "562",  # stock correction — phantom removal
        quantity: float = 0.0,       # zero-out the phantom stock
    ) -> dict:
        """POST to SAP BTP AI Core. Raises BAPIError on HTTP 4xx/5xx."""
```

- Movement type `562` = inventory difference posting (standard phantom correction)
- Quantity `0.0` = zero out the on-hand balance for the phantom SKU
- Retries: 3 attempts with exponential backoff (1s, 2s, 4s)
- `BAPIError` raised on non-2xx response — caller decides whether to dead-letter or alert

`bapi_client.py` is thin by design — no business logic, just HTTP + retry. Mocked in all tests.

## DABs Wiring

### `resources/jobs/phantom_stock_train.yml`

```yaml
name: phantom-stock-train-${bundle.target}
schedule:
  quartz_cron_expression: "0 0 3 ? * SUN"
  timezone_id: "Europe/Amsterdam"
  pause_status: ${var.schedule_enabled == "true" ? "UNPAUSED" : "PAUSED"}
tasks:
  - task_key: generate_labels
    python_wheel_task:
      package_name: ahold_freshness_poc
      entry_point: phantom_generate_labels
    timeout_seconds: 3600
  - task_key: train_model
    depends_on:
      - task_key: generate_labels
    python_wheel_task:
      package_name: ahold_freshness_poc
      entry_point: phantom_train_model
    timeout_seconds: 7200
```

### `resources/jobs/phantom_stock_score.yml`

```yaml
name: phantom-stock-score-${bundle.target}
schedule:
  quartz_cron_expression: "0 0 6 * * ?"
  timezone_id: "Europe/Amsterdam"
  pause_status: ${var.schedule_enabled == "true" ? "UNPAUSED" : "PAUSED"}
tasks:
  - task_key: score_batch
    python_wheel_task:
      package_name: ahold_freshness_poc
      entry_point: phantom_score_batch
    timeout_seconds: 1800
  - task_key: write_alerts
    depends_on:
      - task_key: score_batch
    python_wheel_task:
      package_name: ahold_freshness_poc
      entry_point: phantom_write_alerts
    timeout_seconds: 900
  - task_key: bapi_writeback
    depends_on:
      - task_key: write_alerts
    python_wheel_task:
      package_name: ahold_freshness_poc
      entry_point: phantom_bapi_writeback
    timeout_seconds: 1800
```

## Testing

### `tests/unit/test_phantom_stock.py` — 6 tests

1. `generate_labels` correctly labels phantom: stock_qty > 0, sales_7d = 0, sales_28d > 5 → is_phantom = 1
2. `generate_labels` ignores zero-stock rows: stock_qty = 0 → is_phantom = 0 regardless of sales
3. `generate_labels` ignores no-prior-sales rows: sales_28d ≤ 5 → is_phantom = 0 (slow-mover guard)
4. `PhantomStockClassifier.predict()` returns `action = "AUTO_CORRECTED"` for score ≥ 0.95
5. `PhantomStockClassifier.predict()` returns `action = "PENDING_REVIEW"` for score in [0.85, 0.95)
6. `BAPIClient.post_goods_movement()` raises `BAPIError` on HTTP 500 response

## Verification

Phase 2B is verified when:

1. `phantom-stock-train` job runs green; model registered in MLflow under `phantom_stock_detector/Production`
2. Backtest precision on 90-day historical labels ≥ 80% (Week 8 Go/No-Go gate)
3. `phantom-stock-score` runs daily; `gold.phantom_stock.alerts` populated with `_scored_at`
4. At least one `AUTO_CORRECTED` row visible in `gold.phantom_stock.alerts` within 7 days of live run
5. Corresponding `BAPI_GOODSMVT_CREATE` call confirmed in SAP QA movement log (movement type 562)
6. `gold.phantom_stock.review_queue` populated for `PENDING_REVIEW` cases
