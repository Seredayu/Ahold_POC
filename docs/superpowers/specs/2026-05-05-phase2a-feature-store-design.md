# Phase 2A Design — Feature Store (Feature Engineering in Unity Catalog)

**Date:** 2026-05-05
**Sprint phase:** 2A (Weeks 6–8)
**Status:** Approved

## Context

Phase 2A builds the Feature Store that all three engines depend on. It runs immediately after Phase 1 Gold aggregates and registers reusable feature tables in Databricks Feature Engineering in Unity Catalog (FEU). Phase 2B (Phantom Stock Detector) and Phase 3 (LightGBM demand model) both read from these tables via FEU feature lookups.

## Architecture

```
05:00 AM  gold-aggregates (Phase 1)
  → 05:30 AM  feature-store-refresh (new DABs job)
      ├─ velocity_features     reads gold.sales.velocity
      ├─ stock_features        reads gold.inventory.daily_positions + gold.sales.velocity
      └─ collapse_signals      reads silver.sap.enriched_movements + silver.master.unified_sku_registry
```

**Approach:** `FeatureStoreBase` ABC mirroring `GoldAggregateBase` from Phase 1, but using `FeatureEngineeringClient.write_table()` instead of `saveAsTable()`. Three concrete writers, one per feature table. Registered in the Python wheel with `_entry_*` no-arg wrappers. All tables in Unity Catalog under the `feature_store` catalog.

## File Map

```
src/medallion/feature_store/
  __init__.py                  ← new
  base.py                      ← FeatureStoreBase ABC (new)
  velocity_features.py         ← VelocityFeaturesWriter (new)
  stock_features.py            ← StockFeaturesWriter (new)
  collapse_signals.py          ← CollapseSignalsWriter (new)
tests/unit/
  test_feature_store.py        ← 6 tests (new)
resources/jobs/
  feature_store_refresh.yml    ← DABs job (new)
```

`databricks.yml` gains one new job entry. `setup.py` gains three new `_entry_*` console scripts.

## FeatureStoreBase

```python
from abc import ABC, abstractmethod
from databricks.feature_engineering import FeatureEngineeringClient
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


class FeatureStoreBase(ABC):
    def __init__(self, spark: SparkSession, catalog: str = "feature_store"):
        self.spark = spark
        self.catalog = catalog
        self._fs = FeatureEngineeringClient()

    @abstractmethod
    def feature_table_name(self) -> str: ...

    @abstractmethod
    def primary_keys(self) -> list[str]: ...

    @abstractmethod
    def compute(self) -> DataFrame: ...

    def run(self) -> None:
        df = self.compute().withColumn("_computed_at", F.current_timestamp())
        self._fs.write_table(
            name=f"{self.catalog}.{self.feature_table_name()}",
            df=df,
            mode="overwrite",
        )
```

All three writers share the same `primary_keys()` return value: `["werks", "unified_sku_id"]`.

## Feature Tables

### `feature_store.velocity.sku_site_velocity`

Source: `gold.sales.velocity`

Logic: direct passthrough — rename columns to lowercase, cast to consistent types.

| Column | Type | Notes |
|--------|------|-------|
| `werks` | string | primary key |
| `unified_sku_id` | string | primary key |
| `sales_7d` | decimal(13,3) | |
| `sales_14d` | decimal(13,3) | |
| `sales_28d` | decimal(13,3) | |
| `sales_90d` | decimal(13,3) | |
| `uom` | string | |
| `_computed_at` | timestamp | added by `run()` |

### `feature_store.stock.sku_site_positions`

Source: `gold.inventory.daily_positions` LEFT JOIN `gold.sales.velocity` ON (werks, unified_sku_id)

Logic:
- `days_of_cover = stock_qty / (sales_7d / 7)` — NULL when `sales_7d = 0` (avoids division by zero)
- All columns from `daily_positions`, plus `days_of_cover`

| Column | Type | Notes |
|--------|------|-------|
| `werks` | string | primary key |
| `unified_sku_id` | string | primary key |
| `stock_qty` | decimal(13,3) | |
| `days_of_cover` | decimal(8,2) | NULL when sales_7d = 0 |
| `uom` | string | |
| `_computed_at` | timestamp | added by `run()` |

### `feature_store.phantom.collapse_signals`

Source: `gold.sales.velocity` + `silver.sap.enriched_movements` (BWART 601 rows only) + `silver.master.unified_sku_registry`

Logic:
- `velocity_collapse_ratio = sales_7d / sales_28d` — NULL when `sales_28d = 0` (from `gold.sales.velocity`)
- `days_since_last_sale`: days between current_date() and the most recent BUDAT where BWART = 601 per (WERKS, unified_sku_id), from `silver.sap.enriched_movements`
- `shelf_life_days`: from `silver.master.unified_sku_registry`

| Column | Type | Notes |
|--------|------|-------|
| `werks` | string | primary key |
| `unified_sku_id` | string | primary key |
| `velocity_collapse_ratio` | double | NULL when sales_28d = 0 |
| `days_since_last_sale` | int | days since last BWART 601 movement |
| `shelf_life_days` | int | from unified_sku_registry |
| `_computed_at` | timestamp | added by `run()` |

## DABs Wiring

### `resources/jobs/feature_store_refresh.yml`

```yaml
name: feature-store-refresh-${bundle.target}
schedule:
  quartz_cron_expression: "0 30 5 * * ?"
  timezone_id: "Europe/Amsterdam"
  pause_status: ${var.schedule_enabled == "true" ? "UNPAUSED" : "PAUSED"}
tasks:
  - task_key: velocity_features
    python_wheel_task:
      package_name: ahold_freshness_poc
      entry_point: fs_velocity_features
    timeout_seconds: 1800
  - task_key: stock_features
    python_wheel_task:
      package_name: ahold_freshness_poc
      entry_point: fs_stock_features
    timeout_seconds: 1800
  - task_key: collapse_signals
    python_wheel_task:
      package_name: ahold_freshness_poc
      entry_point: fs_collapse_signals
    timeout_seconds: 1800
```

All three tasks run in parallel (no `depends_on`). Register in `databricks.yml` under `resources.jobs`.

## Testing

All tests use the existing `tests/conftest.py` PySpark session fixture.

### `tests/unit/test_feature_store.py` — 6 tests

1. `VelocityFeaturesWriter.run()` adds `_computed_at` column (write interception pattern from Phase 1)
2. `StockFeaturesWriter.run()` adds `_computed_at` column
3. `CollapseSignalsWriter.run()` adds `_computed_at` column
4. `days_of_cover` is NULL when `sales_7d = 0` (division-by-zero guard)
5. `velocity_collapse_ratio` is NULL when `sales_28d = 0`
6. `days_since_last_sale` is computed correctly from enriched_movements

## Verification

Phase 2A is verified when:

1. `feature-store-refresh` job runs green after `gold-aggregates`
2. All three feature tables queryable in Unity Catalog with `_computed_at` populated
3. `feature_store.phantom.collapse_signals` shows non-NULL `velocity_collapse_ratio` for SKUs with recent sales history
4. FEU feature lookup succeeds in a notebook: `FeatureEngineeringClient().read_table("feature_store.phantom.collapse_signals")`
