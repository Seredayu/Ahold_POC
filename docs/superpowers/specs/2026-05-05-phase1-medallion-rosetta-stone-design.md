# Phase 1 Design — Medallion Pipelines + Rosetta Stone

**Date:** 2026-05-05
**Sprint phase:** 1 (Weeks 3–5)
**Status:** Approved

## Context

Phase 1 builds the data foundation that all three engines depend on. Bronze tables landed by Phase 0 (Lakeflow Connect CDC) are transformed through Silver into Gold analytical aggregates. The critical path item is the Rosetta Stone: a ≥95% match rate between SAP material numbers and EAN barcodes is the Week 5 Go/No-Go gate that unblocks all model training in Phase 2.

## Architecture

```
bronze.sap.materials      ─┐
bronze.sap.inventory       ├─→ [silver-rosetta-stone DLT]     ─→ silver.master.unified_sku_registry
bronze.sap.open_orders    ─┘       (04:00 AM)

bronze.sap.materials      ─┐
bronze.sap.inventory       ├─→ [silver-demand-features DLT]   ─→ silver.sap.materials_clean
bronze.sap.open_orders    ─┘       (04:00 AM, after rosetta)      silver.sap.inventory_clean
                                                                   silver.sap.open_orders_clean
                                                                   silver.sap.enriched_movements

silver.sap.*              ─→ [gold-aggregates Spark batch]    ─→ gold.inventory.daily_positions
                                   (05:00 AM, after Silver)       gold.sales.velocity
                                                                   gold.replenishment.open_orders
```

**Approach:** DLT for Silver (schema evolution, data quality expectations, pipeline monitoring), Spark batch Python classes for Gold (precise windowed aggregations, unit-testable). Follows patterns established in Phase 0 Bronze Auto Loaders.

## File Map

```
src/medallion/silver/
  rosetta_stone/
    entity_resolution.py          ← expand existing stub
    dlt_pipeline.py               ← DLT table definitions (new)
  demand_features/
    __init__.py                   ← new
    dlt_pipeline.py               ← DLT table definitions (new)
src/medallion/gold/
  aggregates.py                   ← GoldAggregateBase + 3 writers (new)
tests/unit/
  test_rosetta_stone.py           ← new
  test_silver_demand_features.py  ← new
  test_gold_aggregates.py         ← new
resources/pipelines/
  silver_rosetta_stone.yml        ← DABs DLT pipeline (new)
  silver_demand_features.yml      ← DABs DLT pipeline (new)
resources/jobs/
  gold_aggregates.yml             ← DABs Spark batch job (new)
```

## Rosetta Stone

### Purpose

Maps SAP MATNR → EAN11 barcode → `unified_sku_id`. Every downstream model and Gold table joins on `unified_sku_id`. The ≥95% match rate gate at Week 5 blocks Phase 2.

### Matching Logic

1. Read MATNR + EAN11 + MAKTX + MTART + MATKL + MEINS + MHDRZ + MHDLP from `bronze.sap.materials`
2. Deduplicate on MATNR — keep row with latest LAEDA (last change date)
3. EAN11 non-null → `unified_sku_id = EAN11`, `match_confidence = 1.0`
4. EAN11 null → `unified_sku_id = "MATNR-" + MATNR`, `match_confidence = 0.0`
5. Write to `silver.master.unified_sku_registry`

Rows with `match_confidence = 0.0` flow through all pipelines unchanged. Models in Phase 2 filter on `match_confidence = 1.0` for training; the Sweeper uses the full registry.

### Output Schema: `silver.master.unified_sku_registry`

| Column | Type | Source | Notes |
|--------|------|--------|-------|
| `unified_sku_id` | string | derived | EAN11 or `"MATNR-{MATNR}"` |
| `sap_material_number` | string | MATNR | primary SAP key |
| `ean_barcode` | string | EAN11 | nullable |
| `material_description` | string | MAKTX | |
| `material_type` | string | MTART | |
| `material_group` | string | MATKL | |
| `base_uom` | string | MEINS | |
| `shelf_life_days` | decimal(5,0) | MHDRZ | days |
| `min_remaining_shelf_life` | decimal(5,0) | MHDLP | days |
| `match_confidence` | double | derived | 1.0 or 0.0 |
| `source_banner` | string | hardcoded | `"AH_NL"` for POC |
| `_ingested_at` | timestamp | DLT | managed by DLT |

### DLT Data Quality Expectations

```python
@dlt.expect_or_drop("valid_matnr", "sap_material_number IS NOT NULL")
@dlt.expect("match_rate_informational", "match_confidence >= 0.0")
```

### Match Rate Gate

`entity_resolution.py` exposes `get_match_rate()` and `is_ready()` (already stubbed). At Week 5, run against live `silver.master.unified_sku_registry`:

```python
rs = RosettaStone(spark)
assert rs.is_ready(), f"Match rate {rs.get_match_rate():.1%} < 95% — models blocked"
```

## Silver Demand Features

### Purpose

Cleans and validates the three Bronze SAP tables, then produces a pre-joined enriched movements fact table. Downstream Gold and Phase 2 Feature Store query `enriched_movements` directly — no further joins needed.

### DLT Pipeline: `silver-demand-features`

Depends on `silver-rosetta-stone` completing first (DABs job dependency).

#### `silver.sap.materials_clean`

- Source: `bronze.sap.materials`
- Dedup on MATNR, keep latest LAEDA
- Filter: MTART IN (`FERT`, `HALB`, `ROH`) — excludes services, packaging, assets
- Cast MHDRZ, MHDLP to integer
- DLT expectation: `expect_or_drop("valid_matnr", "MATNR IS NOT NULL")`

#### `silver.sap.inventory_clean`

- Source: `bronze.sap.inventory`
- Dedup on (MBLNR, ZEILE)
- Filter: BWART IN (`101`, `102`, `261`, `262`, `601`, `602`)
  - `101`/`102`: goods receipt / reversal
  - `261`/`262`: goods issue to cost center / reversal
  - `601`/`602`: delivery to customer / reversal
- Cast BUDAT, CPUDT to date
- DLT expectation: `expect_or_drop("valid_document", "MBLNR IS NOT NULL AND ZEILE IS NOT NULL")`

#### `silver.sap.open_orders_clean`

- Source: `bronze.sap.open_orders`
- Dedup on (EBELN, EBELP)
- Filter: MENGE > 0 (excludes cancelled lines)
- Cast EINDT, BEDAT to date
- DLT expectation: `expect_or_drop("valid_po_line", "EBELN IS NOT NULL AND EBELP IS NOT NULL")`

#### `silver.sap.enriched_movements`

- Source: `silver.sap.inventory_clean` LEFT JOIN `silver.sap.materials_clean` ON MATNR, LEFT JOIN `silver.master.unified_sku_registry` ON MATNR
- Adds columns: `unified_sku_id`, `material_description`, `material_type`, `shelf_life_days`, `match_confidence`
- This is the single denormalized fact table consumed by Gold aggregates and Phase 2 Feature Store

## Gold Aggregates

### Purpose

Three daily Spark batch writers producing analytical aggregates for engines and the Feature Store. Full overwrite each run — no CDC complexity at Gold layer. All writers subclass `GoldAggregateBase`.

### `GoldAggregateBase`

```python
class GoldAggregateBase(ABC):
    def __init__(self, spark, catalog="gold"):
        self.spark = spark
        self.catalog = catalog

    @abstractmethod
    def target_table(self) -> str: ...

    @abstractmethod
    def compute(self) -> DataFrame: ...

    def run(self) -> None:
        df = self.compute()
        df = df.withColumn("_computed_at", F.current_timestamp())
        df.write.format("delta").mode("overwrite").saveAsTable(
            f"{self.catalog}.{self.target_table()}"
        )
```

### `gold.inventory.daily_positions`

Source: `silver.sap.inventory_clean` joined to `silver.master.unified_sku_registry` on MATNR

Logic:
- Apply movement type sign: receipts (101) positive, issues (261, 601) negative, reversals (102, 262, 602) invert sign
- Sum signed MENGE by (WERKS, unified_sku_id, BUDAT)
- Compute cumulative position with window ordered by BUDAT, take latest snapshot per (WERKS, unified_sku_id)

| Column | Type |
|--------|------|
| `werks` | string |
| `unified_sku_id` | string |
| `snapshot_date` | date |
| `stock_qty` | decimal(13,3) |
| `uom` | string |
| `_computed_at` | timestamp |

### `gold.sales.velocity`

Source: `silver.sap.enriched_movements` filtered to BWART IN (`601`, `602`)

Logic: for each (WERKS, unified_sku_id), sum MENGE over 7, 14, 28, 90 calendar days ending today (reversals 602 subtract)

| Column | Type |
|--------|------|
| `werks` | string |
| `unified_sku_id` | string |
| `sales_7d` | decimal(13,3) |
| `sales_14d` | decimal(13,3) |
| `sales_28d` | decimal(13,3) |
| `sales_90d` | decimal(13,3) |
| `uom` | string |
| `_computed_at` | timestamp |

### `gold.replenishment.open_orders`

Source: `silver.sap.open_orders_clean` joined to `silver.master.unified_sku_registry` on MATNR

Logic: sum MENGE by (WERKS, unified_sku_id) where EINDT >= current_date() (future deliveries only)

| Column | Type |
|--------|------|
| `werks` | string |
| `unified_sku_id` | string |
| `open_qty` | decimal(13,3) |
| `earliest_delivery` | date |
| `uom` | string |
| `_computed_at` | timestamp |

## DABs Wiring

### New pipeline resources

**`resources/pipelines/silver_rosetta_stone.yml`**
```yaml
name: silver-rosetta-stone-${bundle.target}
target: silver
channel: CURRENT
clusters:
  - label: default
    policy_id: ${var.cluster_policy_id}
    autoscale:
      min_workers: 1
      max_workers: 4
libraries:
  - notebook:
      path: /Shared/freshness-poc/notebooks/silver_rosetta_stone
configuration:
  bronze_catalog: bronze
  silver_catalog: silver
```

**`resources/pipelines/silver_demand_features.yml`**
```yaml
name: silver-demand-features-${bundle.target}
target: silver
channel: CURRENT
clusters:
  - label: default
    policy_id: ${var.cluster_policy_id}
    autoscale:
      min_workers: 1
      max_workers: 4
libraries:
  - notebook:
      path: /Shared/freshness-poc/notebooks/silver_demand_features
configuration:
  bronze_catalog: bronze
  silver_catalog: silver
```

### New job resource

**`resources/jobs/gold_aggregates.yml`**
```yaml
name: gold-aggregates-${bundle.target}
schedule:
  quartz_cron_expression: "0 0 5 * * ?"
  timezone_id: "Europe/Amsterdam"
  pause_status: ${var.schedule_enabled == "true" ? "UNPAUSED" : "PAUSED"}
tasks:
  - task_key: daily_positions
    python_wheel_task:
      package_name: ahold_freshness_poc
      entry_point: gold_daily_positions
    timeout_seconds: 1800
  - task_key: sales_velocity
    python_wheel_task:
      package_name: ahold_freshness_poc
      entry_point: gold_sales_velocity
    timeout_seconds: 1800
  - task_key: open_orders
    python_wheel_task:
      package_name: ahold_freshness_poc
      entry_point: gold_open_orders
    timeout_seconds: 1800
```

### Execution order

```
04:00 AM  lakeflow-daily-bronze  (Phase 0 — Bronze landing)
  → silver-rosetta-stone
    → silver-demand-features
05:00 AM  gold-aggregates (daily_positions + sales_velocity + open_orders in parallel)
05:00 AM  freshness-replenishment-daily (Phase 3+ engines — unchanged)
```

## Testing

All tests use the existing `tests/conftest.py` PySpark session fixture. Same known limitation: requires `pyspark` installed locally or Databricks runtime.

### `tests/unit/test_rosetta_stone.py` — 6 tests

1. EAN11 present → `unified_sku_id = EAN11`, `match_confidence = 1.0`
2. EAN11 null → `unified_sku_id = "MATNR-{MATNR}"`, `match_confidence = 0.0`
3. Duplicate MATNR → dedup keeps row with latest LAEDA
4. `get_match_rate()` returns correct ratio with known data
5. `is_ready()` returns True when rate ≥ 0.95
6. `is_ready()` returns False when rate < 0.95

### `tests/unit/test_silver_demand_features.py` — 5 tests

1. Materials: invalid MTART row is filtered out
2. Materials: duplicate MATNR → dedup keeps latest LAEDA
3. Inventory: invalid BWART row is dropped
4. Open orders: MENGE = 0 row is dropped
5. Enriched movements: join produces `unified_sku_id` column

### `tests/unit/test_gold_aggregates.py` — 6 tests

1. Daily positions: receipt + issue net correctly to stock_qty
2. Daily positions: `_computed_at` column present
3. Sales velocity: sales_7d, sales_14d, sales_28d, sales_90d all computed
4. Sales velocity: reversal (602) subtracts from sum
5. Open orders: past delivery date filtered out
6. Open orders: `_computed_at` column present

## Verification

Phase 1 is verified when:

1. `silver-rosetta-stone` DLT pipeline runs green with zero dropped records for MATNR nulls
2. `silver.master.unified_sku_registry` is queryable in Unity Catalog with `unified_sku_id` populated for all rows
3. `RosettaStone(spark).get_match_rate()` ≥ 0.95 against live data (Week 5 gate)
4. `silver-demand-features` DLT pipeline runs green; `silver.sap.enriched_movements` has `unified_sku_id` non-null for all rows with `match_confidence = 1.0`
5. `gold-aggregates` job completes; all three Gold tables queryable with `_computed_at` populated
6. `gold.sales.velocity` shows non-zero `sales_7d` for at least one (WERKS, unified_sku_id) pair
