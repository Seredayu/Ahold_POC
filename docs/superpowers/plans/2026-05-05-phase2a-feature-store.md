# Phase 2A Feature Store Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build three Feature Engineering in Unity Catalog (FEU) feature tables — velocity, stock positions, and phantom collapse signals — behind a `FeatureStoreBase` ABC, wired into the daily DABs job at 05:30 AM.

**Architecture:** `FeatureStoreBase` ABC mirrors `GoldAggregateBase` from Phase 1 but calls `FeatureEngineeringClient.write_table()` instead of `saveAsTable()`. Each of the three feature tables has a pure computation function (testable with raw DataFrames) plus a thin Writer subclass that reads from Spark catalog. Entry points registered as console scripts in `setup.py`.

**Tech Stack:** PySpark, Databricks Feature Engineering in Unity Catalog (`databricks-feature-engineering`), MLflow, pytest with `tests/conftest.py` spark fixture, Databricks Asset Bundles YAML.

---

## File Structure

| File | Role |
|------|------|
| `src/medallion/feature_store/__init__.py` | Package marker |
| `src/medallion/feature_store/base.py` | `FeatureStoreBase` ABC |
| `src/medallion/feature_store/velocity_features.py` | `compute_velocity_features()` + `VelocityFeaturesWriter` |
| `src/medallion/feature_store/stock_features.py` | `compute_stock_features()` + `StockFeaturesWriter` |
| `src/medallion/feature_store/collapse_signals.py` | `compute_collapse_signals()` + `CollapseSignalsWriter` |
| `tests/unit/test_feature_store.py` | 6 unit tests |
| `resources/jobs/feature_store_refresh.yml` | DABs job (05:30 AM, 3 parallel tasks) |
| `setup.py` | Add 3 new `_entry_*` console scripts |
| `databricks.yml` | Register `feature_store_refresh` job |

---

## Context for implementers

**Existing pattern to follow:** `src/medallion/gold/aggregates.py` — `GoldAggregateBase` is the direct model. `FeatureStoreBase` is identical in structure but writes via FEU.

**Test fixture:** `tests/conftest.py` provides a `spark` fixture (PySpark local session). Import it as a function argument: `def test_foo(spark):`.

**FEU import:** `from databricks.feature_engineering import FeatureEngineeringClient`. This package is `databricks-feature-engineering` on PyPI, only available in Databricks runtime. Tests mock it — never call it directly in tests.

**Mock pattern for `run()` tests:** Patch `FeatureEngineeringClient` at the point of import (`src.medallion.feature_store.base.FeatureEngineeringClient`), capture the DataFrame passed to `write_table()`, assert `_computed_at` is in its columns.

**Working directory:** repo root (`.worktrees/phase1-medallion` or the project root after the Phase 1 PR merges).

---

## Task 1: `FeatureStoreBase` + `VelocityFeaturesWriter` (TDD)

**Files:**
- Create: `src/medallion/feature_store/__init__.py`
- Create: `src/medallion/feature_store/base.py`
- Create: `src/medallion/feature_store/velocity_features.py`
- Create: `tests/unit/test_feature_store.py`

- [ ] **Step 1: Create the package marker**

```bash
# Create empty file
touch src/medallion/feature_store/__init__.py
```

- [ ] **Step 2: Write the failing test for `_computed_at`**

Create `tests/unit/test_feature_store.py`:

```python
import pytest
from datetime import date
from decimal import Decimal

from pyspark.sql.types import (
    DateType, DecimalType, StringType, StructField, StructType,
)

VELOCITY_SCHEMA = StructType([
    StructField("werks", StringType(), True),
    StructField("unified_sku_id", StringType(), True),
    StructField("sales_7d", DecimalType(13, 3), True),
    StructField("sales_14d", DecimalType(13, 3), True),
    StructField("sales_28d", DecimalType(13, 3), True),
    StructField("sales_90d", DecimalType(13, 3), True),
    StructField("uom", StringType(), True),
])

POSITIONS_SCHEMA = StructType([
    StructField("werks", StringType(), True),
    StructField("unified_sku_id", StringType(), True),
    StructField("snapshot_date", DateType(), True),
    StructField("stock_qty", DecimalType(13, 3), True),
    StructField("uom", StringType(), True),
])

MOVEMENTS_SCHEMA = StructType([
    StructField("WERKS", StringType(), True),
    StructField("unified_sku_id", StringType(), True),
    StructField("BWART", StringType(), True),
    StructField("BUDAT", DateType(), True),
    StructField("MENGE", DecimalType(13, 3), True),
])

REGISTRY_SCHEMA = StructType([
    StructField("unified_sku_id", StringType(), True),
    StructField("shelf_life_days", DecimalType(5, 0), True),
])


def test_velocity_run_adds_computed_at(spark):
    from unittest.mock import patch
    from src.medallion.feature_store.velocity_features import VelocityFeaturesWriter

    captured = {}

    def fake_write_table(name, df, mode):
        captured["cols"] = df.columns

    with patch("src.medallion.feature_store.base.FeatureEngineeringClient") as MockFS:
        MockFS.return_value.write_table.side_effect = fake_write_table

        class _Stub(VelocityFeaturesWriter):
            def compute(self_inner):
                return spark.createDataFrame(
                    [("1000", "5000100000001", Decimal("5"), Decimal("10"),
                      Decimal("15"), Decimal("50"), "KG")],
                    VELOCITY_SCHEMA,
                )

        _Stub(spark).run()

    assert "_computed_at" in captured["cols"]
```

- [ ] **Step 3: Run test — expect ImportError (module doesn't exist yet)**

```bash
python -m pytest tests/unit/test_feature_store.py::test_velocity_run_adds_computed_at -v
```

Expected: `ImportError: No module named 'src.medallion.feature_store'`

- [ ] **Step 4: Write `src/medallion/feature_store/base.py`**

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

- [ ] **Step 5: Write `src/medallion/feature_store/velocity_features.py`**

```python
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from medallion.feature_store.base import FeatureStoreBase


def compute_velocity_features(velocity_df: DataFrame) -> DataFrame:
    """Passthrough from gold.sales.velocity — cast to canonical types."""
    return velocity_df.select(
        F.col("werks"),
        F.col("unified_sku_id"),
        F.col("sales_7d").cast("decimal(13,3)"),
        F.col("sales_14d").cast("decimal(13,3)"),
        F.col("sales_28d").cast("decimal(13,3)"),
        F.col("sales_90d").cast("decimal(13,3)"),
        F.col("uom"),
    )


class VelocityFeaturesWriter(FeatureStoreBase):
    def feature_table_name(self) -> str:
        return "velocity.sku_site_velocity"

    def primary_keys(self) -> list[str]:
        return ["werks", "unified_sku_id"]

    def compute(self) -> DataFrame:
        return compute_velocity_features(self.spark.table("gold.sales.velocity"))


def _entry_velocity_features() -> None:
    from pyspark.sql import SparkSession
    VelocityFeaturesWriter(SparkSession.getActiveSession()).run()
```

- [ ] **Step 6: Run test — expect PASS**

```bash
python -m pytest tests/unit/test_feature_store.py::test_velocity_run_adds_computed_at -v
```

Expected: `PASSED`

- [ ] **Step 7: Commit**

```bash
git add src/medallion/feature_store/ tests/unit/test_feature_store.py
git commit -m "feat(feature-store): FeatureStoreBase + VelocityFeaturesWriter, 1 test"
```

---

## Task 2: `StockFeaturesWriter` (TDD)

**Files:**
- Create: `src/medallion/feature_store/stock_features.py`
- Modify: `tests/unit/test_feature_store.py` — append 2 tests

- [ ] **Step 1: Append 2 tests to `tests/unit/test_feature_store.py`**

Add after the existing test:

```python
def test_stock_run_adds_computed_at(spark):
    from unittest.mock import patch
    from src.medallion.feature_store.stock_features import StockFeaturesWriter

    captured = {}

    def fake_write_table(name, df, mode):
        captured["cols"] = df.columns

    with patch("src.medallion.feature_store.base.FeatureEngineeringClient") as MockFS:
        MockFS.return_value.write_table.side_effect = fake_write_table

        class _Stub(StockFeaturesWriter):
            def compute(self_inner):
                return spark.createDataFrame(
                    [("1000", "5000100000001", Decimal("100"), Decimal("14.00"), "KG")],
                    StructType([
                        StructField("werks", StringType(), True),
                        StructField("unified_sku_id", StringType(), True),
                        StructField("stock_qty", DecimalType(13, 3), True),
                        StructField("days_of_cover", DecimalType(8, 2), True),
                        StructField("uom", StringType(), True),
                    ]),
                )

        _Stub(spark).run()

    assert "_computed_at" in captured["cols"]


def test_days_of_cover_null_when_sales_7d_zero(spark):
    from src.medallion.feature_store.stock_features import compute_stock_features

    positions = spark.createDataFrame(
        [("1000", "5000100000001", date(2024, 1, 1), Decimal("50"), "KG")],
        POSITIONS_SCHEMA,
    )
    velocity = spark.createDataFrame(
        [("1000", "5000100000001", Decimal("0"), Decimal("10"),
          Decimal("20"), Decimal("80"), "KG")],
        VELOCITY_SCHEMA,
    )
    result = compute_stock_features(positions, velocity).collect()
    assert result[0]["days_of_cover"] is None
```

- [ ] **Step 2: Run tests — expect 2 failures (module not found)**

```bash
python -m pytest tests/unit/test_feature_store.py::test_stock_run_adds_computed_at tests/unit/test_feature_store.py::test_days_of_cover_null_when_sales_7d_zero -v
```

Expected: `ImportError: No module named 'src.medallion.feature_store.stock_features'`

- [ ] **Step 3: Write `src/medallion/feature_store/stock_features.py`**

```python
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from medallion.feature_store.base import FeatureStoreBase


def compute_stock_features(positions_df: DataFrame, velocity_df: DataFrame) -> DataFrame:
    """
    Join daily_positions to sales_velocity, derive days_of_cover.
    days_of_cover = stock_qty / (sales_7d / 7), NULL when sales_7d = 0.
    """
    vel = velocity_df.select("werks", "unified_sku_id", "sales_7d")
    return (
        positions_df
        .join(vel, on=["werks", "unified_sku_id"], how="left")
        .withColumn(
            "days_of_cover",
            F.when(F.col("sales_7d") == 0, F.lit(None).cast("decimal(8,2)"))
             .otherwise((F.col("stock_qty") / (F.col("sales_7d") / 7)).cast("decimal(8,2)")),
        )
        .select(
            F.col("werks"),
            F.col("unified_sku_id"),
            F.col("stock_qty").cast("decimal(13,3)"),
            F.col("days_of_cover"),
            F.col("uom"),
        )
    )


class StockFeaturesWriter(FeatureStoreBase):
    def feature_table_name(self) -> str:
        return "stock.sku_site_positions"

    def primary_keys(self) -> list[str]:
        return ["werks", "unified_sku_id"]

    def compute(self) -> DataFrame:
        return compute_stock_features(
            self.spark.table("gold.inventory.daily_positions"),
            self.spark.table("gold.sales.velocity"),
        )


def _entry_stock_features() -> None:
    from pyspark.sql import SparkSession
    StockFeaturesWriter(SparkSession.getActiveSession()).run()
```

- [ ] **Step 4: Run tests — expect 2 PASSes**

```bash
python -m pytest tests/unit/test_feature_store.py::test_stock_run_adds_computed_at tests/unit/test_feature_store.py::test_days_of_cover_null_when_sales_7d_zero -v
```

Expected: `2 passed`

- [ ] **Step 5: Commit**

```bash
git add src/medallion/feature_store/stock_features.py tests/unit/test_feature_store.py
git commit -m "feat(feature-store): StockFeaturesWriter + days_of_cover guard, 2 tests"
```

---

## Task 3: `CollapseSignalsWriter` (TDD)

**Files:**
- Create: `src/medallion/feature_store/collapse_signals.py`
- Modify: `tests/unit/test_feature_store.py` — append 3 tests

- [ ] **Step 1: Append 3 tests to `tests/unit/test_feature_store.py`**

Add after the existing tests:

```python
def test_collapse_run_adds_computed_at(spark):
    from unittest.mock import patch
    from src.medallion.feature_store.collapse_signals import CollapseSignalsWriter

    captured = {}

    def fake_write_table(name, df, mode):
        captured["cols"] = df.columns

    with patch("src.medallion.feature_store.base.FeatureEngineeringClient") as MockFS:
        MockFS.return_value.write_table.side_effect = fake_write_table

        class _Stub(CollapseSignalsWriter):
            def compute(self_inner):
                return spark.createDataFrame(
                    [("1000", "5000100000001", 0.1, 3, 7)],
                    StructType([
                        StructField("werks", StringType(), True),
                        StructField("unified_sku_id", StringType(), True),
                        StructField("velocity_collapse_ratio", DecimalType(8, 4), True),
                        StructField("days_since_last_sale", DecimalType(6, 0), True),
                        StructField("shelf_life_days", DecimalType(5, 0), True),
                    ]),
                )

        _Stub(spark).run()

    assert "_computed_at" in captured["cols"]


def test_velocity_collapse_ratio_null_when_sales_28d_zero(spark):
    from src.medallion.feature_store.collapse_signals import compute_collapse_signals

    velocity = spark.createDataFrame(
        [("1000", "5000100000001", Decimal("2"), Decimal("4"), Decimal("0"), Decimal("0"), "KG")],
        VELOCITY_SCHEMA,
    )
    movements = spark.createDataFrame([], MOVEMENTS_SCHEMA)
    registry = spark.createDataFrame(
        [("5000100000001", Decimal("7"))],
        REGISTRY_SCHEMA,
    )
    result = compute_collapse_signals(velocity, movements, registry).collect()
    assert result[0]["velocity_collapse_ratio"] is None


def test_days_since_last_sale_computed_correctly(spark):
    from src.medallion.feature_store.collapse_signals import compute_collapse_signals

    ref_date = date(2024, 2, 1)
    velocity = spark.createDataFrame(
        [("1000", "5000100000001", Decimal("0"), Decimal("4"), Decimal("10"), Decimal("40"), "KG")],
        VELOCITY_SCHEMA,
    )
    movements = spark.createDataFrame(
        [("1000", "5000100000001", "601", date(2024, 1, 22), Decimal("5"))],
        MOVEMENTS_SCHEMA,
    )
    registry = spark.createDataFrame(
        [("5000100000001", Decimal("7"))],
        REGISTRY_SCHEMA,
    )
    result = compute_collapse_signals(velocity, movements, registry, reference_date=ref_date).collect()
    assert result[0]["days_since_last_sale"] == 10
```

- [ ] **Step 2: Run tests — expect 3 failures**

```bash
python -m pytest tests/unit/test_feature_store.py::test_collapse_run_adds_computed_at tests/unit/test_feature_store.py::test_velocity_collapse_ratio_null_when_sales_28d_zero tests/unit/test_feature_store.py::test_days_since_last_sale_computed_correctly -v
```

Expected: `ImportError: No module named 'src.medallion.feature_store.collapse_signals'`

- [ ] **Step 3: Write `src/medallion/feature_store/collapse_signals.py`**

```python
from datetime import date as _date
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from medallion.feature_store.base import FeatureStoreBase


def compute_collapse_signals(
    velocity_df: DataFrame,
    movements_df: DataFrame,
    registry_df: DataFrame,
    reference_date: Optional[_date] = None,
) -> DataFrame:
    """
    Compute phantom stock collapse signals per (werks, unified_sku_id).

    - velocity_collapse_ratio = sales_7d / sales_28d, NULL when sales_28d = 0
    - days_since_last_sale: days between reference_date and last BWART=601 BUDAT
    - shelf_life_days: from unified_sku_registry
    """
    ref = F.lit(reference_date) if reference_date is not None else F.current_date()

    last_sale = (
        movements_df
        .filter(F.col("BWART") == "601")
        .groupBy("WERKS", "unified_sku_id")
        .agg(F.max("BUDAT").alias("last_sale_date"))
        .withColumn("days_since_last_sale", F.datediff(ref, F.col("last_sale_date")).cast("int"))
        .select(
            F.col("WERKS").alias("werks"),
            F.col("unified_sku_id"),
            F.col("days_since_last_sale"),
        )
    )

    registry = registry_df.select(
        F.col("unified_sku_id"),
        F.col("shelf_life_days").cast("int"),
    )

    return (
        velocity_df
        .withColumn(
            "velocity_collapse_ratio",
            F.when(F.col("sales_28d") == 0, F.lit(None).cast("double"))
             .otherwise((F.col("sales_7d") / F.col("sales_28d")).cast("double")),
        )
        .join(last_sale, on=["werks", "unified_sku_id"], how="left")
        .join(registry, on="unified_sku_id", how="left")
        .select(
            F.col("werks"),
            F.col("unified_sku_id"),
            F.col("velocity_collapse_ratio"),
            F.col("days_since_last_sale"),
            F.col("shelf_life_days"),
        )
    )


class CollapseSignalsWriter(FeatureStoreBase):
    def feature_table_name(self) -> str:
        return "phantom.collapse_signals"

    def primary_keys(self) -> list[str]:
        return ["werks", "unified_sku_id"]

    def compute(self) -> DataFrame:
        return compute_collapse_signals(
            self.spark.table("gold.sales.velocity"),
            self.spark.table("silver.sap.enriched_movements"),
            self.spark.table("silver.master.unified_sku_registry"),
        )


def _entry_collapse_signals() -> None:
    from pyspark.sql import SparkSession
    CollapseSignalsWriter(SparkSession.getActiveSession()).run()
```

- [ ] **Step 4: Run all 6 tests — expect all PASS**

```bash
python -m pytest tests/unit/test_feature_store.py -v
```

Expected: `6 passed`

- [ ] **Step 5: Commit**

```bash
git add src/medallion/feature_store/collapse_signals.py tests/unit/test_feature_store.py
git commit -m "feat(feature-store): CollapseSignalsWriter + 3 tests (collapse ratio, days_since_last_sale)"
```

---

## Task 4: DABs job + `setup.py` entry points

**Files:**
- Create: `resources/jobs/feature_store_refresh.yml`
- Modify: `databricks.yml`
- Modify: `setup.py`

- [ ] **Step 1: Read current `databricks.yml` to see the `resources.jobs` section**

```bash
cat databricks.yml
```

Confirm `resources.jobs` currently has `lakeflow_trigger` and `gold_aggregates`.

- [ ] **Step 2: Create `resources/jobs/feature_store_refresh.yml`**

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

- [ ] **Step 3: Add `feature_store_refresh` to `databricks.yml`**

In `databricks.yml`, under `resources.jobs`, add after `gold_aggregates`:

```yaml
    feature_store_refresh:
      source: resources/jobs/feature_store_refresh.yml
```

- [ ] **Step 4: Update `setup.py` — add 3 new console scripts**

Replace the `entry_points` block in `setup.py` with:

```python
    entry_points={
        "console_scripts": [
            "gold_daily_positions=medallion.gold.aggregates:_entry_daily_positions",
            "gold_sales_velocity=medallion.gold.aggregates:_entry_sales_velocity",
            "gold_open_orders=medallion.gold.aggregates:_entry_open_orders",
            "fs_velocity_features=medallion.feature_store.velocity_features:_entry_velocity_features",
            "fs_stock_features=medallion.feature_store.stock_features:_entry_stock_features",
            "fs_collapse_signals=medallion.feature_store.collapse_signals:_entry_collapse_signals",
        ],
    },
```

- [ ] **Step 5: Run full test suite to confirm no regressions**

```bash
python -m pytest tests/unit/ -v
```

Expected: all existing tests pass + 6 new feature store tests pass.

- [ ] **Step 6: Commit**

```bash
git add resources/jobs/feature_store_refresh.yml databricks.yml setup.py
git commit -m "feat(feature-store): DABs job, setup.py entry points, databricks.yml wiring"
```
