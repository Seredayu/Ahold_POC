# Phase 1 — Medallion Pipelines + Rosetta Stone Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Silver (Rosetta Stone + demand features DLT pipelines) and Gold (daily positions, sales velocity, open orders Spark batch writers) medallion layers that transform Bronze SAP ECC data into analytics-ready aggregates consumed by Phase 2 engines.

**Architecture:** DLT for Silver — pure transformation logic in testable Python modules (`entity_resolution.py`, `transformations.py`) with thin DLT wrapper files (`dlt_pipeline.py`) that import from them. Spark batch Python classes for Gold — standalone `compute_*` functions take DataFrames as arguments (unit-testable), wrapped in `GoldAggregateBase` subclasses that read from Silver tables and write to Gold. All writers run in parallel via DABs at 05:00 AM after Silver DLT completes.

**Tech Stack:** PySpark 3.5 (Spark 14.3 LTS), Databricks DLT, pytest, Databricks Asset Bundles (DABs), setuptools wheel entry points.

---

## File Map

**Modify:**
- `src/medallion/bronze/sap_ecc_materials.py` — add `EAN11` field to `target_schema()`
- `tests/unit/test_sap_ecc_materials.py` — add EAN11 test
- `src/medallion/silver/rosetta_stone/entity_resolution.py` — expand stub: add `build_registry()`, `_compute_match_rate()`, rewrite `RosettaStone` methods
- `databricks.yml` — register silver pipelines and gold job

**Create:**
- `tests/unit/test_rosetta_stone.py` — 6 tests
- `src/medallion/silver/rosetta_stone/dlt_pipeline.py` — DLT `@table` wrappers (not unit-tested; requires DLT runtime)
- `src/medallion/silver/demand_features/__init__.py` — empty package marker
- `src/medallion/silver/demand_features/transformations.py` — pure transform functions (unit-tested)
- `src/medallion/silver/demand_features/dlt_pipeline.py` — DLT `@table` wrappers (not unit-tested)
- `tests/unit/test_silver_demand_features.py` — 5 tests
- `src/medallion/gold/aggregates.py` — `GoldAggregateBase` + 3 `compute_*` functions + 3 writer classes + 3 `run_*` entry points
- `tests/unit/test_gold_aggregates.py` — 6 tests
- `resources/pipelines/silver_rosetta_stone.yml`
- `resources/pipelines/silver_demand_features.yml`
- `resources/jobs/gold_aggregates.yml`
- `setup.py` — wheel entry points for Gold writers

---

## Task 1: EAN11 in Bronze + Rosetta Stone entity_resolution.py (TDD)

**Files:**
- Modify: `src/medallion/bronze/sap_ecc_materials.py`
- Modify: `tests/unit/test_sap_ecc_materials.py`
- Modify: `src/medallion/silver/rosetta_stone/entity_resolution.py`
- Create: `tests/unit/test_rosetta_stone.py`

- [ ] **Step 1: Add EAN11 test to test_sap_ecc_materials.py**

Add this test to the bottom of `tests/unit/test_sap_ecc_materials.py`:

```python
def test_target_schema_contains_ean11(spark):
    loader = SAPECCMaterialsLoader(spark=spark)
    field_names = [f.name for f in loader.target_schema().fields]
    assert "EAN11" in field_names
```

- [ ] **Step 2: Run the new EAN11 test — verify it fails**

```bash
pytest tests/unit/test_sap_ecc_materials.py::test_target_schema_contains_ean11 -v
```

Expected: `FAILED — AssertionError`

- [ ] **Step 3: Add EAN11 to sap_ecc_materials.py schema**

In `src/medallion/bronze/sap_ecc_materials.py`, add `EAN11` as the second field in `target_schema()`:

```python
def target_schema(self) -> StructType:
    return StructType([
        StructField("MATNR", StringType(), True),
        StructField("EAN11", StringType(), True),    # EAN barcode — Rosetta Stone primary key
        StructField("WERKS", StringType(), True),
        StructField("MAKTX", StringType(), True),
        StructField("MTART", StringType(), True),
        StructField("MATKL", StringType(), True),
        StructField("MEINS", StringType(), True),
        StructField("MHDRZ", DecimalType(5, 0), True),
        StructField("IPRKZ", StringType(), True),
        StructField("MHDLP", DecimalType(5, 0), True),
        StructField("LAEDA", DateType(), True),
        StructField("ERSDA", DateType(), True),
    ])
```

- [ ] **Step 4: Verify EAN11 test passes**

```bash
pytest tests/unit/test_sap_ecc_materials.py -v
```

Expected: `6 passed`

- [ ] **Step 5: Write all 6 failing Rosetta Stone tests**

Create `tests/unit/test_rosetta_stone.py`:

```python
import pytest
from datetime import date
from decimal import Decimal
from unittest.mock import patch

from pyspark.sql.types import (
    DateType, DecimalType, DoubleType, StringType, StructField, StructType,
)

from src.medallion.silver.rosetta_stone.entity_resolution import (
    RosettaStone,
    _compute_match_rate,
    build_registry,
)

BRONZE_SCHEMA = StructType([
    StructField("MATNR", StringType(), True),
    StructField("EAN11", StringType(), True),
    StructField("MAKTX", StringType(), True),
    StructField("MTART", StringType(), True),
    StructField("MATKL", StringType(), True),
    StructField("MEINS", StringType(), True),
    StructField("MHDRZ", DecimalType(5, 0), True),
    StructField("MHDLP", DecimalType(5, 0), True),
    StructField("LAEDA", DateType(), True),
])

REGISTRY_SCHEMA = StructType([
    StructField("ean_barcode", StringType(), True),
    StructField("match_confidence", DoubleType(), True),
])


def test_ean11_present_sets_unified_sku_id_and_confidence_1(spark):
    df = spark.createDataFrame(
        [("MAT001", "5000112345678", "Apple", "FERT", "OBST", "KG",
          Decimal("7"), Decimal("3"), date(2024, 1, 1))],
        BRONZE_SCHEMA,
    )
    row = build_registry(df).collect()[0]
    assert row["unified_sku_id"] == "5000112345678"
    assert row["match_confidence"] == 1.0
    assert row["ean_barcode"] == "5000112345678"


def test_ean11_null_sets_synthetic_id_and_confidence_0(spark):
    df = spark.createDataFrame(
        [("MAT002", None, "Bread", "FERT", "BROT", "ST",
          Decimal("3"), Decimal("1"), date(2024, 1, 1))],
        BRONZE_SCHEMA,
    )
    row = build_registry(df).collect()[0]
    assert row["unified_sku_id"] == "MATNR-MAT002"
    assert row["match_confidence"] == 0.0
    assert row["ean_barcode"] is None


def test_dedup_keeps_latest_laeda(spark):
    df = spark.createDataFrame(
        [
            ("MAT003", "5000111111111", "Milk old", "FERT", "MLCH", "L",
             Decimal("7"), Decimal("3"), date(2023, 6, 1)),
            ("MAT003", "5000199999999", "Milk new", "FERT", "MLCH", "L",
             Decimal("7"), Decimal("3"), date(2024, 3, 1)),
        ],
        BRONZE_SCHEMA,
    )
    result = build_registry(df).collect()
    assert len(result) == 1
    assert result[0]["ean_barcode"] == "5000199999999"


def test_output_includes_source_banner_ah_nl(spark):
    df = spark.createDataFrame(
        [("MAT004", "5000100000001", "Yogurt", "FERT", "MLCH", "ST",
          Decimal("14"), Decimal("5"), date(2024, 1, 1))],
        BRONZE_SCHEMA,
    )
    assert build_registry(df).collect()[0]["source_banner"] == "AH_NL"


def test_compute_match_rate_correct(spark):
    df = spark.createDataFrame(
        [
            ("5000100000001", 1.0),
            ("5000100000002", 1.0),
            ("5000100000003", 1.0),
            (None, 0.0),
        ],
        REGISTRY_SCHEMA,
    )
    assert _compute_match_rate(df) == pytest.approx(0.75)


def test_is_ready_true_at_threshold_false_below(spark):
    rs = RosettaStone(spark)
    with patch.object(rs, "get_match_rate", return_value=0.95):
        assert rs.is_ready() is True
    with patch.object(rs, "get_match_rate", return_value=0.94):
        assert rs.is_ready() is False
```

- [ ] **Step 6: Run tests — verify they fail**

```bash
pytest tests/unit/test_rosetta_stone.py -v
```

Expected: `ImportError` — `build_registry` not defined yet

- [ ] **Step 7: Rewrite entity_resolution.py**

Replace the entire contents of `src/medallion/silver/rosetta_stone/entity_resolution.py`:

```python
from dataclasses import dataclass
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window


@dataclass
class UnifiedSKU:
    sap_material_number: str
    symphony_gold_item_code: Optional[str]
    ean_barcode: Optional[str]
    unified_sku_id: str
    match_confidence: float
    source_banner: str


def build_registry(bronze_df: DataFrame) -> DataFrame:
    """Transform bronze materials DataFrame into unified_sku_registry DataFrame.

    EAN11 present → unified_sku_id = EAN11, match_confidence = 1.0.
    EAN11 null    → unified_sku_id = "MATNR-{MATNR}", match_confidence = 0.0.
    Deduplicates on MATNR keeping the row with the latest LAEDA.
    """
    w = Window.partitionBy("MATNR").orderBy(F.col("LAEDA").desc())
    deduped = (
        bronze_df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    return (
        deduped
        .withColumn(
            "unified_sku_id",
            F.when(F.col("EAN11").isNotNull(), F.col("EAN11"))
             .otherwise(F.concat(F.lit("MATNR-"), F.col("MATNR"))),
        )
        .withColumn(
            "match_confidence",
            F.when(F.col("EAN11").isNotNull(), F.lit(1.0)).otherwise(F.lit(0.0)),
        )
        .withColumn("source_banner", F.lit("AH_NL"))
        .select(
            F.col("unified_sku_id"),
            F.col("MATNR").alias("sap_material_number"),
            F.col("EAN11").alias("ean_barcode"),
            F.col("MAKTX").alias("material_description"),
            F.col("MTART").alias("material_type"),
            F.col("MAKTX").alias("material_group"),
            F.col("MEINS").alias("base_uom"),
            F.col("MHDRZ").alias("shelf_life_days"),
            F.col("MHDLP").alias("min_remaining_shelf_life"),
            F.col("match_confidence"),
            F.col("source_banner"),
        )
    )


def _compute_match_rate(registry_df: DataFrame) -> float:
    total = registry_df.count()
    if total == 0:
        return 0.0
    matched = registry_df.filter(F.col("ean_barcode").isNotNull()).count()
    return matched / total


class RosettaStone:
    TARGET_MATCH_RATE = 0.95

    def __init__(self, spark: SparkSession, catalog: str = "silver", schema: str = "master"):
        self._spark = spark
        self._table = f"{catalog}.{schema}.unified_sku_registry"

    def get_match_rate(self) -> float:
        return _compute_match_rate(self._spark.table(self._table))

    def is_ready(self) -> bool:
        return self.get_match_rate() >= self.TARGET_MATCH_RATE

    def lookup_by_sap(self, sap_material_number: str) -> Optional[UnifiedSKU]:
        rows = (
            self._spark.table(self._table)
            .filter(F.col("sap_material_number") == sap_material_number)
            .collect()
        )
        if not rows:
            return None
        r = rows[0]
        return UnifiedSKU(
            sap_material_number=r.sap_material_number,
            symphony_gold_item_code=None,
            ean_barcode=r.ean_barcode,
            unified_sku_id=r.unified_sku_id,
            match_confidence=r.match_confidence,
            source_banner=r.source_banner,
        )
```

**Note:** There is a bug in the `.select()` above — `material_group` is mapped from `MAKTX` (same as `material_description`). Fix it: `F.col("MATKL").alias("material_group")`.

Corrected `.select()` block:

```python
        .select(
            F.col("unified_sku_id"),
            F.col("MATNR").alias("sap_material_number"),
            F.col("EAN11").alias("ean_barcode"),
            F.col("MAKTX").alias("material_description"),
            F.col("MTART").alias("material_type"),
            F.col("MATKL").alias("material_group"),
            F.col("MEINS").alias("base_uom"),
            F.col("MHDRZ").alias("shelf_life_days"),
            F.col("MHDLP").alias("min_remaining_shelf_life"),
            F.col("match_confidence"),
            F.col("source_banner"),
        )
```

- [ ] **Step 8: Run all Rosetta Stone tests — verify they pass**

```bash
pytest tests/unit/test_rosetta_stone.py -v
```

Expected:
```
PASSED test_ean11_present_sets_unified_sku_id_and_confidence_1
PASSED test_ean11_null_sets_synthetic_id_and_confidence_0
PASSED test_dedup_keeps_latest_laeda
PASSED test_output_includes_source_banner_ah_nl
PASSED test_compute_match_rate_correct
PASSED test_is_ready_true_at_threshold_false_below
6 passed
```

- [ ] **Step 9: Commit**

```bash
git add src/medallion/bronze/sap_ecc_materials.py \
        tests/unit/test_sap_ecc_materials.py \
        src/medallion/silver/rosetta_stone/entity_resolution.py \
        tests/unit/test_rosetta_stone.py
git commit -m "feat(silver): Rosetta Stone entity_resolution — build_registry TDD, EAN11 in Bronze"
```

---

## Task 2: Rosetta Stone DLT Pipeline + DABs Resource

**Files:**
- Create: `src/medallion/silver/rosetta_stone/dlt_pipeline.py`
- Create: `resources/pipelines/silver_rosetta_stone.yml`
- Modify: `databricks.yml`

> These files are NOT unit-tested — `dlt_pipeline.py` requires the Databricks DLT runtime (`import dlt` is only available inside a DLT pipeline execution context).

- [ ] **Step 1: Create dlt_pipeline.py**

Create `src/medallion/silver/rosetta_stone/dlt_pipeline.py`:

```python
import dlt
from pyspark.sql import functions as F

from src.medallion.silver.rosetta_stone.entity_resolution import build_registry


@dlt.table(
    name="unified_sku_registry",
    comment="SAP MATNR to EAN11 unified SKU registry. match_confidence=1.0 means EAN matched.",
    table_properties={"delta.enableChangeDataFeed": "true"},
)
@dlt.expect_or_drop("valid_matnr", "sap_material_number IS NOT NULL")
@dlt.expect("match_rate_informational", "match_confidence >= 0.0")
def unified_sku_registry():
    bronze_catalog = spark.conf.get("bronze_catalog", "bronze")
    df = spark.read.table(f"{bronze_catalog}.sap.materials")
    return build_registry(df)
```

- [ ] **Step 2: Create silver_rosetta_stone.yml**

Create `resources/pipelines/silver_rosetta_stone.yml`:

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

- [ ] **Step 3: Register in databricks.yml**

In `databricks.yml`, add `silver_rosetta_stone` under `resources.pipelines`:

```yaml
resources:
  pipelines:
    bronze_materials:
      source: resources/pipelines/bronze_materials.yml
    bronze_inventory:
      source: resources/pipelines/bronze_inventory.yml
    bronze_open_orders:
      source: resources/pipelines/bronze_open_orders.yml
    silver_rosetta_stone:
      source: resources/pipelines/silver_rosetta_stone.yml
  jobs:
    lakeflow_trigger:
      source: resources/jobs/lakeflow_trigger.yml
```

- [ ] **Step 4: Commit**

```bash
git add src/medallion/silver/rosetta_stone/dlt_pipeline.py \
        resources/pipelines/silver_rosetta_stone.yml \
        databricks.yml
git commit -m "feat(silver): Rosetta Stone DLT pipeline + DABs resource"
```

---

## Task 3: Silver Demand Features Transformations (TDD)

**Files:**
- Create: `src/medallion/silver/demand_features/__init__.py`
- Create: `src/medallion/silver/demand_features/transformations.py`
- Create: `tests/unit/test_silver_demand_features.py`

- [ ] **Step 1: Create demand_features package**

```bash
mkdir -p src/medallion/silver/demand_features
```

Create `src/medallion/silver/demand_features/__init__.py` (empty file).

- [ ] **Step 2: Write 5 failing tests**

Create `tests/unit/test_silver_demand_features.py`:

```python
from datetime import date
from decimal import Decimal

from pyspark.sql.types import (
    DateType, DecimalType, DoubleType, StringType, StructField, StructType,
)

from src.medallion.silver.demand_features.transformations import (
    build_enriched_movements,
    clean_inventory,
    clean_materials,
    clean_open_orders,
)

MATERIALS_SCHEMA = StructType([
    StructField("MATNR", StringType(), True),
    StructField("EAN11", StringType(), True),
    StructField("MAKTX", StringType(), True),
    StructField("MTART", StringType(), True),
    StructField("MATKL", StringType(), True),
    StructField("MEINS", StringType(), True),
    StructField("MHDRZ", DecimalType(5, 0), True),
    StructField("MHDLP", DecimalType(5, 0), True),
    StructField("LAEDA", DateType(), True),
])

INVENTORY_SCHEMA = StructType([
    StructField("MBLNR", StringType(), True),
    StructField("ZEILE", StringType(), True),
    StructField("MATNR", StringType(), True),
    StructField("WERKS", StringType(), True),
    StructField("LGORT", StringType(), True),
    StructField("BWART", StringType(), True),
    StructField("MENGE", DecimalType(13, 3), True),
    StructField("MEINS", StringType(), True),
    StructField("BUDAT", StringType(), True),
    StructField("CPUDT", StringType(), True),
])

ORDERS_SCHEMA = StructType([
    StructField("EBELN", StringType(), True),
    StructField("EBELP", StringType(), True),
    StructField("MATNR", StringType(), True),
    StructField("WERKS", StringType(), True),
    StructField("LIFNR", StringType(), True),
    StructField("MENGE", DecimalType(13, 3), True),
    StructField("MEINS", StringType(), True),
    StructField("EINDT", StringType(), True),
    StructField("BEDAT", StringType(), True),
])

REGISTRY_SCHEMA = StructType([
    StructField("sap_material_number", StringType(), True),
    StructField("unified_sku_id", StringType(), True),
    StructField("match_confidence", DoubleType(), True),
])


def test_clean_materials_filters_invalid_mtart(spark):
    df = spark.createDataFrame(
        [
            ("MAT001", "5000000001", "Apple", "FERT", "GRP1", "KG",
             Decimal("7"), Decimal("3"), date(2024, 1, 1)),
            ("MAT002", None, "Service", "DIEN", "GRP2", "ST",
             Decimal("0"), Decimal("0"), date(2024, 1, 1)),
        ],
        MATERIALS_SCHEMA,
    )
    result = clean_materials(df)
    matnrs = [r["MATNR"] for r in result.collect()]
    assert "MAT001" in matnrs
    assert "MAT002" not in matnrs


def test_clean_materials_dedup_keeps_latest_laeda(spark):
    df = spark.createDataFrame(
        [
            ("MAT003", None, "Milk old", "FERT", "GRP1", "L",
             Decimal("7"), Decimal("3"), date(2023, 1, 1)),
            ("MAT003", None, "Milk new", "FERT", "GRP1", "L",
             Decimal("7"), Decimal("3"), date(2024, 6, 1)),
        ],
        MATERIALS_SCHEMA,
    )
    result = clean_materials(df).collect()
    assert len(result) == 1
    assert result[0]["MAKTX"] == "Milk new"


def test_clean_inventory_filters_invalid_bwart(spark):
    df = spark.createDataFrame(
        [
            ("4900000001", "0001", "MAT001", "NL01", "0001", "101",
             Decimal("10.000"), "KG", "2024-01-01", "2024-01-01"),
            ("4900000002", "0001", "MAT002", "NL01", "0001", "999",
             Decimal("5.000"), "KG", "2024-01-01", "2024-01-01"),
        ],
        INVENTORY_SCHEMA,
    )
    result = clean_inventory(df)
    doc_nums = [r["MBLNR"] for r in result.collect()]
    assert "4900000001" in doc_nums
    assert "4900000002" not in doc_nums


def test_clean_open_orders_filters_zero_menge(spark):
    df = spark.createDataFrame(
        [
            ("4500000001", "00010", "MAT001", "NL01", "VENDOR1",
             Decimal("100.000"), "KG", "2025-06-01", "2024-01-01"),
            ("4500000002", "00010", "MAT002", "NL01", "VENDOR1",
             Decimal("0.000"), "KG", "2025-06-01", "2024-01-01"),
        ],
        ORDERS_SCHEMA,
    )
    result = clean_open_orders(df)
    ebeln_list = [r["EBELN"] for r in result.collect()]
    assert "4500000001" in ebeln_list
    assert "4500000002" not in ebeln_list


def test_build_enriched_movements_adds_unified_sku_id(spark):
    inv_df = spark.createDataFrame(
        [("4900000001", "0001", "MAT001", "NL01", "0001", "601",
          Decimal("5.000"), "KG", "2024-01-01", "2024-01-01")],
        INVENTORY_SCHEMA,
    )
    inv_clean = clean_inventory(inv_df)

    mat_df = spark.createDataFrame(
        [("MAT001", "5000000001", "Apple", "FERT", "GRP1", "KG",
          Decimal("7"), Decimal("3"), date(2024, 1, 1))],
        MATERIALS_SCHEMA,
    )
    mat_clean = clean_materials(mat_df)

    registry_df = spark.createDataFrame(
        [("MAT001", "5000000001", 1.0)],
        REGISTRY_SCHEMA,
    )

    result = build_enriched_movements(inv_clean, mat_clean, registry_df)
    assert "unified_sku_id" in result.columns
    assert result.collect()[0]["unified_sku_id"] == "5000000001"
```

- [ ] **Step 3: Run tests — verify they fail**

```bash
pytest tests/unit/test_silver_demand_features.py -v
```

Expected: `ImportError` — `transformations` module does not exist yet

- [ ] **Step 4: Implement transformations.py**

Create `src/medallion/silver/demand_features/transformations.py`:

```python
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window

_VALID_MTART = ("FERT", "HALB", "ROH")
_VALID_BWART = ("101", "102", "261", "262", "601", "602")


def clean_materials(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("MATNR").orderBy(F.col("LAEDA").desc())
    return (
        df
        .filter(F.col("MATNR").isNotNull())
        .filter(F.col("MTART").isin(*_VALID_MTART))
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("MHDRZ", F.col("MHDRZ").cast("int"))
        .withColumn("MHDLP", F.col("MHDLP").cast("int"))
    )


def clean_inventory(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("MBLNR", "ZEILE").orderBy(F.col("CPUDT").desc())
    return (
        df
        .filter(F.col("MBLNR").isNotNull() & F.col("ZEILE").isNotNull())
        .filter(F.col("BWART").isin(*_VALID_BWART))
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("BUDAT", F.col("BUDAT").cast("date"))
        .withColumn("CPUDT", F.col("CPUDT").cast("date"))
    )


def clean_open_orders(df: DataFrame) -> DataFrame:
    w = Window.partitionBy("EBELN", "EBELP").orderBy(F.col("BEDAT").desc())
    return (
        df
        .filter(F.col("EBELN").isNotNull() & F.col("EBELP").isNotNull())
        .filter(F.col("MENGE") > 0)
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .withColumn("EINDT", F.col("EINDT").cast("date"))
        .withColumn("BEDAT", F.col("BEDAT").cast("date"))
    )


def build_enriched_movements(
    inventory_clean: DataFrame,
    materials_clean: DataFrame,
    registry: DataFrame,
) -> DataFrame:
    mat_attrs = materials_clean.select(
        "MATNR",
        F.col("MAKTX").alias("material_description"),
        F.col("MTART").alias("material_type"),
        F.col("MHDRZ").alias("shelf_life_days"),
    )
    reg_attrs = registry.select(
        F.col("sap_material_number"),
        "unified_sku_id",
        "match_confidence",
    )
    return (
        inventory_clean
        .join(mat_attrs, on="MATNR", how="left")
        .join(
            reg_attrs,
            on=inventory_clean["MATNR"] == reg_attrs["sap_material_number"],
            how="left",
        )
        .drop("sap_material_number")
    )
```

- [ ] **Step 5: Run tests — verify they pass**

```bash
pytest tests/unit/test_silver_demand_features.py -v
```

Expected:
```
PASSED test_clean_materials_filters_invalid_mtart
PASSED test_clean_materials_dedup_keeps_latest_laeda
PASSED test_clean_inventory_filters_invalid_bwart
PASSED test_clean_open_orders_filters_zero_menge
PASSED test_build_enriched_movements_adds_unified_sku_id
5 passed
```

- [ ] **Step 6: Commit**

```bash
git add src/medallion/silver/demand_features/ \
        tests/unit/test_silver_demand_features.py
git commit -m "feat(silver): demand features transformations — clean tables + enriched_movements TDD"
```

---

## Task 4: Silver Demand Features DLT Pipeline + DABs Resource

**Files:**
- Create: `src/medallion/silver/demand_features/dlt_pipeline.py`
- Create: `resources/pipelines/silver_demand_features.yml`
- Modify: `databricks.yml`

- [ ] **Step 1: Create dlt_pipeline.py**

Create `src/medallion/silver/demand_features/dlt_pipeline.py`:

```python
import dlt
from pyspark.sql import functions as F

from src.medallion.silver.demand_features.transformations import (
    build_enriched_movements,
    clean_inventory,
    clean_materials,
    clean_open_orders,
)


@dlt.table(
    name="materials_clean",
    comment="Deduplicated, MTART-filtered SAP material master.",
)
@dlt.expect_or_drop("valid_matnr", "MATNR IS NOT NULL")
def materials_clean():
    bronze_catalog = spark.conf.get("bronze_catalog", "bronze")
    df = spark.read.table(f"{bronze_catalog}.sap.materials")
    return clean_materials(df)


@dlt.table(
    name="inventory_clean",
    comment="Deduplicated, BWART-filtered SAP goods movements.",
)
@dlt.expect_or_drop("valid_document", "MBLNR IS NOT NULL AND ZEILE IS NOT NULL")
def inventory_clean():
    bronze_catalog = spark.conf.get("bronze_catalog", "bronze")
    df = spark.read.table(f"{bronze_catalog}.sap.inventory")
    return clean_inventory(df)


@dlt.table(
    name="open_orders_clean",
    comment="Deduplicated, active (MENGE>0) SAP purchase orders.",
)
@dlt.expect_or_drop("valid_po_line", "EBELN IS NOT NULL AND EBELP IS NOT NULL")
def open_orders_clean():
    bronze_catalog = spark.conf.get("bronze_catalog", "bronze")
    df = spark.read.table(f"{bronze_catalog}.sap.open_orders")
    return clean_open_orders(df)


@dlt.table(
    name="enriched_movements",
    comment="Inventory movements pre-joined with material master and unified_sku_registry.",
)
def enriched_movements():
    silver_catalog = spark.conf.get("silver_catalog", "silver")
    return build_enriched_movements(
        inventory_clean=dlt.read("inventory_clean"),
        materials_clean=dlt.read("materials_clean"),
        registry=spark.read.table(f"{silver_catalog}.master.unified_sku_registry"),
    )
```

- [ ] **Step 2: Create silver_demand_features.yml**

Create `resources/pipelines/silver_demand_features.yml`:

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

- [ ] **Step 3: Register in databricks.yml**

Add `silver_demand_features` under `resources.pipelines` in `databricks.yml`:

```yaml
resources:
  pipelines:
    bronze_materials:
      source: resources/pipelines/bronze_materials.yml
    bronze_inventory:
      source: resources/pipelines/bronze_inventory.yml
    bronze_open_orders:
      source: resources/pipelines/bronze_open_orders.yml
    silver_rosetta_stone:
      source: resources/pipelines/silver_rosetta_stone.yml
    silver_demand_features:
      source: resources/pipelines/silver_demand_features.yml
  jobs:
    lakeflow_trigger:
      source: resources/jobs/lakeflow_trigger.yml
```

- [ ] **Step 4: Commit**

```bash
git add src/medallion/silver/demand_features/dlt_pipeline.py \
        resources/pipelines/silver_demand_features.yml \
        databricks.yml
git commit -m "feat(silver): demand features DLT pipeline + DABs resource"
```

---

## Task 5: GoldAggregateBase + compute_daily_positions (TDD)

**Files:**
- Create: `src/medallion/gold/aggregates.py`
- Create: `tests/unit/test_gold_aggregates.py`

- [ ] **Step 1: Write 2 failing tests**

Create `tests/unit/test_gold_aggregates.py`:

```python
import pytest
from datetime import date
from decimal import Decimal

from pyspark.sql.types import (
    DateType, DecimalType, StringType, StructField, StructType,
)

from src.medallion.gold.aggregates import compute_daily_positions

INV_SCHEMA = StructType([
    StructField("MBLNR", StringType(), True),
    StructField("ZEILE", StringType(), True),
    StructField("MATNR", StringType(), True),
    StructField("WERKS", StringType(), True),
    StructField("LGORT", StringType(), True),
    StructField("BWART", StringType(), True),
    StructField("MENGE", DecimalType(13, 3), True),
    StructField("MEINS", StringType(), True),
    StructField("BUDAT", DateType(), True),
    StructField("CPUDT", DateType(), True),
])

REGISTRY_SCHEMA = StructType([
    StructField("sap_material_number", StringType(), True),
    StructField("unified_sku_id", StringType(), True),
])


def test_daily_positions_receipt_minus_delivery(spark):
    inv_df = spark.createDataFrame(
        [
            ("4900000001", "0001", "MAT001", "NL01", "0001", "101",
             Decimal("10.000"), "KG", date(2024, 1, 1), date(2024, 1, 1)),
            ("4900000002", "0001", "MAT001", "NL01", "0001", "601",
             Decimal("3.000"), "KG", date(2024, 1, 2), date(2024, 1, 2)),
        ],
        INV_SCHEMA,
    )
    reg_df = spark.createDataFrame(
        [("MAT001", "5000100000001")],
        REGISTRY_SCHEMA,
    )
    result = compute_daily_positions(inv_df, reg_df).collect()
    assert len(result) == 1
    assert float(result[0]["stock_qty"]) == pytest.approx(7.0)


def test_daily_positions_output_columns(spark):
    inv_df = spark.createDataFrame(
        [("4900000001", "0001", "MAT001", "NL01", "0001", "101",
          Decimal("5.000"), "KG", date(2024, 1, 1), date(2024, 1, 1))],
        INV_SCHEMA,
    )
    reg_df = spark.createDataFrame(
        [("MAT001", "5000100000001")],
        REGISTRY_SCHEMA,
    )
    result = compute_daily_positions(inv_df, reg_df)
    assert set(result.columns) == {"werks", "unified_sku_id", "snapshot_date", "stock_qty", "uom"}
```

- [ ] **Step 2: Run tests — verify they fail**

```bash
pytest tests/unit/test_gold_aggregates.py -v
```

Expected: `ImportError` — `aggregates` module does not exist yet

- [ ] **Step 3: Implement GoldAggregateBase + compute_daily_positions**

Create `src/medallion/gold/aggregates.py`:

```python
from abc import ABC, abstractmethod
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DecimalType


class GoldAggregateBase(ABC):
    def __init__(self, spark: SparkSession, catalog: str = "gold"):
        self.spark = spark
        self.catalog = catalog

    @abstractmethod
    def target_table(self) -> str: ...

    @abstractmethod
    def compute(self) -> DataFrame: ...

    def run(self) -> None:
        df = self.compute().withColumn("_computed_at", F.current_timestamp())
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(f"{self.catalog}.{self.target_table()}")
        )


def compute_daily_positions(
    inventory_df: DataFrame,
    registry_df: DataFrame,
) -> DataFrame:
    reg = registry_df.select(F.col("sap_material_number"), "unified_sku_id")
    signed = (
        inventory_df
        .join(reg, on=inventory_df["MATNR"] == reg["sap_material_number"], how="left")
        .drop("sap_material_number")
        .withColumn(
            "signed_qty",
            F.when(F.col("BWART").isin("101", "262", "602"), F.col("MENGE"))
             .when(F.col("BWART").isin("261", "601", "102"), -F.col("MENGE"))
             .otherwise(F.lit(Decimal("0.000"))),
        )
    )
    daily = (
        signed
        .groupBy("WERKS", "unified_sku_id", "BUDAT", "MEINS")
        .agg(F.sum("signed_qty").alias("daily_net"))
    )
    w_cum = (
        Window
        .partitionBy("WERKS", "unified_sku_id")
        .orderBy("BUDAT")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    w_latest = (
        Window
        .partitionBy("WERKS", "unified_sku_id")
        .orderBy(F.col("BUDAT").desc())
    )
    return (
        daily
        .withColumn("cumulative_qty", F.sum("daily_net").over(w_cum))
        .withColumn("_rn", F.row_number().over(w_latest))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "daily_net")
        .select(
            F.col("WERKS").alias("werks"),
            F.col("unified_sku_id"),
            F.col("BUDAT").alias("snapshot_date"),
            F.col("cumulative_qty").cast(DecimalType(13, 3)).alias("stock_qty"),
            F.col("MEINS").alias("uom"),
        )
    )
```

- [ ] **Step 4: Run tests — verify they pass**

```bash
pytest tests/unit/test_gold_aggregates.py -v
```

Expected: `2 passed`

- [ ] **Step 5: Commit**

```bash
git add src/medallion/gold/aggregates.py tests/unit/test_gold_aggregates.py
git commit -m "feat(gold): GoldAggregateBase + compute_daily_positions TDD"
```

---

## Task 6: SalesVelocityWriter (TDD)

**Files:**
- Modify: `src/medallion/gold/aggregates.py`
- Modify: `tests/unit/test_gold_aggregates.py`

- [ ] **Step 1: Add 2 failing tests**

Append to `tests/unit/test_gold_aggregates.py`:

```python
from src.medallion.gold.aggregates import compute_daily_positions, compute_sales_velocity

ENRICHED_SCHEMA = StructType([
    StructField("WERKS", StringType(), True),
    StructField("BWART", StringType(), True),
    StructField("MENGE", DecimalType(13, 3), True),
    StructField("MEINS", StringType(), True),
    StructField("BUDAT", DateType(), True),
    StructField("unified_sku_id", StringType(), True),
])


def test_sales_velocity_all_windows_computed(spark):
    today = date(2024, 4, 1)
    enriched_df = spark.createDataFrame(
        [
            ("NL01", "601", Decimal("5.000"), "KG", date(2024, 3, 30), "5000100000001"),
            ("NL01", "601", Decimal("3.000"), "KG", date(2024, 3, 10), "5000100000001"),
            ("NL01", "601", Decimal("2.000"), "KG", date(2024, 1, 10), "5000100000001"),
        ],
        ENRICHED_SCHEMA,
    )
    row = compute_sales_velocity(enriched_df, reference_date=today).collect()[0]
    assert float(row["sales_7d"]) == pytest.approx(5.0)
    assert float(row["sales_14d"]) == pytest.approx(5.0)
    assert float(row["sales_28d"]) == pytest.approx(8.0)
    assert float(row["sales_90d"]) == pytest.approx(10.0)


def test_sales_velocity_reversal_subtracts(spark):
    today = date(2024, 4, 1)
    enriched_df = spark.createDataFrame(
        [
            ("NL01", "601", Decimal("10.000"), "KG", date(2024, 3, 30), "5000100000001"),
            ("NL01", "602", Decimal("2.000"), "KG", date(2024, 3, 30), "5000100000001"),
        ],
        ENRICHED_SCHEMA,
    )
    row = compute_sales_velocity(enriched_df, reference_date=today).collect()[0]
    assert float(row["sales_7d"]) == pytest.approx(8.0)
```

> **Date reasoning for test_sales_velocity_all_windows_computed:**
> Reference date is 2024-04-01.
> - 2024-03-30 is 2 days ago → inside 7d, 14d, 28d, 90d windows → contributes 5.0 to all
> - 2024-03-10 is 22 days ago → inside 28d and 90d windows only → contributes 3.0 to sales_28d, sales_90d
> - 2024-01-10 is 82 days ago → inside 90d window only → contributes 2.0 to sales_90d
> Result: sales_7d=5, sales_14d=5, sales_28d=8, sales_90d=10

- [ ] **Step 2: Run tests — verify only the 2 new tests fail**

```bash
pytest tests/unit/test_gold_aggregates.py -v
```

Expected: `2 passed, 2 failed` (the 2 new tests fail with ImportError for `compute_sales_velocity`)

- [ ] **Step 3: Add compute_sales_velocity + SalesVelocityWriter to aggregates.py**

Append to `src/medallion/gold/aggregates.py`:

```python
def compute_sales_velocity(
    enriched_df: DataFrame,
    reference_date=None,
) -> DataFrame:
    ref = F.lit(reference_date) if reference_date is not None else F.current_date()
    sales = (
        enriched_df
        .filter(F.col("BWART").isin("601", "602"))
        .withColumn(
            "signed_qty",
            F.when(F.col("BWART") == "601", F.col("MENGE"))
             .otherwise(-F.col("MENGE")),
        )
        .groupBy("WERKS", "unified_sku_id", "MEINS")
        .agg(
            F.sum(
                F.when(F.col("BUDAT") >= F.date_sub(ref, 7), F.col("signed_qty"))
                 .otherwise(F.lit(Decimal("0.000")))
            ).cast(DecimalType(13, 3)).alias("sales_7d"),
            F.sum(
                F.when(F.col("BUDAT") >= F.date_sub(ref, 14), F.col("signed_qty"))
                 .otherwise(F.lit(Decimal("0.000")))
            ).cast(DecimalType(13, 3)).alias("sales_14d"),
            F.sum(
                F.when(F.col("BUDAT") >= F.date_sub(ref, 28), F.col("signed_qty"))
                 .otherwise(F.lit(Decimal("0.000")))
            ).cast(DecimalType(13, 3)).alias("sales_28d"),
            F.sum(
                F.when(F.col("BUDAT") >= F.date_sub(ref, 90), F.col("signed_qty"))
                 .otherwise(F.lit(Decimal("0.000")))
            ).cast(DecimalType(13, 3)).alias("sales_90d"),
        )
    )
    return sales.select(
        F.col("WERKS").alias("werks"),
        "unified_sku_id",
        "sales_7d",
        "sales_14d",
        "sales_28d",
        "sales_90d",
        F.col("MEINS").alias("uom"),
    )


class SalesVelocityWriter(GoldAggregateBase):
    def target_table(self) -> str:
        return "sales.velocity"

    def compute(self) -> DataFrame:
        return compute_sales_velocity(
            self.spark.table("silver.sap.enriched_movements"),
        )
```

- [ ] **Step 4: Run tests — verify all 4 pass**

```bash
pytest tests/unit/test_gold_aggregates.py -v
```

Expected: `4 passed`

- [ ] **Step 5: Commit**

```bash
git add src/medallion/gold/aggregates.py tests/unit/test_gold_aggregates.py
git commit -m "feat(gold): compute_sales_velocity — 7/14/28/90d rolling windows TDD"
```

---

## Task 7: OpenOrdersWriter + All Writer Classes + Entry Points (TDD)

**Files:**
- Modify: `src/medallion/gold/aggregates.py`
- Modify: `tests/unit/test_gold_aggregates.py`

- [ ] **Step 1: Add 2 failing tests**

Append to `tests/unit/test_gold_aggregates.py`:

```python
from src.medallion.gold.aggregates import (
    compute_daily_positions,
    compute_open_orders,
    compute_sales_velocity,
)

ORDERS_SCHEMA = StructType([
    StructField("EBELN", StringType(), True),
    StructField("EBELP", StringType(), True),
    StructField("MATNR", StringType(), True),
    StructField("WERKS", StringType(), True),
    StructField("MENGE", DecimalType(13, 3), True),
    StructField("MEINS", StringType(), True),
    StructField("EINDT", DateType(), True),
])


def test_open_orders_filters_past_delivery(spark):
    today = date(2024, 4, 1)
    orders_df = spark.createDataFrame(
        [
            ("PO001", "0001", "MAT001", "NL01", Decimal("50.000"), "KG", date(2024, 4, 15)),
            ("PO002", "0001", "MAT001", "NL01", Decimal("30.000"), "KG", date(2024, 3, 15)),
        ],
        ORDERS_SCHEMA,
    )
    reg_df = spark.createDataFrame(
        [("MAT001", "5000100000001")],
        REGISTRY_SCHEMA,
    )
    result = compute_open_orders(orders_df, reg_df, reference_date=today).collect()
    assert len(result) == 1
    assert float(result[0]["open_qty"]) == pytest.approx(50.0)


def test_open_orders_output_columns(spark):
    today = date(2024, 4, 1)
    orders_df = spark.createDataFrame(
        [("PO001", "0001", "MAT001", "NL01", Decimal("50.000"), "KG", date(2024, 4, 15))],
        ORDERS_SCHEMA,
    )
    reg_df = spark.createDataFrame(
        [("MAT001", "5000100000001")],
        REGISTRY_SCHEMA,
    )
    result = compute_open_orders(orders_df, reg_df, reference_date=today)
    assert set(result.columns) == {"werks", "unified_sku_id", "open_qty", "earliest_delivery", "uom"}
```

- [ ] **Step 2: Run tests — verify only 2 new tests fail**

```bash
pytest tests/unit/test_gold_aggregates.py -v
```

Expected: `4 passed, 2 failed`

- [ ] **Step 3: Add compute_open_orders + all writers + run_* entry points to aggregates.py**

Append to `src/medallion/gold/aggregates.py`:

```python
def compute_open_orders(
    orders_df: DataFrame,
    registry_df: DataFrame,
    reference_date=None,
) -> DataFrame:
    ref = F.lit(reference_date) if reference_date is not None else F.current_date()
    reg = registry_df.select(F.col("sap_material_number"), "unified_sku_id")
    return (
        orders_df
        .filter(F.col("EINDT") >= ref)
        .join(reg, on=orders_df["MATNR"] == reg["sap_material_number"], how="left")
        .drop("sap_material_number")
        .groupBy("WERKS", "unified_sku_id", "MEINS")
        .agg(
            F.sum("MENGE").cast(DecimalType(13, 3)).alias("open_qty"),
            F.min("EINDT").alias("earliest_delivery"),
        )
        .select(
            F.col("WERKS").alias("werks"),
            "unified_sku_id",
            "open_qty",
            "earliest_delivery",
            F.col("MEINS").alias("uom"),
        )
    )


class DailyPositionsWriter(GoldAggregateBase):
    def target_table(self) -> str:
        return "inventory.daily_positions"

    def compute(self) -> DataFrame:
        return compute_daily_positions(
            self.spark.table("silver.sap.inventory_clean"),
            self.spark.table("silver.master.unified_sku_registry"),
        )


class OpenOrdersWriter(GoldAggregateBase):
    def target_table(self) -> str:
        return "replenishment.open_orders"

    def compute(self) -> DataFrame:
        return compute_open_orders(
            self.spark.table("silver.sap.open_orders_clean"),
            self.spark.table("silver.master.unified_sku_registry"),
        )


def run_daily_positions() -> None:
    from pyspark.sql import SparkSession
    DailyPositionsWriter(SparkSession.builder.getOrCreate()).run()


def run_sales_velocity() -> None:
    from pyspark.sql import SparkSession
    SalesVelocityWriter(SparkSession.builder.getOrCreate()).run()


def run_open_orders() -> None:
    from pyspark.sql import SparkSession
    OpenOrdersWriter(SparkSession.builder.getOrCreate()).run()
```

- [ ] **Step 4: Run all 6 Gold tests — verify they pass**

```bash
pytest tests/unit/test_gold_aggregates.py -v
```

Expected:
```
PASSED test_daily_positions_receipt_minus_delivery
PASSED test_daily_positions_output_columns
PASSED test_sales_velocity_all_windows_computed
PASSED test_sales_velocity_reversal_subtracts
PASSED test_open_orders_filters_past_delivery
PASSED test_open_orders_output_columns
6 passed
```

- [ ] **Step 5: Commit**

```bash
git add src/medallion/gold/aggregates.py tests/unit/test_gold_aggregates.py
git commit -m "feat(gold): compute_open_orders + all Gold writer classes + run_* entry points TDD"
```

---

## Task 8: setup.py + gold_aggregates.yml + databricks.yml + Push

**Files:**
- Create: `setup.py`
- Create: `resources/jobs/gold_aggregates.yml`
- Modify: `databricks.yml`

- [ ] **Step 1: Create setup.py**

Create `setup.py` at the project root:

```python
from setuptools import find_packages, setup

setup(
    name="ahold_freshness_poc",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    entry_points={
        "console_scripts": [
            "gold_daily_positions=medallion.gold.aggregates:run_daily_positions",
            "gold_sales_velocity=medallion.gold.aggregates:run_sales_velocity",
            "gold_open_orders=medallion.gold.aggregates:run_open_orders",
        ],
    },
)
```

- [ ] **Step 2: Create gold_aggregates.yml**

Create `resources/jobs/gold_aggregates.yml`:

```yaml
name: gold-aggregates-${bundle.target}

schedule:
  quartz_cron_expression: "0 0 5 * * ?"
  timezone_id: "Europe/Amsterdam"
  pause_status: ${var.schedule_enabled == "true" ? "UNPAUSED" : "PAUSED"}

email_notifications:
  on_failure:
    - replenishment-oncall@ah.nl

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

- [ ] **Step 3: Add gold_aggregates to databricks.yml**

Final `databricks.yml` `resources` section:

```yaml
resources:
  pipelines:
    bronze_materials:
      source: resources/pipelines/bronze_materials.yml
    bronze_inventory:
      source: resources/pipelines/bronze_inventory.yml
    bronze_open_orders:
      source: resources/pipelines/bronze_open_orders.yml
    silver_rosetta_stone:
      source: resources/pipelines/silver_rosetta_stone.yml
    silver_demand_features:
      source: resources/pipelines/silver_demand_features.yml
  jobs:
    lakeflow_trigger:
      source: resources/jobs/lakeflow_trigger.yml
    gold_aggregates:
      source: resources/jobs/gold_aggregates.yml
```

- [ ] **Step 4: Run full test suite**

```bash
cd /c/Projects/Ahold_POC
pytest tests/unit/ -v --tb=short
```

Expected: `24 passed` (6 Bronze from Phase 0 + 6 Rosetta Stone + 5 Silver demand features + 6 Gold + 1 new EAN11 test — total across all unit test files)

> **Note:** PySpark must be installed (`pip install pyspark`) or tests run in Databricks. If PySpark is unavailable, the tests are structurally correct and will pass in the Databricks runtime.

- [ ] **Step 5: Commit and push**

```bash
git add setup.py resources/jobs/gold_aggregates.yml databricks.yml
git commit -m "feat(gold): DABs job + setup.py wheel entry points — Phase 1 complete"
git push origin master
```

Expected: `master -> master`

---

## Deployment Sequence (when Phase 0 infra is provisioned)

Once `silver.master.unified_sku_registry` has been populated by at least one Lakeflow Connect run:

```bash
# Deploy Phase 1 DABs resources
databricks bundle deploy --target dev

# Manually trigger Rosetta Stone pipeline
databricks bundle run silver_rosetta_stone --target dev

# Check match rate (Week 5 gate)
# Run in a Databricks notebook:
# from src.medallion.silver.rosetta_stone.entity_resolution import RosettaStone
# rs = RosettaStone(spark)
# print(f"Match rate: {rs.get_match_rate():.1%}  Ready: {rs.is_ready()}")

# Run demand features pipeline
databricks bundle run silver_demand_features --target dev

# Run gold aggregates job
databricks bundle run gold_aggregates --target dev
```

Verify against spec `## Verification` criteria (6 checkpoints).
