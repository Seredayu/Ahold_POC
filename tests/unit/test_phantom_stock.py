import pytest
from datetime import date
from decimal import Decimal

from pyspark.sql.types import (
    DateType, DecimalType, DoubleType, IntegerType,
    StringType, StructField, StructType,
)

INVENTORY_SCHEMA = StructType([
    StructField("werks", StringType(), True),
    StructField("unified_sku_id", StringType(), True),
    StructField("snapshot_date", DateType(), True),
    StructField("stock_qty", DecimalType(13, 3), True),
    StructField("uom", StringType(), True),
])

VELOCITY_SCHEMA = StructType([
    StructField("werks", StringType(), True),
    StructField("unified_sku_id", StringType(), True),
    StructField("sales_7d", DecimalType(13, 3), True),
    StructField("sales_14d", DecimalType(13, 3), True),
    StructField("sales_28d", DecimalType(13, 3), True),
    StructField("sales_90d", DecimalType(13, 3), True),
    StructField("uom", StringType(), True),
])

# All 8 XGBoost feature columns + identity columns + shelf_life context
FEATURES_SCHEMA = StructType([
    StructField("werks", StringType(), True),
    StructField("unified_sku_id", StringType(), True),
    StructField("sales_7d", DoubleType(), True),
    StructField("sales_14d", DoubleType(), True),
    StructField("sales_28d", DoubleType(), True),
    StructField("sales_90d", DoubleType(), True),
    StructField("stock_qty", DoubleType(), True),
    StructField("days_of_cover", DoubleType(), True),
    StructField("velocity_collapse_ratio", DoubleType(), True),
    StructField("days_since_last_sale", IntegerType(), True),
    StructField("shelf_life_days", IntegerType(), True),
])


def test_generate_labels_labels_phantom(spark):
    from engines.phantom_stock.label_generator import generate_labels

    # stock_qty > 0, sales_7d = 0, sales_28d > 5 → phantom
    inventory = spark.createDataFrame(
        [("1000", "SKU001", date(2024, 1, 1), Decimal("10.000"), "KG")],
        INVENTORY_SCHEMA,
    )
    velocity = spark.createDataFrame(
        [("1000", "SKU001", Decimal("0.000"), Decimal("0.000"),
          Decimal("20.000"), Decimal("60.000"), "KG")],
        VELOCITY_SCHEMA,
    )
    result = generate_labels(inventory, velocity).collect()
    assert len(result) == 1
    assert result[0]["is_phantom"] == 1


def test_generate_labels_ignores_zero_stock(spark):
    from engines.phantom_stock.label_generator import generate_labels

    # stock_qty = 0 → not phantom even if sales also stopped
    inventory = spark.createDataFrame(
        [("1000", "SKU002", date(2024, 1, 1), Decimal("0.000"), "KG")],
        INVENTORY_SCHEMA,
    )
    velocity = spark.createDataFrame(
        [("1000", "SKU002", Decimal("0.000"), Decimal("0.000"),
          Decimal("20.000"), Decimal("60.000"), "KG")],
        VELOCITY_SCHEMA,
    )
    result = generate_labels(inventory, velocity).collect()
    assert result[0]["is_phantom"] == 0


def test_generate_labels_ignores_slow_movers(spark):
    from engines.phantom_stock.label_generator import generate_labels

    # sales_28d ≤ 5 → slow-mover guard, not phantom
    inventory = spark.createDataFrame(
        [("1000", "SKU003", date(2024, 1, 1), Decimal("10.000"), "KG")],
        INVENTORY_SCHEMA,
    )
    velocity = spark.createDataFrame(
        [("1000", "SKU003", Decimal("0.000"), Decimal("0.000"),
          Decimal("3.000"), Decimal("5.000"), "KG")],
        VELOCITY_SCHEMA,
    )
    result = generate_labels(inventory, velocity).collect()
    assert result[0]["is_phantom"] == 0
