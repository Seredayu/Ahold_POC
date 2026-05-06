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


def test_days_of_cover_computed_correctly(spark):
    from src.medallion.feature_store.stock_features import compute_stock_features

    # stock_qty=49, sales_7d=7 → days_of_cover = 49 / (7/7) = 49.00
    positions = spark.createDataFrame(
        [("1000", "5000100000001", date(2024, 1, 1), Decimal("49"), "KG")],
        POSITIONS_SCHEMA,
    )
    velocity = spark.createDataFrame(
        [("1000", "5000100000001", Decimal("7"), Decimal("14"),
          Decimal("28"), Decimal("90"), "KG")],
        VELOCITY_SCHEMA,
    )
    result = compute_stock_features(positions, velocity).collect()
    assert result[0]["days_of_cover"] == Decimal("49.00")
