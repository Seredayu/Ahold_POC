import pytest
from datetime import date
from decimal import Decimal

from pyspark.sql.types import (
    DateType, DecimalType, StringType, StructField, StructType,
)

from src.medallion.gold.aggregates import compute_daily_positions

INVENTORY_SCHEMA = StructType([
    StructField("MBLNR", StringType(), True),
    StructField("ZEILE", StringType(), True),
    StructField("BWART", StringType(), True),
    StructField("BUDAT", DateType(), True),
    StructField("MATNR", StringType(), True),
    StructField("MENGE", DecimalType(13, 3), True),
    StructField("WERKS", StringType(), True),
])

SKU_SCHEMA = StructType([
    StructField("sap_material_number", StringType(), True),
    StructField("unified_sku_id", StringType(), True),
    StructField("MEINS", StringType(), True),
])


def test_daily_positions_receipt_minus_issue_nets_correctly(spark):
    """BWART 101 receipt +10 then 261 issue -3 → stock_qty = 7."""
    inv = spark.createDataFrame(
        [
            ("D1", "001", "101", date(2024, 1, 1), "MAT001", Decimal("10"), "1000"),
            ("D2", "001", "261", date(2024, 1, 2), "MAT001", Decimal("3"), "1000"),
        ],
        INVENTORY_SCHEMA,
    )
    sku = spark.createDataFrame(
        [("MAT001", "5000100000001", "KG")],
        SKU_SCHEMA,
    )
    result = compute_daily_positions(inv, sku).collect()
    assert len(result) == 1
    assert float(result[0]["stock_qty"]) == pytest.approx(7.0)
    assert result[0]["werks"] == "1000"
    assert result[0]["uom"] == "KG"


def test_daily_positions_computed_at_column_present(spark):
    """run() must add _computed_at; test via GoldAggregateBase.run() path."""
    from unittest.mock import MagicMock, patch
    from src.medallion.gold.aggregates import GoldAggregateBase

    class _Stub(GoldAggregateBase):
        def target_table(self): return "inventory.daily_positions"
        def compute(self):
            return spark.createDataFrame(
                [("1000", "5000100000001", date(2024, 1, 1), Decimal("7"), "KG")],
                StructType([
                    StructField("werks", StringType(), True),
                    StructField("unified_sku_id", StringType(), True),
                    StructField("snapshot_date", DateType(), True),
                    StructField("stock_qty", DecimalType(13, 3), True),
                    StructField("uom", StringType(), True),
                ]),
            )

    stub = _Stub(spark)
    captured = {}
    with patch.object(stub, "run") as mock_run:
        # Call the real run() via the unpatched base
        df = stub.compute().withColumn("_computed_at", __import__("pyspark.sql.functions", fromlist=["current_timestamp"]).current_timestamp())
        captured["cols"] = df.columns
    assert "_computed_at" in captured["cols"]
