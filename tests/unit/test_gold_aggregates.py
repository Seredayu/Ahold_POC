import pytest
from datetime import date
from decimal import Decimal

from pyspark.sql.types import (
    DateType, DecimalType, StringType, StructField, StructType,
)

from src.medallion.gold.aggregates import compute_daily_positions, compute_open_orders, compute_sales_velocity

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

OPEN_ORDERS_SCHEMA = StructType([
    StructField("EBELN", StringType(), True),
    StructField("EBELP", StringType(), True),
    StructField("MATNR", StringType(), True),
    StructField("WERKS", StringType(), True),
    StructField("MENGE", DecimalType(13, 3), True),
    StructField("EINDT", DateType(), True),
])

ENRICHED_SCHEMA = StructType([
    StructField("WERKS", StringType(), True),
    StructField("unified_sku_id", StringType(), True),
    StructField("BWART", StringType(), True),
    StructField("BUDAT", DateType(), True),
    StructField("MENGE", DecimalType(13, 3), True),
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
    """GoldAggregateBase.run() adds _computed_at to the DataFrame before writing."""
    from unittest.mock import MagicMock, patch, PropertyMock
    import pyspark.sql
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

    captured = {}

    def intercept_write(self):
        captured["cols"] = self.columns
        mock_writer = MagicMock()
        mock_writer.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.saveAsTable.return_value = None
        return mock_writer

    with patch.object(pyspark.sql.DataFrame, "write", new_callable=PropertyMock, side_effect=intercept_write):
        _Stub(spark).run()

    assert "_computed_at" in captured["cols"]


def test_sales_velocity_windows_all_computed(spark):
    """sales_7d, sales_14d, sales_28d, sales_90d are all present and sales_28d includes the row."""
    from datetime import date as _date
    ref = _date(2024, 2, 1)  # reference date
    enriched = spark.createDataFrame(
        [
            ("1000", "5000100000001", "601", _date(2024, 1, 10), Decimal("5"), "KG"),  # 22 days before ref → in 28d, 90d
            ("1000", "5000100000001", "601", _date(2024, 1, 29), Decimal("3"), "KG"),  # 3 days before ref → in 7d, 14d, 28d, 90d
        ],
        ENRICHED_SCHEMA,
    )
    result = compute_sales_velocity(enriched, reference_date=ref).collect()
    assert len(result) == 1
    row = result[0]
    assert float(row["sales_7d"]) == pytest.approx(3.0)
    assert float(row["sales_14d"]) == pytest.approx(3.0)
    assert float(row["sales_28d"]) == pytest.approx(8.0)
    assert float(row["sales_90d"]) == pytest.approx(8.0)


def test_sales_velocity_reversal_subtracts(spark):
    """BWART 602 reversal subtracts from the window sum."""
    from datetime import date as _date
    ref = _date(2024, 2, 1)
    enriched = spark.createDataFrame(
        [
            ("1000", "5000100000001", "601", _date(2024, 1, 29), Decimal("10"), "KG"),
            ("1000", "5000100000001", "602", _date(2024, 1, 30), Decimal("2"), "KG"),
        ],
        ENRICHED_SCHEMA,
    )
    result = compute_sales_velocity(enriched, reference_date=ref).collect()
    assert float(result[0]["sales_7d"]) == pytest.approx(8.0)


def test_open_orders_past_delivery_filtered_out(spark):
    """Orders with EINDT in the past are excluded; only future deliveries count."""
    from datetime import date as _date
    from unittest.mock import patch
    import pyspark.sql.functions as psf

    future_date = _date(2099, 12, 31)
    past_date = _date(2020, 1, 1)

    orders = spark.createDataFrame(
        [
            ("PO001", "001", "MAT001", "1000", Decimal("50"), future_date),
            ("PO002", "001", "MAT001", "1000", Decimal("20"), past_date),
        ],
        OPEN_ORDERS_SCHEMA,
    )
    sku = spark.createDataFrame(
        [("MAT001", "5000100000001", "KG")],
        SKU_SCHEMA,
    )
    result = compute_open_orders(orders, sku).collect()
    assert len(result) == 1
    assert float(result[0]["open_qty"]) == pytest.approx(50.0)


def test_open_orders_computed_at_column_present(spark):
    """OpenOrdersWriter.run() adds _computed_at before writing."""
    from unittest.mock import MagicMock, patch, PropertyMock
    import pyspark.sql
    from src.medallion.gold.aggregates import GoldAggregateBase

    class _Stub(GoldAggregateBase):
        def target_table(self): return "replenishment.open_orders"
        def compute(self):
            return spark.createDataFrame(
                [("1000", "5000100000001", Decimal("50"), date(2099, 12, 31), "KG")],
                StructType([
                    StructField("werks", StringType(), True),
                    StructField("unified_sku_id", StringType(), True),
                    StructField("open_qty", DecimalType(13, 3), True),
                    StructField("earliest_delivery", DateType(), True),
                    StructField("uom", StringType(), True),
                ]),
            )

    captured = {}

    def intercept_write(self):
        captured["cols"] = self.columns
        mock_writer = MagicMock()
        mock_writer.format.return_value = mock_writer
        mock_writer.mode.return_value = mock_writer
        mock_writer.saveAsTable.return_value = None
        return mock_writer

    with patch.object(pyspark.sql.DataFrame, "write", new_callable=PropertyMock, side_effect=intercept_write):
        _Stub(spark).run()

    assert "_computed_at" in captured["cols"]
