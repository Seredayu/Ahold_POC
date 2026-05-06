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


def test_generate_labels_null_stock_is_not_phantom(spark):
    from engines.phantom_stock.label_generator import generate_labels

    # NULL stock_qty → Spark three-valued logic yields NULL > 0 = NULL → not phantom
    inventory = spark.createDataFrame(
        [("1000", "SKU004", date(2024, 1, 1), None, "KG")],
        INVENTORY_SCHEMA,
    )
    velocity = spark.createDataFrame(
        [("1000", "SKU004", Decimal("0.000"), Decimal("0.000"),
          Decimal("20.000"), Decimal("60.000"), "KG")],
        VELOCITY_SCHEMA,
    )
    result = generate_labels(inventory, velocity).collect()
    assert result[0]["is_phantom"] == 0


def test_predict_auto_corrected_for_high_score(spark):
    import numpy as np
    from unittest.mock import MagicMock
    from engines.phantom_stock.classifier import PhantomStockClassifier

    clf = PhantomStockClassifier()
    mock_model = MagicMock()
    mock_model.predict_proba.return_value = np.array([[0.03, 0.97]])
    clf._model = mock_model

    # Row with all 8 feature columns + identity + shelf_life context
    row = ("1000", "SKU001", 0.0, 0.0, 20.0, 60.0, 50.0, None, 0.0, 14, 7)
    features_df = spark.createDataFrame([row], FEATURES_SCHEMA)

    result = clf.predict(features_df).collect()
    assert result[0]["action"] == "AUTO_CORRECTED"
    assert result[0]["phantom_score"] == pytest.approx(0.97, abs=1e-6)
    assert result[0]["is_phantom"] is True


def test_predict_pending_review_for_mid_score(spark):
    import numpy as np
    from unittest.mock import MagicMock
    from engines.phantom_stock.classifier import PhantomStockClassifier

    clf = PhantomStockClassifier()
    mock_model = MagicMock()
    mock_model.predict_proba.return_value = np.array([[0.12, 0.88]])
    clf._model = mock_model

    row = ("1000", "SKU002", 0.0, 0.0, 10.0, 40.0, 50.0, None, 0.0, 21, 5)
    features_df = spark.createDataFrame([row], FEATURES_SCHEMA)

    result = clf.predict(features_df).collect()
    assert result[0]["action"] == "PENDING_REVIEW"
    assert result[0]["is_phantom"] is True


def test_bapi_error_raised_on_http_500():
    from unittest.mock import patch, MagicMock
    from engines.phantom_stock.bapi_client import BAPIClient, BAPIError

    mock_response = MagicMock()
    mock_response.ok = False
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error"

    # Patch requests.post AND time.sleep to skip real delays
    with patch("engines.phantom_stock.bapi_client.requests.post", return_value=mock_response), \
         patch("engines.phantom_stock.bapi_client.time.sleep"):
        client = BAPIClient("https://fake-btp-endpoint", "fake-token")
        with pytest.raises(BAPIError) as exc_info:
            client.post_goods_movement("1000", "SKU001")

    assert "500" in str(exc_info.value)
