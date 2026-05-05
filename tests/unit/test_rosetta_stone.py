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
