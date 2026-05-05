import pytest
from datetime import date
from decimal import Decimal

from pyspark.sql.types import (
    DateType, DecimalType, DoubleType, IntegerType, StringType,
    StructField, StructType,
)

from src.medallion.silver.demand_features.transformations import (
    build_enriched_movements,
    clean_inventory,
    clean_materials,
    clean_open_orders,
)

# --- Schemas ---

MATERIALS_SCHEMA = StructType([
    StructField("MATNR", StringType(), True),
    StructField("LAEDA", DateType(), True),
    StructField("MTART", StringType(), True),
    StructField("MHDRZ", DecimalType(5, 0), True),
    StructField("MHDLP", DecimalType(5, 0), True),
    StructField("MAKTX", StringType(), True),
])

INVENTORY_SCHEMA = StructType([
    StructField("MBLNR", StringType(), True),
    StructField("ZEILE", StringType(), True),
    StructField("BWART", StringType(), True),
    StructField("BUDAT", StringType(), True),
    StructField("CPUDT", StringType(), True),
    StructField("MATNR", StringType(), True),
    StructField("MENGE", DecimalType(13, 3), True),
])

OPEN_ORDERS_SCHEMA = StructType([
    StructField("EBELN", StringType(), True),
    StructField("EBELP", StringType(), True),
    StructField("MENGE", DecimalType(13, 3), True),
    StructField("EINDT", StringType(), True),
    StructField("BEDAT", StringType(), True),
])

SKU_REGISTRY_SCHEMA = StructType([
    StructField("sap_material_number", StringType(), True),
    StructField("unified_sku_id", StringType(), True),
    StructField("match_confidence", DoubleType(), True),
])


def test_clean_materials_filters_invalid_mtart(spark):
    df = spark.createDataFrame(
        [
            ("MAT001", date(2024, 1, 1), "FERT", Decimal("7"), Decimal("3"), "Apple"),
            ("MAT002", date(2024, 1, 1), "VERP", Decimal("0"), Decimal("0"), "Packaging"),
        ],
        MATERIALS_SCHEMA,
    )
    result = clean_materials(df)
    rows = result.select("MATNR").collect()
    assert len(rows) == 1
    assert rows[0]["MATNR"] == "MAT001"


def test_clean_materials_dedup_keeps_latest_laeda(spark):
    df = spark.createDataFrame(
        [
            ("MAT001", date(2023, 1, 1), "FERT", Decimal("7"), Decimal("3"), "Old"),
            ("MAT001", date(2024, 6, 1), "FERT", Decimal("7"), Decimal("3"), "New"),
        ],
        MATERIALS_SCHEMA,
    )
    result = clean_materials(df)
    rows = result.collect()
    assert len(rows) == 1
    assert rows[0]["MAKTX"] == "New"


def test_clean_inventory_drops_invalid_bwart(spark):
    df = spark.createDataFrame(
        [
            ("DOC001", "001", "101", "2024-01-01", "2024-01-01", "MAT001", Decimal("10")),
            ("DOC002", "001", "999", "2024-01-01", "2024-01-01", "MAT001", Decimal("5")),
        ],
        INVENTORY_SCHEMA,
    )
    result = clean_inventory(df)
    rows = result.select("MBLNR").collect()
    assert len(rows) == 1
    assert rows[0]["MBLNR"] == "DOC001"


def test_clean_open_orders_drops_zero_menge(spark):
    df = spark.createDataFrame(
        [
            ("PO001", "001", Decimal("100"), "2024-06-01", "2024-05-01"),
            ("PO002", "001", Decimal("0"), "2024-06-01", "2024-05-01"),
        ],
        OPEN_ORDERS_SCHEMA,
    )
    result = clean_open_orders(df)
    rows = result.select("EBELN").collect()
    assert len(rows) == 1
    assert rows[0]["EBELN"] == "PO001"


def test_enriched_movements_has_unified_sku_id(spark):
    inv = spark.createDataFrame(
        [("DOC001", "001", "601", "2024-01-01", "2024-01-01", "MAT001", Decimal("10"))],
        INVENTORY_SCHEMA,
    )
    mat = spark.createDataFrame(
        [("MAT001", date(2024, 1, 1), "FERT", Decimal("7"), Decimal("3"), "Apple")],
        MATERIALS_SCHEMA,
    )
    sku = spark.createDataFrame(
        [("MAT001", "5000100000001", 1.0)],
        SKU_REGISTRY_SCHEMA,
    )
    result = build_enriched_movements(clean_inventory(inv), clean_materials(mat), sku)
    row = result.collect()[0]
    assert row["unified_sku_id"] == "5000100000001"
