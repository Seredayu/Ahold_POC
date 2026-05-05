import pytest
from pyspark.sql.types import FloatType, StringType, StructField, StructType

from src.medallion.bronze.autoloader import AutoLoaderBase


class _ConcreteLoader(AutoLoaderBase):
    def target_schema(self) -> StructType:
        return StructType([
            StructField("id", StringType(), True),
            StructField("value", FloatType(), True),
        ])


def test_target_table_name_formed_correctly(spark):
    loader = _ConcreteLoader(
        spark=spark,
        source_path="/tmp/src",
        checkpoint_path="/tmp/ckpt",
        catalog="bronze",
        schema="sap",
        table="materials",
    )
    assert loader.target_table == "bronze.sap.materials"


def test_add_audit_columns_adds_all_three(spark):
    loader = _ConcreteLoader(
        spark=spark,
        source_path="/tmp/src",
        checkpoint_path="/tmp/ckpt",
        catalog="bronze",
        schema="sap",
        table="materials",
    )
    df = spark.createDataFrame([("a", 1.0)], ["id", "value"])
    result = loader.add_audit_columns(df)
    assert "_ingested_at" in result.columns
    assert "_source_file" in result.columns
    assert "_cdc_operation" in result.columns


def test_add_audit_columns_ingested_at_is_not_null(spark):
    loader = _ConcreteLoader(
        spark=spark,
        source_path="/tmp/src",
        checkpoint_path="/tmp/ckpt",
        catalog="bronze",
        schema="sap",
        table="materials",
    )
    df = spark.createDataFrame([("a", 1.0)], ["id", "value"])
    result = loader.add_audit_columns(df)
    row = result.collect()[0]
    assert row["_ingested_at"] is not None


def test_target_schema_must_be_implemented(spark):
    with pytest.raises(TypeError):
        AutoLoaderBase(
            spark=spark,
            source_path="/tmp/src",
            checkpoint_path="/tmp/ckpt",
            catalog="bronze",
            schema="sap",
            table="test",
        )
