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
