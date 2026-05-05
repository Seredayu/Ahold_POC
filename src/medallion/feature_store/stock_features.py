from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from medallion.feature_store.base import FeatureStoreBase


def compute_stock_features(positions_df: DataFrame, velocity_df: DataFrame) -> DataFrame:
    """
    Join daily_positions to sales_velocity, derive days_of_cover.
    days_of_cover = stock_qty / (sales_7d / 7), NULL when sales_7d = 0.
    """
    vel = velocity_df.select("werks", "unified_sku_id", "sales_7d")
    return (
        positions_df
        .join(vel, on=["werks", "unified_sku_id"], how="left")
        .withColumn(
            "days_of_cover",
            F.when(F.col("sales_7d") == 0, F.lit(None).cast("decimal(8,2)"))
             .otherwise((F.col("stock_qty") / (F.col("sales_7d") / 7)).cast("decimal(8,2)")),
        )
        .select(
            F.col("werks"),
            F.col("unified_sku_id"),
            F.col("stock_qty").cast("decimal(13,3)"),
            F.col("days_of_cover"),
            F.col("uom"),
        )
    )


class StockFeaturesWriter(FeatureStoreBase):
    def feature_table_name(self) -> str:
        return "stock.sku_site_positions"

    def primary_keys(self) -> list[str]:
        return ["werks", "unified_sku_id"]

    def compute(self) -> DataFrame:
        return compute_stock_features(
            self.spark.table("gold.inventory.daily_positions"),
            self.spark.table("gold.sales.velocity"),
        )


def _entry_stock_features() -> None:
    from pyspark.sql import SparkSession
    StockFeaturesWriter(SparkSession.getActiveSession()).run()
