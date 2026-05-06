from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from medallion.feature_store.base import FeatureStoreBase


def compute_collapse_signals(
    velocity_df: DataFrame,   # gold.sales.velocity — has werks, unified_sku_id, sales_7d, sales_28d
    movements_df: DataFrame,  # silver.sap.enriched_movements — has WERKS, unified_sku_id, BWART, BUDAT
    registry_df: DataFrame,   # silver.master.unified_sku_registry — has unified_sku_id, shelf_life_days
) -> DataFrame:
    """
    Compute collapse signals for phantom-stock detection.

    Features produced:
      - velocity_collapse_ratio : sales_7d / sales_28d (NULL when sales_28d = 0)
      - days_since_last_sale    : datediff(current_date, max(BUDAT)) for BWART='601'
      - shelf_life_days         : from unified_sku_registry, cast to int
    """
    # --- 1. velocity_collapse_ratio from gold.sales.velocity ---
    vel = velocity_df.select(
        F.col("werks"),
        F.col("unified_sku_id"),
        F.col("sales_7d").cast("double"),
        F.col("sales_28d").cast("double"),
    ).withColumn(
        "velocity_collapse_ratio",
        F.when(F.col("sales_28d") == 0, F.lit(None).cast("double"))
         .otherwise(F.col("sales_7d") / F.col("sales_28d")),
    )

    # --- 2. days_since_last_sale from silver.sap.enriched_movements (BWART='601') ---
    last_sale = (
        movements_df
        .filter(F.col("BWART") == "601")
        .groupBy(
            F.col("WERKS").alias("werks"),
            F.col("unified_sku_id"),
        )
        .agg(F.max("BUDAT").alias("last_sale_date"))
        .withColumn(
            "days_since_last_sale",
            F.datediff(F.current_date(), F.col("last_sale_date")).cast("int"),
        )
        .select("werks", "unified_sku_id", "days_since_last_sale")
    )

    # --- 3. shelf_life_days from silver.master.unified_sku_registry ---
    shelf = registry_df.select(
        F.col("unified_sku_id"),
        F.col("shelf_life_days").cast("int"),
    )

    # --- 4. Join everything together ---
    result = (
        vel
        .join(last_sale, on=["werks", "unified_sku_id"], how="left")
        .join(shelf, on="unified_sku_id", how="left")
        .select(
            F.col("werks"),
            F.col("unified_sku_id"),
            F.col("velocity_collapse_ratio"),
            F.col("days_since_last_sale"),
            F.col("shelf_life_days"),
        )
    )

    return result


class CollapseSignalsWriter(FeatureStoreBase):
    def feature_table_name(self) -> str:
        return "phantom.collapse_signals"

    def primary_keys(self) -> list[str]:
        return ["werks", "unified_sku_id"]

    def compute(self) -> DataFrame:
        return compute_collapse_signals(
            self.spark.table("gold.sales.velocity"),
            self.spark.table("silver.sap.enriched_movements"),
            self.spark.table("silver.master.unified_sku_registry"),
        )


def _entry_collapse_signals() -> None:
    from pyspark.sql import SparkSession
    CollapseSignalsWriter(SparkSession.getActiveSession()).run()
