from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def generate_labels(
    inventory_df: DataFrame,   # gold.inventory.daily_positions
    velocity_df: DataFrame,    # gold.sales.velocity
) -> DataFrame:
    """
    Label (werks, unified_sku_id) as phantom if:
      stock_qty > 0 AND sales_7d = 0 AND sales_28d > 5

    Returns DataFrame with columns: werks, unified_sku_id, is_phantom (int 0/1), label_date
    """
    vel = velocity_df.select("werks", "unified_sku_id", "sales_7d", "sales_28d")
    return (
        inventory_df
        .join(vel, on=["werks", "unified_sku_id"], how="inner")
        .withColumn(
            "is_phantom",
            F.when(
                (F.col("stock_qty") > F.lit(0).cast("decimal(13,3)"))
                & (F.col("sales_7d") == F.lit(0).cast("decimal(13,3)"))
                & (F.col("sales_28d") > F.lit(5).cast("decimal(13,3)")),
                F.lit(1),
            ).otherwise(F.lit(0)).cast("int"),
        )
        .withColumn("label_date", F.current_date())
        .select("werks", "unified_sku_id", "is_phantom", "label_date")
    )


def _entry_generate_labels() -> None:
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    labels = generate_labels(
        spark.table("gold.inventory.daily_positions"),
        spark.table("gold.sales.velocity"),
    )
    labels.write.format("delta").mode("overwrite").saveAsTable(
        "gold.phantom_stock.training_labels"
    )
