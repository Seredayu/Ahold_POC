from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window


class GoldAggregateBase(ABC):
    def __init__(self, spark: SparkSession, catalog: str = "gold"):
        self.spark = spark
        self.catalog = catalog

    @abstractmethod
    def target_table(self) -> str: ...

    @abstractmethod
    def compute(self) -> DataFrame: ...

    def run(self) -> None:
        df = self.compute()
        df = df.withColumn("_computed_at", F.current_timestamp())
        df.write.format("delta").mode("overwrite").saveAsTable(
            f"{self.catalog}.{self.target_table()}"
        )


def compute_daily_positions(inventory_clean: DataFrame, sku_registry: DataFrame) -> DataFrame:
    """
    Net stock positions by (WERKS, unified_sku_id, BUDAT).

    Movement type sign rules:
      101 (goods receipt)          → +MENGE
      102 (GR reversal)            → -MENGE
      261 (goods issue to CC)      → -MENGE
      262 (GI reversal)            → +MENGE
      601 (delivery to customer)   → -MENGE
      602 (delivery reversal)      → +MENGE

    Steps:
    1. Join inventory_clean to sku_registry on MATNR == sap_material_number, keep unified_sku_id + MEINS
    2. Apply sign to MENGE
    3. Sum signed_qty by (WERKS, unified_sku_id, BUDAT, MEINS)
    4. Compute cumulative sum window ordered by BUDAT, take latest snapshot per (WERKS, unified_sku_id)
    5. Return columns: werks, unified_sku_id, snapshot_date, stock_qty, uom
    """
    POSITIVE = ("101", "262", "602")
    NEGATIVE = ("102", "261", "601")

    joined = (
        inventory_clean
        .join(
            sku_registry.select("sap_material_number", "unified_sku_id", "MEINS"),
            inventory_clean["MATNR"] == sku_registry["sap_material_number"],
            how="inner",
        )
        .drop("sap_material_number")
    )

    signed = joined.withColumn(
        "signed_qty",
        F.when(F.col("BWART").isin(*POSITIVE), F.col("MENGE"))
         .when(F.col("BWART").isin(*NEGATIVE), -F.col("MENGE"))
         .otherwise(F.lit(0)),
    )

    daily = (
        signed
        .groupBy("WERKS", "unified_sku_id", "BUDAT", "MEINS")
        .agg(F.sum("signed_qty").alias("daily_net"))
    )

    w_cum = (
        Window.partitionBy("WERKS", "unified_sku_id")
        .orderBy("BUDAT")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    cumulative = daily.withColumn("stock_qty", F.sum("daily_net").over(w_cum))

    w_latest = Window.partitionBy("WERKS", "unified_sku_id").orderBy(F.col("BUDAT").desc())

    return (
        cumulative
        .withColumn("_rn", F.row_number().over(w_latest))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "daily_net")
        .select(
            F.col("WERKS").alias("werks"),
            F.col("unified_sku_id"),
            F.col("BUDAT").alias("snapshot_date"),
            F.col("stock_qty").cast("decimal(13,3)"),
            F.col("MEINS").alias("uom"),
        )
    )
