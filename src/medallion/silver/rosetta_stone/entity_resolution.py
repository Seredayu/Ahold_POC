from dataclasses import dataclass
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window


@dataclass
class UnifiedSKU:
    sap_material_number: str
    symphony_gold_item_code: Optional[str]
    ean_barcode: Optional[str]
    unified_sku_id: str
    match_confidence: float
    source_banner: str


def build_registry(bronze_df: DataFrame) -> DataFrame:
    w = Window.partitionBy("MATNR").orderBy(F.col("LAEDA").desc())
    deduped = (
        bronze_df
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    return (
        deduped
        .withColumn(
            "unified_sku_id",
            F.when(F.col("EAN11").isNotNull(), F.col("EAN11"))
             .otherwise(F.concat(F.lit("MATNR-"), F.col("MATNR"))),
        )
        .withColumn(
            "match_confidence",
            F.when(F.col("EAN11").isNotNull(), F.lit(1.0)).otherwise(F.lit(0.0)),
        )
        .withColumn("source_banner", F.lit("AH_NL"))
        .select(
            F.col("unified_sku_id"),
            F.col("MATNR").alias("sap_material_number"),
            F.col("EAN11").alias("ean_barcode"),
            F.col("MAKTX").alias("material_description"),
            F.col("MTART").alias("material_type"),
            F.col("MATKL").alias("material_group"),
            F.col("MEINS").alias("base_uom"),
            F.col("MHDRZ").alias("shelf_life_days"),
            F.col("MHDLP").alias("min_remaining_shelf_life"),
            F.col("match_confidence"),
            F.col("source_banner"),
        )
    )


def _compute_match_rate(registry_df: DataFrame) -> float:
    total = registry_df.count()
    if total == 0:
        return 0.0
    matched = registry_df.filter(F.col("ean_barcode").isNotNull()).count()
    return matched / total


class RosettaStone:
    TARGET_MATCH_RATE = 0.95

    def __init__(self, spark: SparkSession, catalog: str = "silver", schema: str = "master"):
        self._spark = spark
        self._table = f"{catalog}.{schema}.unified_sku_registry"

    def get_match_rate(self) -> float:
        return _compute_match_rate(self._spark.table(self._table))

    def is_ready(self) -> bool:
        return self.get_match_rate() >= self.TARGET_MATCH_RATE

    def lookup_by_sap(self, sap_material_number: str) -> Optional[UnifiedSKU]:
        rows = (
            self._spark.table(self._table)
            .filter(F.col("sap_material_number") == sap_material_number)
            .collect()
        )
        if not rows:
            return None
        r = rows[0]
        return UnifiedSKU(
            sap_material_number=r.sap_material_number,
            symphony_gold_item_code=None,
            ean_barcode=r.ean_barcode,
            unified_sku_id=r.unified_sku_id,
            match_confidence=r.match_confidence,
            source_banner=r.source_banner,
        )
