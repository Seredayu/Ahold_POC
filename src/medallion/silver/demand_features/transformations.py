from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window


def clean_materials(df: DataFrame) -> DataFrame:
    """Dedup on MATNR (keep latest LAEDA), filter MTART IN (FERT, HALB, ROH), cast MHDRZ/MHDLP to int."""
    w = Window.partitionBy("MATNR").orderBy(F.col("LAEDA").desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .filter(F.col("MTART").isin("FERT", "HALB", "ROH"))
        .withColumn("MHDRZ", F.col("MHDRZ").cast("int"))
        .withColumn("MHDLP", F.col("MHDLP").cast("int"))
    )


def clean_inventory(df: DataFrame) -> DataFrame:
    """Dedup on (MBLNR, ZEILE), filter BWART IN (101,102,261,262,601,602), cast BUDAT/CPUDT to date."""
    w = Window.partitionBy("MBLNR", "ZEILE").orderBy(F.col("CPUDT").desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .filter(F.col("BWART").isin("101", "102", "261", "262", "601", "602"))
        .withColumn("BUDAT", F.col("BUDAT").cast("date"))
        .withColumn("CPUDT", F.col("CPUDT").cast("date"))
    )


def clean_open_orders(df: DataFrame) -> DataFrame:
    """Dedup on (EBELN, EBELP), filter MENGE > 0, cast EINDT/BEDAT to date."""
    w = Window.partitionBy("EBELN", "EBELP").orderBy(F.col("BEDAT").desc())
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .filter(F.col("MENGE") > 0)
        .withColumn("EINDT", F.col("EINDT").cast("date"))
        .withColumn("BEDAT", F.col("BEDAT").cast("date"))
    )


def build_enriched_movements(
    inventory_clean: DataFrame,
    materials_clean: DataFrame,
    sku_registry: DataFrame,
) -> DataFrame:
    """LEFT JOIN inventory_clean -> materials_clean -> sku_registry on MATNR, add unified_sku_id etc."""
    return (
        inventory_clean
        .join(materials_clean.select("MATNR", "MAKTX", "MTART", "MHDRZ"), on="MATNR", how="left")
        .join(
            sku_registry.select("sap_material_number", "unified_sku_id", "match_confidence"),
            inventory_clean["MATNR"] == sku_registry["sap_material_number"],
            how="left",
        )
        .drop("sap_material_number")
        .withColumnRenamed("MAKTX", "material_description")
        .withColumnRenamed("MTART", "material_type")
        .withColumnRenamed("MHDRZ", "shelf_life_days")
    )
