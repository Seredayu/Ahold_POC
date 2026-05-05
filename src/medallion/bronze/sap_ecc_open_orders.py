from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, DecimalType, StringType, StructField, StructType

from .autoloader import AutoLoaderBase

_DEFAULT_SOURCE = "abfss://bronze@{storage}.dfs.core.windows.net/sap/open_orders/"
_DEFAULT_CKPT   = "abfss://bronze@{storage}.dfs.core.windows.net/_checkpoints/sap_open_orders/"


class SAPECCOpenOrdersLoader(AutoLoaderBase):
    """Bronze Auto Loader for EKKO + EKPO purchase order CDC feed.
    Lands to bronze.sap.open_orders. Key columns: EBELN, EBELP.
    """

    def __init__(
        self,
        spark: SparkSession,
        storage_account: str = "saholdpocdata",
        catalog: str = "bronze",
    ):
        super().__init__(
            spark=spark,
            source_path=_DEFAULT_SOURCE.format(storage=storage_account),
            checkpoint_path=_DEFAULT_CKPT.format(storage=storage_account),
            catalog=catalog,
            schema="sap",
            table="open_orders",
        )

    def target_schema(self) -> StructType:
        return StructType([
            StructField("EBELN", StringType(), True),
            StructField("EBELP", StringType(), True),
            StructField("MATNR", StringType(), True),
            StructField("WERKS", StringType(), True),
            StructField("LIFNR", StringType(), True),
            StructField("MENGE", DecimalType(13, 3), True),
            StructField("MEINS", StringType(), True),
            StructField("NETPR", DecimalType(11, 2), True),
            StructField("WAERS", StringType(), True),
            StructField("EINDT", DateType(), True),
            StructField("BEDAT", DateType(), True),
            StructField("EKORG", StringType(), True),
            StructField("EKGRP", StringType(), True),
        ])
