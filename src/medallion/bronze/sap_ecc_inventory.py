from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, DecimalType, StringType, StructField, StructType, TimestampType

from .autoloader import AutoLoaderBase

_DEFAULT_SOURCE = "abfss://bronze@{storage}.dfs.core.windows.net/sap/inventory/"
_DEFAULT_CKPT   = "abfss://bronze@{storage}.dfs.core.windows.net/_checkpoints/sap_inventory/"


class SAPECCInventoryLoader(AutoLoaderBase):
    """Bronze Auto Loader for MSEG + MKPF goods movement CDC feed.
    Lands to bronze.sap.inventory. Key columns: MBLNR, ZEILE.
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
            table="inventory",
        )

    def target_schema(self) -> StructType:
        return StructType([
            StructField("MBLNR", StringType(), True),
            StructField("ZEILE", StringType(), True),
            StructField("MATNR", StringType(), True),
            StructField("WERKS", StringType(), True),
            StructField("LGORT", StringType(), True),
            StructField("BWART", StringType(), True),
            StructField("MENGE", DecimalType(13, 3), True),
            StructField("MEINS", StringType(), True),
            StructField("BUDAT", DateType(), True),
            StructField("CPUDT", DateType(), True),
            StructField("CPUTM", TimestampType(), True),
        ])
