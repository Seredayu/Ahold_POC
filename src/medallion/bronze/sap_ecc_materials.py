from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, DecimalType, StringType, StructField, StructType

from .autoloader import AutoLoaderBase

_DEFAULT_SOURCE = "abfss://bronze@{storage}.dfs.core.windows.net/sap/materials/"
_DEFAULT_CKPT   = "abfss://bronze@{storage}.dfs.core.windows.net/_checkpoints/sap_materials/"


class SAPECCMaterialsLoader(AutoLoaderBase):
    """Bronze Auto Loader for MARA + MARC material master CDC feed.
    Lands to bronze.sap.materials. Key columns: MATNR, WERKS.
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
            table="materials",
        )

    def target_schema(self) -> StructType:
        return StructType([
            StructField("MATNR", StringType(), True),
            StructField("EAN11", StringType(), True),    # EAN barcode — Rosetta Stone primary key
            StructField("WERKS", StringType(), True),
            StructField("MAKTX", StringType(), True),
            StructField("MTART", StringType(), True),
            StructField("MATKL", StringType(), True),
            StructField("MEINS", StringType(), True),
            StructField("MHDRZ", DecimalType(5, 0), True),
            StructField("IPRKZ", StringType(), True),
            StructField("MHDLP", DecimalType(5, 0), True),
            StructField("LAEDA", DateType(), True),
            StructField("ERSDA", DateType(), True),
        ])
