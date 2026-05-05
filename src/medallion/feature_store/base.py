from abc import ABC, abstractmethod

from databricks.feature_engineering import FeatureEngineeringClient
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


class FeatureStoreBase(ABC):
    def __init__(self, spark: SparkSession, catalog: str = "feature_store"):
        self.spark = spark
        self.catalog = catalog
        self._fs = FeatureEngineeringClient()

    @abstractmethod
    def feature_table_name(self) -> str: ...

    @abstractmethod
    def primary_keys(self) -> list[str]: ...

    @abstractmethod
    def compute(self) -> DataFrame: ...

    def run(self) -> None:
        df = self.compute().withColumn("_computed_at", F.current_timestamp())
        self._fs.write_table(
            name=f"{self.catalog}.{self.feature_table_name()}",
            df=df,
            mode="overwrite",
        )
