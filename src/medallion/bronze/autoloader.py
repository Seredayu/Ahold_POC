from abc import ABC, abstractmethod

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


class AutoLoaderBase(ABC):
    def __init__(
        self,
        spark: SparkSession,
        source_path: str,
        checkpoint_path: str,
        catalog: str,
        schema: str,
        table: str,
    ):
        self.spark = spark
        self.source_path = source_path
        self.checkpoint_path = checkpoint_path
        self.target_table = f"{catalog}.{schema}.{table}"

    @abstractmethod
    def target_schema(self) -> StructType:
        ...

    def add_audit_columns(self, df: DataFrame) -> DataFrame:
        df = df.withColumn("_ingested_at", F.current_timestamp())
        if "_metadata" in df.columns:
            df = df.withColumn("_source_file", F.col("_metadata.file_path"))
            df = df.withColumn(
                "_cdc_operation",
                F.col("_metadata.file_modification_time").cast("string"),
            )
        else:
            df = df.withColumn("_source_file", F.lit(""))
            df = df.withColumn("_cdc_operation", F.lit(""))
        return df

    def read_stream(self) -> DataFrame:
        return (
            self.spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .schema(self.target_schema())
            .load(self.source_path)
        )

    def run(self) -> None:
        df = self.read_stream()
        df = self.add_audit_columns(df)
        (
            df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", self.checkpoint_path)
            .option("mergeSchema", "true")
            .trigger(availableNow=True)
            .toTable(self.target_table)
            .awaitTermination()
        )
