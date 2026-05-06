from databricks.feature_engineering import FeatureEngineeringClient
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def load_features(spark: SparkSession) -> DataFrame:
    """
    Load all three Phase 2A feature tables from FEU and join on (werks, unified_sku_id).
    Returns a single DataFrame ready for PhantomStockClassifier.predict().

    Feature columns produced (8 model inputs + identity + shelf_life context):
      sales_7d, sales_14d, sales_28d, sales_90d,
      stock_qty, days_of_cover,
      velocity_collapse_ratio, days_since_last_sale,
      shelf_life_days (context only — not a model input)
    """
    fs = FeatureEngineeringClient()
    velocity = fs.read_table("feature_store.velocity.sku_site_velocity")
    stock = fs.read_table("feature_store.stock.sku_site_positions")
    collapse = fs.read_table("feature_store.phantom.collapse_signals")
    return (
        velocity
        .join(stock.drop("uom", "_computed_at"), on=["werks", "unified_sku_id"], how="inner")
        .join(collapse.drop("_computed_at"), on=["werks", "unified_sku_id"], how="inner")
    )


def _entry_score_batch() -> None:
    from pyspark.sql import SparkSession
    from engines.phantom_stock.classifier import PhantomStockClassifier

    spark = SparkSession.getActiveSession()
    clf = PhantomStockClassifier()
    clf.load()
    features = load_features(spark)
    scores = clf.predict(features)
    # Persist scores for downstream write_alerts task
    scores.write.format("delta").mode("overwrite").saveAsTable(
        "gold.phantom_stock.scores"
    )


def _entry_write_alerts() -> None:
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    scores = spark.table("gold.phantom_stock.scores")

    # Full overwrite — consumed daily by Phase 4 React app exception queue
    scores.write.format("delta").mode("overwrite").saveAsTable(
        "gold.phantom_stock.alerts"
    )

    # Append-only review queue — manager resolves via Phase 4 app
    review = scores.filter(F.col("action") == "PENDING_REVIEW").select(
        "werks",
        "unified_sku_id",
        "phantom_score",
        "_scored_at",
        F.lit(None).cast("timestamp").alias("_resolved_at"),
        F.lit(None).cast("string").alias("_resolution"),
    )
    review.write.format("delta").mode("append").saveAsTable(
        "gold.phantom_stock.review_queue"
    )


def _entry_bapi_writeback() -> None:
    import mlflow
    from pyspark.sql import SparkSession
    from databricks.sdk.runtime import dbutils
    from engines.phantom_stock.bapi_client import BAPIClient, BAPIError

    spark = SparkSession.getActiveSession()
    token = dbutils.secrets.get(scope="sap-btp", key="ai-core-token")
    endpoint = dbutils.secrets.get(scope="sap-btp", key="ai-core-endpoint")
    client = BAPIClient(endpoint_url=endpoint, token=token)

    auto_rows = (
        spark.table("gold.phantom_stock.alerts")
        .filter(F.col("action") == "AUTO_CORRECTED")
        .collect()
    )

    mlflow.set_experiment("phantom_stock_bapi_writeback")
    with mlflow.start_run():
        results = []
        for row in auto_rows:
            try:
                response = client.post_goods_movement(
                    werks=row["werks"],
                    unified_sku_id=row["unified_sku_id"],
                )
                results.append({"werks": row["werks"], "sku": row["unified_sku_id"],
                                 "status": "ok", "response": response})
            except BAPIError as exc:
                results.append({"werks": row["werks"], "sku": row["unified_sku_id"],
                                 "status": "error", "message": str(exc)})
        mlflow.log_dict({"writeback_audit": results}, "bapi_audit.json")
        mlflow.log_metric("auto_corrected_count", len(auto_rows))
        error_count = sum(1 for r in results if r["status"] == "error")
        mlflow.log_metric("bapi_error_count", error_count)
