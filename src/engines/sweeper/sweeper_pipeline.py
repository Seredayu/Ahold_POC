import logging
from datetime import date, datetime, time, timedelta

_log = logging.getLogger(__name__)

# Heavy runtime imports (pyspark, mlflow) are deferred to function bodies so
# that unit tests can import this module and exercise pure-Python helpers
# (_compute_deviation, _minutes_to_edi_deadline) without a Spark/MLflow
# environment.  Schema constants are also defined lazily for the same reason.

_EDI_SENDER_ID = "AHOLD_NL"
_EDI_DEADLINE = time(8, 15)


def _exception_queue_schema():
    from pyspark.sql.types import (
        DoubleType, IntegerType, StringType,
        StructField, StructType, TimestampType,
    )
    return StructType([
        StructField("werks", StringType(), False),
        StructField("unified_sku_id", StringType(), False),
        StructField("recommended_qty", IntegerType(), False),
        StructField("transit_to_life_ratio", DoubleType(), False),
        StructField("order_value", DoubleType(), False),
        StructField("quantity_deviation_pct", DoubleType(), False),
        StructField("sweeper_action", StringType(), False),
        StructField("manager_decision", StringType(), True),
        StructField("manager_id", StringType(), True),
        StructField("decision_timestamp", TimestampType(), True),
        StructField("override_reason", StringType(), True),
    ])


def _po_audit_schema():
    from pyspark.sql.types import (
        IntegerType, StringType,
        StructField, StructType,
    )
    return StructType([
        StructField("werks", StringType(), True),
        StructField("unified_sku_id", StringType(), True),
        StructField("recommended_qty", IntegerType(), True),
        StructField("approval_status", StringType(), True),
        StructField("bapi_status", StringType(), True),
        StructField("po_number", StringType(), True),
        StructField("bapi_message", StringType(), True),
    ])


def _compute_deviation(recommended_qty: int, p50: float) -> float:
    if p50 <= 0.0:
        return 0.0
    return abs(recommended_qty - p50) / p50


def _minutes_to_edi_deadline() -> int:
    now = datetime.now()
    deadline = datetime.combine(now.date(), _EDI_DEADLINE)
    delta = (deadline - now).total_seconds() / 60
    return max(0, int(delta))


def _entry_load_exceptions() -> None:
    import mlflow
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from engines.sweeper.state_machine import SweeperStateMachine
    from engines.sweeper.decision_rules import SweeperAction

    spark = SparkSession.getActiveSession()
    sm = SweeperStateMachine()

    pending = (
        spark.table("gold.replenishment.order_recommendations")
        .filter(F.col("approval_status") == "PENDING_REVIEW")
    )
    m4 = spark.table("feature_store.demand.m4_probabilistic").select(
        "werks", "unified_sku_id", "p50"
    )
    joined = pending.join(m4, on=["werks", "unified_sku_id"], how="left").toPandas()

    rows = []
    for _, row in joined.iterrows():
        p50 = float(row.get("p50") or 0.0)
        qty = int(row["recommended_qty"])
        deviation = _compute_deviation(qty, p50)
        context = {
            "transit_to_life_ratio": float(row["transit_to_life_ratio"]),
            "quantity_deviation_pct": deviation,
            "minutes_to_deadline": _minutes_to_edi_deadline(),
        }
        action = sm.process_exception(context)
        rows.append({
            "werks": str(row["werks"]),
            "unified_sku_id": str(row["unified_sku_id"]),
            "recommended_qty": qty,
            "transit_to_life_ratio": float(row["transit_to_life_ratio"]),
            "order_value": float(row.get("order_value") or 0.0),
            "quantity_deviation_pct": deviation,
            "sweeper_action": action.value,
            "manager_decision": None,
            "manager_id": None,
            "decision_timestamp": None,
            "override_reason": None,
        })

    output_df = (
        spark.createDataFrame(rows, schema=_exception_queue_schema())
        .withColumn("_loaded_at", F.current_timestamp())
    )
    output_df.write.format("delta").mode("overwrite").saveAsTable(
        "gold.replenishment.exception_queue"
    )

    mlflow.set_experiment("freshness_orchestrator")
    with mlflow.start_run():
        mlflow.log_metric("pending_review_input_count", len(rows))
        mlflow.log_metric(
            "sweeper_auto_approved_count",
            sum(1 for r in rows if r["sweeper_action"] == SweeperAction.AUTO_APPROVE.value),
        )
        mlflow.log_metric(
            "sweeper_escalated_count",
            sum(1 for r in rows if r["sweeper_action"] == SweeperAction.ESCALATE.value),
        )
        mlflow.log_metric(
            "sweeper_blocked_count",
            sum(1 for r in rows if r["sweeper_action"] == SweeperAction.BLOCK.value),
        )


def _entry_finalize() -> None:
    pass  # implemented in Task 2
