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


def _classify_finalize_row(row: dict) -> tuple:
    """Returns (needs_bapi, approval_status). Pure — no Spark."""
    action = row["sweeper_action"]
    decision = row.get("manager_decision")
    if action == "AUTO_APPROVE":
        return True, "SWEEPER_AUTO_APPROVED"
    if action == "BLOCK":
        return False, "BLOCKED"
    # ESCALATE
    if decision == "APPROVED":
        return True, "MANAGER_APPROVED"
    if decision == "REJECTED":
        return False, "MANAGER_REJECTED"
    return True, "FORCE_APPROVED"


def _build_edi_lines_by_vendor(rows: list) -> dict:
    """Group consolidated po_audit+registry rows into EDI lines keyed by vendor_id."""
    from integration.edi.edi_850_generator import EDI850Line

    result: dict = {}
    for row in rows:
        vendor_id = str(row.get("vendor_id") or "UNKNOWN")
        ship_date = (
            date.today() + timedelta(days=int(row.get("transit_days") or 0))
        ).strftime("%Y%m%d")
        line_number = len(result.get(vendor_id, [])) + 1
        line = EDI850Line(
            po_number=str(row["po_number"]),
            line_number=line_number,
            ean_barcode=str(row.get("ean_barcode") or ""),
            quantity=int(row["recommended_qty"]),
            unit="EA",
            unit_price=float(row.get("unit_cost") or 0.0),
            requested_ship_date=ship_date,
        )
        result.setdefault(vendor_id, []).append(line)
    return result


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
    from databricks.sdk.runtime import dbutils
    from pyspark.sql import SparkSession
    from engines.freshness.po_client import POClient
    from engines.phantom_stock.bapi_client import BAPIError
    from integration.edi.edi_850_generator import EDI850Generator

    spark = SparkSession.getActiveSession()
    token = dbutils.secrets.get(scope="sap-btp", key="ai-core-token")
    endpoint = dbutils.secrets.get(scope="sap-btp", key="ai-core-endpoint")
    conn_str = dbutils.secrets.get(scope="azure-storage", key="connection-string")

    # Read exception queue; gracefully skip BAPI loop if unavailable
    try:
        queue_pdf = spark.table("gold.replenishment.exception_queue").toPandas()
    except Exception:
        _log.warning("exception_queue unavailable — skipping Sweeper BAPI loop")
        queue_pdf = None

    client = POClient(endpoint_url=endpoint, token=token)
    audit_rows: list = []
    force_approved = manager_approved = manager_rejected = 0

    if queue_pdf is not None and not queue_pdf.empty:
        for _, row in queue_pdf.iterrows():
            needs_bapi, approval_status = _classify_finalize_row(row.to_dict())
            if approval_status == "FORCE_APPROVED":
                force_approved += 1
            elif approval_status == "MANAGER_APPROVED":
                manager_approved += 1
            elif approval_status == "MANAGER_REJECTED":
                manager_rejected += 1

            if needs_bapi:
                try:
                    response = client.create_purchase_order(
                        werks=str(row["werks"]),
                        unified_sku_id=str(row["unified_sku_id"]),
                        quantity=int(row["recommended_qty"]),
                    )
                    audit_rows.append({
                        "werks": str(row["werks"]),
                        "unified_sku_id": str(row["unified_sku_id"]),
                        "recommended_qty": int(row["recommended_qty"]),
                        "approval_status": approval_status,
                        "bapi_status": "ok",
                        "po_number": response.get("PO_NUMBER"),
                        "bapi_message": None,
                    })
                except BAPIError as exc:
                    audit_rows.append({
                        "werks": str(row["werks"]),
                        "unified_sku_id": str(row["unified_sku_id"]),
                        "recommended_qty": int(row["recommended_qty"]),
                        "approval_status": approval_status,
                        "bapi_status": "error",
                        "po_number": None,
                        "bapi_message": str(exc),
                    })
            else:
                audit_rows.append({
                    "werks": str(row["werks"]),
                    "unified_sku_id": str(row["unified_sku_id"]),
                    "recommended_qty": int(row["recommended_qty"]),
                    "approval_status": approval_status,
                    "bapi_status": "skipped",
                    "po_number": None,
                    "bapi_message": None,
                })

    if audit_rows:
        import mlflow
        from pyspark.sql import functions as F
        audit_df = (
            spark.createDataFrame(audit_rows, schema=_po_audit_schema())
            .withColumn("_created_at", F.current_timestamp())
        )
        audit_df.write.format("delta").mode("append").saveAsTable(
            "gold.replenishment.po_audit"
        )

    # EDI consolidation — all approved POs for today (Phase 3B AUTO_APPROVED + Sweeper)
    import mlflow
    from pyspark.sql import functions as F
    today = date.today().isoformat()
    po_audit_df = (
        spark.table("gold.replenishment.po_audit")
        .filter(F.col("_created_at").cast("date") == today)
        .filter(F.col("bapi_status") == "ok")
    )
    sku_registry = spark.table("silver.master.unified_sku_registry").select(
        "werks", "unified_sku_id", "ean_barcode", "vendor_id", "unit_cost", "transit_days"
    )
    consolidated = (
        po_audit_df.join(sku_registry, on=["werks", "unified_sku_id"], how="left")
        .toPandas()
    )

    if consolidated.empty:
        raise RuntimeError(
            "No approved POs found for EDI 850 — possible pipeline failure"
        )

    edi_gen = EDI850Generator(connection_string=conn_str)
    lines_by_vendor = _build_edi_lines_by_vendor(consolidated.to_dict("records"))
    edi_files = []
    for vendor_id, lines in lines_by_vendor.items():
        filename = edi_gen.generate(
            lines=lines, sender_id=_EDI_SENDER_ID, receiver_id=vendor_id
        )
        edi_files.append(filename)

    mlflow.set_experiment("freshness_orchestrator")
    with mlflow.start_run():
        mlflow.log_metric("force_approved_count", force_approved)
        mlflow.log_metric("manager_approved_count", manager_approved)
        mlflow.log_metric("manager_rejected_count", manager_rejected)
        mlflow.log_metric(
            "sweeper_bapi_created_count",
            sum(1 for r in audit_rows if r["bapi_status"] == "ok"),
        )
        mlflow.log_metric(
            "sweeper_bapi_error_count",
            sum(1 for r in audit_rows if r["bapi_status"] == "error"),
        )
        mlflow.log_metric("edi_files_generated", len(edi_files))
        mlflow.log_metric(
            "total_po_lines",
            sum(len(v) for v in lines_by_vendor.values()),
        )
