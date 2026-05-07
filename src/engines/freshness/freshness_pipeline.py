import mlflow
from dataclasses import asdict
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType,
    StringType, StructField, StructType,
)

_PO_CREATE_SANITY_CAP = 200

_SOLVER_OUTPUT_SCHEMA = StructType([
    StructField("sku_id", StringType(), False),
    StructField("site_id", StringType(), False),
    StructField("recommended_qty", IntegerType(), True),
    StructField("transit_to_life_ratio", DoubleType(), True),
    StructField("blocked", BooleanType(), True),
    StructField("solver_status", StringType(), True),
    StructField("approval_status", StringType(), True),
    StructField("approval_reason", StringType(), True),
    StructField("confidence_ratio", DoubleType(), True),
    StructField("order_value", DoubleType(), True),
    StructField("ttl_policy_applied", StringType(), True),
    StructField("day_old_discount", BooleanType(), True),
    StructField("promo_lift", DoubleType(), True),
    StructField("weather_lift", DoubleType(), True),
])

_PO_AUDIT_SCHEMA = StructType([
    StructField("werks", StringType(), True),
    StructField("unified_sku_id", StringType(), True),
    StructField("recommended_qty", IntegerType(), True),
    StructField("approval_status", StringType(), True),
    StructField("bapi_status", StringType(), True),
    StructField("po_number", StringType(), True),
    StructField("bapi_message", StringType(), True),
])


def _entry_freshness_milp_solve() -> None:
    from engines.freshness.solver_interface import SolverInput
    from engines.freshness.replenishment_quantity_optimizer import PuLPCBCSolver
    from engines.freshness.approval_gate import ApprovalGate

    spark = SparkSession.getActiveSession()
    solver = PuLPCBCSolver()
    gate = ApprovalGate()

    m4 = spark.table("feature_store.demand.m4_probabilistic")
    m2 = spark.table("feature_store.demand.m2_corrected").select(
        "werks", "unified_sku_id", "promo_lift", "weather_lift"
    )
    stock = spark.table("feature_store.stock.sku_site_positions").select(
        "werks", "unified_sku_id", "stock_qty"
    )
    sku_registry = spark.table("silver.master.unified_sku_registry").select(
        "werks", "unified_sku_id", "unit_cost", "transit_days",
        "min_order_qty", "max_order_qty", "category",
        "shelf_life_days", "truck_capacity_units",
    )

    joined = (
        m4
        .join(m2, on=["werks", "unified_sku_id"], how="inner")
        .join(stock, on=["werks", "unified_sku_id"], how="inner")
        .join(sku_registry, on=["werks", "unified_sku_id"], how="inner")
        .dropna(subset=[
            "p10", "p50", "p90", "stock_qty", "shelf_life_days",
            "transit_days", "unit_cost", "min_order_qty", "max_order_qty",
            "category", "truck_capacity_units",
        ])
    )

    pandas_df = joined.toPandas()

    inputs = [
        SolverInput(
            sku_id=row["unified_sku_id"],
            site_id=row["werks"],
            demand_p10=float(row["p10"]),
            demand_p50=float(row["p50"]),
            demand_p90=float(row["p90"]),
            current_stock=int(row["stock_qty"]),
            shelf_life_days=int(row["shelf_life_days"]),
            transit_days=int(row["transit_days"]),
            min_order_qty=int(row["min_order_qty"]),
            max_order_qty=int(row["max_order_qty"]),
            truck_capacity_remaining=float(row["truck_capacity_units"]),
            category=str(row["category"]),
            unit_cost=float(row["unit_cost"]),
        )
        for _, row in pandas_df.iterrows()
    ]

    recommendations = solver.solve(inputs)
    evaluated = [gate.evaluate(rec, inp) for rec, inp in zip(recommendations, inputs)]

    lift_lookup = {
        (row["unified_sku_id"], row["werks"]): (
            float(row["promo_lift"]), float(row["weather_lift"])
        )
        for _, row in pandas_df.iterrows()
    }

    output_rows = []
    for rec in evaluated:
        promo_lift, weather_lift = lift_lookup.get((rec.sku_id, rec.site_id), (1.0, 1.0))
        d = asdict(rec)
        d["promo_lift"] = promo_lift
        d["weather_lift"] = weather_lift
        output_rows.append(d)

    output_df = spark.createDataFrame(output_rows, schema=_SOLVER_OUTPUT_SCHEMA)
    output_df.write.format("delta").mode("overwrite").saveAsTable(
        "gold.replenishment.solver_output"
    )


def _entry_write_recommendations() -> None:
    spark = SparkSession.getActiveSession()

    solver_output = (
        spark.table("gold.replenishment.solver_output")
        .withColumnRenamed("sku_id", "unified_sku_id")
        .withColumnRenamed("site_id", "werks")
    )
    m4 = spark.table("feature_store.demand.m4_probabilistic").select(
        "werks", "unified_sku_id", "p10", "p50", "p90", "uncertainty_spread"
    )

    recs = (
        solver_output
        .join(m4, on=["werks", "unified_sku_id"], how="left")
        .withColumn("_computed_at", F.current_timestamp())
        .select(
            "werks", "unified_sku_id", "recommended_qty",
            "transit_to_life_ratio", "approval_status", "approval_reason",
            "confidence_ratio", "order_value", "ttl_policy_applied",
            "day_old_discount", "solver_status",
            "p10", "p50", "p90", "uncertainty_spread",
            "promo_lift", "weather_lift", "_computed_at",
        )
    )

    recs.write.format("delta").mode("overwrite").saveAsTable(
        "gold.replenishment.order_recommendations"
    )

    pandas_recs = recs.toPandas()
    mlflow.set_experiment("freshness_orchestrator")
    with mlflow.start_run():
        mlflow.log_metric(
            "auto_approved_count",
            int((pandas_recs["approval_status"] == "AUTO_APPROVED").sum()),
        )
        mlflow.log_metric(
            "pending_review_count",
            int((pandas_recs["approval_status"] == "PENDING_REVIEW").sum()),
        )
        mlflow.log_metric(
            "blocked_count",
            int((pandas_recs["approval_status"] == "BLOCKED").sum()),
        )
        total_value = float(
            pandas_recs.loc[
                pandas_recs["approval_status"] == "AUTO_APPROVED", "order_value"
            ].sum()
        )
        mlflow.log_metric("total_order_value_eur", total_value)


def _entry_bapi_po_create() -> None:
    from databricks.sdk.runtime import dbutils
    from engines.freshness.po_client import POClient
    from engines.phantom_stock.bapi_client import BAPIError

    spark = SparkSession.getActiveSession()
    token = dbutils.secrets.get(scope="sap-btp", key="ai-core-token")
    endpoint = dbutils.secrets.get(scope="sap-btp", key="ai-core-endpoint")
    client = POClient(endpoint_url=endpoint, token=token)

    auto_rows = (
        spark.table("gold.replenishment.order_recommendations")
        .filter(
            (F.col("approval_status") == "AUTO_APPROVED")
            & (F.col("recommended_qty") > 0)
        )
        .collect()
    )

    if len(auto_rows) > _PO_CREATE_SANITY_CAP:
        raise RuntimeError(
            f"AUTO_APPROVED count {len(auto_rows)} exceeds sanity cap {_PO_CREATE_SANITY_CAP}. "
            "Possible model drift — aborting BAPI PO create. "
            "Review gold.replenishment.order_recommendations."
        )

    audit_rows = []
    for row in auto_rows:
        try:
            response = client.create_purchase_order(
                werks=row["werks"],
                unified_sku_id=row["unified_sku_id"],
                quantity=row["recommended_qty"],
            )
            audit_rows.append({
                "werks": row["werks"],
                "unified_sku_id": row["unified_sku_id"],
                "recommended_qty": row["recommended_qty"],
                "approval_status": row["approval_status"],
                "bapi_status": "ok",
                "po_number": response.get("PO_NUMBER"),
                "bapi_message": None,
            })
        except BAPIError as exc:
            audit_rows.append({
                "werks": row["werks"],
                "unified_sku_id": row["unified_sku_id"],
                "recommended_qty": row["recommended_qty"],
                "approval_status": row["approval_status"],
                "bapi_status": "error",
                "po_number": None,
                "bapi_message": str(exc),
            })

    mlflow.set_experiment("freshness_orchestrator")
    with mlflow.start_run():
        mlflow.log_dict({"po_audit": audit_rows}, "po_audit.json")
        mlflow.log_metric(
            "po_created_count", sum(1 for r in audit_rows if r["bapi_status"] == "ok")
        )
        mlflow.log_metric(
            "po_error_count", sum(1 for r in audit_rows if r["bapi_status"] == "error")
        )

    audit_df = (
        spark.createDataFrame(audit_rows, schema=_PO_AUDIT_SCHEMA)
        .withColumn("_created_at", F.current_timestamp())
    )
    audit_df.write.format("delta").mode("append").saveAsTable(
        "gold.replenishment.po_audit"
    )
