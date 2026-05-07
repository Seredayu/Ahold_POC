# Phase 4A: Sweeper + EDI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the existing `SweeperStateMachine` and `EDI850Generator` stubs into the morning pipeline — loading PENDING_REVIEW exceptions into a queryable queue, finalising all orders at 08:10 AM, and generating one EDI 850 per supplier.

**Architecture:** Two entry points in `src/engines/sweeper/sweeper_pipeline.py`. `_entry_load_exceptions` runs after `bapi_po_create` in `morning_pipeline.yml` and writes `gold.replenishment.exception_queue`. `_entry_finalize` runs at 08:10 AM as a separate cron job, calls BAPI for all approved rows, consolidates all PO numbers from `po_audit`, and generates one EDI 850 per vendor via `EDI850Generator`.

**Tech Stack:** PySpark, Delta Lake, MLflow, `POClient` (Phase 3B), `SweeperStateMachine` + `EDI850Generator` (existing stubs in `src/`), Databricks Asset Bundles.

---

## File Map

| Action | Path | Responsibility |
|---|---|---|
| Create | `src/engines/sweeper/sweeper_pipeline.py` | Two entry points + pure helper functions |
| Create | `tests/unit/test_sweeper_pipeline.py` | 9 unit tests |
| Create | `resources/jobs/sweeper_finalize.yml` | 08:10 AM cron DAB job |
| Modify | `resources/jobs/morning_pipeline.yml` | Add `sweeper_load_exceptions` task after `bapi_po_create` |
| Modify | `databricks.yml` | Add `sweeper_finalize` job reference |
| Modify | `setup.py` | Add 2 console_scripts |

**Unchanged:** `src/engines/sweeper/state_machine.py`, `src/engines/sweeper/decision_rules.py`, `src/integration/edi/edi_850_generator.py`, `src/engines/freshness/po_client.py`

---

## Task 1: `_entry_load_exceptions` — tests + implementation

**Files:**
- Create: `src/engines/sweeper/sweeper_pipeline.py`
- Create: `tests/unit/test_sweeper_pipeline.py`

Tests validate `SweeperStateMachine.process_exception()` behaviour (regression baseline) and the new `_compute_deviation` helper. `_classify_finalize_row` and `_build_edi_lines_by_vendor` are added in Task 2.

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_sweeper_pipeline.py`:

```python
import pytest


# ---------------------------------------------------------------------------
# Task 1 — SweeperStateMachine action evaluation (regression baseline)
# ---------------------------------------------------------------------------

def test_load_exceptions_auto_approve_low_deviation():
    from engines.sweeper.state_machine import SweeperStateMachine
    from engines.sweeper.decision_rules import SweeperAction

    sm = SweeperStateMachine()
    # deviation=0.10 < 0.25 threshold; TTL=0.3 < 0.5; 120 min to deadline
    action = sm.process_exception({
        "transit_to_life_ratio": 0.3,
        "quantity_deviation_pct": 0.10,
        "minutes_to_deadline": 120,
    })
    assert action == SweeperAction.AUTO_APPROVE


def test_load_exceptions_escalate_high_deviation():
    from engines.sweeper.state_machine import SweeperStateMachine
    from engines.sweeper.decision_rules import SweeperAction

    sm = SweeperStateMachine()
    # TTL=0.3 passes; deviation=0.40 > 0.25 → ESCALATE; 120 min → not at deadline cutoff
    action = sm.process_exception({
        "transit_to_life_ratio": 0.3,
        "quantity_deviation_pct": 0.40,
        "minutes_to_deadline": 120,
    })
    assert action == SweeperAction.ESCALATE


def test_load_exceptions_block_high_ttl():
    from engines.sweeper.state_machine import SweeperStateMachine
    from engines.sweeper.decision_rules import SweeperAction

    sm = SweeperStateMachine()
    # TTL=0.8 > 0.5 → TransitToLifeRule fires BLOCK immediately
    action = sm.process_exception({
        "transit_to_life_ratio": 0.8,
        "quantity_deviation_pct": 0.10,
        "minutes_to_deadline": 120,
    })
    assert action == SweeperAction.BLOCK


# ---------------------------------------------------------------------------
# Task 1 — _compute_deviation helper
# ---------------------------------------------------------------------------

def test_compute_deviation_normal():
    from engines.sweeper.sweeper_pipeline import _compute_deviation
    # abs(120 - 100) / 100 = 0.20
    assert abs(_compute_deviation(120, 100.0) - 0.20) < 1e-9


def test_compute_deviation_zero_p50():
    from engines.sweeper.sweeper_pipeline import _compute_deviation
    # p50=0 → return 0.0, not ZeroDivisionError
    assert _compute_deviation(80, 0.0) == 0.0
```

- [ ] **Step 2: Run tests to verify the split between passing and failing**

```bash
pytest tests/unit/test_sweeper_pipeline.py -v
```

Expected: tests 1–3 PASS (state machine exists), tests 4–5 FAIL with `ModuleNotFoundError: No module named 'engines.sweeper.sweeper_pipeline'`.

- [ ] **Step 3: Create `src/engines/sweeper/sweeper_pipeline.py`**

```python
import logging
import mlflow
from datetime import date, datetime, time, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType,
    StructField, StructType, TimestampType,
)

_log = logging.getLogger(__name__)

_EDI_SENDER_ID = "AHOLD_NL"
_EDI_DEADLINE = time(8, 15)

_EXCEPTION_QUEUE_SCHEMA = StructType([
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

_PO_AUDIT_SCHEMA = StructType([
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
        spark.createDataFrame(rows, schema=_EXCEPTION_QUEUE_SCHEMA)
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
```

- [ ] **Step 4: Run tests to verify all 5 pass**

```bash
pytest tests/unit/test_sweeper_pipeline.py -v
```

Expected: 5/5 PASS.

- [ ] **Step 5: Commit**

```bash
git add src/engines/sweeper/sweeper_pipeline.py tests/unit/test_sweeper_pipeline.py
git commit -m "feat: add sweeper_pipeline _entry_load_exceptions + _compute_deviation + tests"
```

---

## Task 2: `_entry_finalize` — tests + implementation

**Files:**
- Modify: `src/engines/sweeper/sweeper_pipeline.py` (add helpers + replace `_entry_finalize` stub)
- Modify: `tests/unit/test_sweeper_pipeline.py` (add 4 tests)

`_classify_finalize_row` and `_build_edi_lines_by_vendor` are pure functions added here so their tests fail first before implementation.

- [ ] **Step 1: Add 4 failing tests to `tests/unit/test_sweeper_pipeline.py`**

Append to the existing file:

```python
# ---------------------------------------------------------------------------
# Task 2 — _classify_finalize_row
# ---------------------------------------------------------------------------

def test_classify_finalize_auto_approve():
    from engines.sweeper.sweeper_pipeline import _classify_finalize_row

    needs_bapi, status = _classify_finalize_row({
        "sweeper_action": "AUTO_APPROVE",
        "manager_decision": None,
    })
    assert needs_bapi is True
    assert status == "SWEEPER_AUTO_APPROVED"


def test_classify_finalize_force_approves_unresolved():
    from engines.sweeper.sweeper_pipeline import _classify_finalize_row

    # ESCALATE with no manager decision → force-approve at deadline
    needs_bapi, status = _classify_finalize_row({
        "sweeper_action": "ESCALATE",
        "manager_decision": None,
    })
    assert needs_bapi is True
    assert status == "FORCE_APPROVED"


def test_classify_finalize_skips_rejected():
    from engines.sweeper.sweeper_pipeline import _classify_finalize_row

    needs_bapi, status = _classify_finalize_row({
        "sweeper_action": "ESCALATE",
        "manager_decision": "REJECTED",
    })
    assert needs_bapi is False
    assert status == "MANAGER_REJECTED"


# ---------------------------------------------------------------------------
# Task 2 — _build_edi_lines_by_vendor
# ---------------------------------------------------------------------------

def test_build_edi_lines_by_vendor_groups_correctly():
    from engines.sweeper.sweeper_pipeline import _build_edi_lines_by_vendor

    rows = [
        {
            "po_number": "4500001234", "recommended_qty": 50,
            "ean_barcode": "8710624212483", "vendor_id": "V001",
            "unit_cost": 2.5, "transit_days": 2,
        },
        {
            "po_number": "4500001235", "recommended_qty": 30,
            "ean_barcode": "8710624212490", "vendor_id": "V002",
            "unit_cost": 1.8, "transit_days": 1,
        },
        {
            "po_number": "4500001236", "recommended_qty": 20,
            "ean_barcode": "8710624212507", "vendor_id": "V001",
            "unit_cost": 2.5, "transit_days": 2,
        },
    ]
    result = _build_edi_lines_by_vendor(rows)

    assert set(result.keys()) == {"V001", "V002"}
    assert len(result["V001"]) == 2      # two SKUs for same vendor
    assert len(result["V002"]) == 1
    assert result["V001"][0].quantity == 50
    assert result["V001"][0].ean_barcode == "8710624212483"
    assert result["V001"][1].line_number == 2  # line numbers increment per vendor
    assert result["V002"][0].quantity == 30
```

- [ ] **Step 2: Run tests to verify new 4 fail**

```bash
pytest tests/unit/test_sweeper_pipeline.py -v
```

Expected: 5 PASS, 4 FAIL with `ImportError: cannot import name '_classify_finalize_row' from 'engines.sweeper.sweeper_pipeline'`.

- [ ] **Step 3: Add helpers and replace `_entry_finalize` stub in `src/engines/sweeper/sweeper_pipeline.py`**

Insert before `_entry_load_exceptions` (after `_minutes_to_edi_deadline`):

```python
def _classify_finalize_row(row: dict) -> tuple[bool, str]:
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


def _build_edi_lines_by_vendor(rows: list[dict]) -> dict[str, list]:
    """Group consolidated po_audit+registry rows into EDI lines keyed by vendor_id."""
    from integration.edi.edi_850_generator import EDI850Line

    result: dict[str, list] = {}
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
```

Then replace `def _entry_finalize() -> None:\n    pass  # implemented in Task 2` with:

```python
def _entry_finalize() -> None:
    from databricks.sdk.runtime import dbutils
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
    audit_rows: list[dict] = []
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
        audit_df = (
            spark.createDataFrame(audit_rows, schema=_PO_AUDIT_SCHEMA)
            .withColumn("_created_at", F.current_timestamp())
        )
        audit_df.write.format("delta").mode("append").saveAsTable(
            "gold.replenishment.po_audit"
        )

    # EDI consolidation — all approved POs for today (Phase 3B AUTO_APPROVED + Sweeper)
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
```

- [ ] **Step 4: Run all tests**

```bash
pytest tests/unit/test_sweeper_pipeline.py -v
```

Expected: 9/9 PASS.

- [ ] **Step 5: Commit**

```bash
git add src/engines/sweeper/sweeper_pipeline.py tests/unit/test_sweeper_pipeline.py
git commit -m "feat: implement _entry_finalize (BAPI loop + EDI consolidation) + tests"
```

---

## Task 3: DAB wiring

**Files:**
- Modify: `resources/jobs/morning_pipeline.yml`
- Create: `resources/jobs/sweeper_finalize.yml`
- Modify: `databricks.yml`
- Modify: `setup.py`

No new tests — DAB YAML correctness is validated by `databricks bundle validate`.

- [ ] **Step 1: Add `sweeper_load_exceptions` task to `resources/jobs/morning_pipeline.yml`**

At the end of the `tasks:` list, after the `bapi_po_create` task block, append:

```yaml
  - task_key: sweeper_load_exceptions
    depends_on:
      - task_key: bapi_po_create
    python_wheel_task:
      package_name: ahold_freshness_poc
      entry_point: sweeper_load_exceptions
    timeout_seconds: 900
```

The morning_pipeline will now have 14 tasks total.

- [ ] **Step 2: Create `resources/jobs/sweeper_finalize.yml`**

```yaml
name: sweeper-finalize-${bundle.target}
schedule:
  quartz_cron_expression: "0 10 8 * * ?"
  timezone_id: "Europe/Amsterdam"
  pause_status: ${var.schedule_enabled == "true" ? "UNPAUSED" : "PAUSED"}
tasks:
  - task_key: sweeper_finalize
    python_wheel_task:
      package_name: ahold_freshness_poc
      entry_point: sweeper_finalize
    timeout_seconds: 1800
```

Quartz cron `"0 10 8 * * ?"` fires daily at 08:10:00 AM. `timezone_id: "Europe/Amsterdam"` handles CET/CEST automatically.

- [ ] **Step 3: Add `sweeper_finalize` job reference to `databricks.yml`**

In the `resources.jobs` section add after the last existing entry:

```yaml
    sweeper_finalize:
      source: resources/jobs/sweeper_finalize.yml
```

The complete `resources.jobs` section becomes:

```yaml
  jobs:
    lakeflow_trigger:
      source: resources/jobs/lakeflow_trigger.yml
    phantom_stock_train:
      source: resources/jobs/phantom_stock_train.yml
    demand_forecast_train:
      source: resources/jobs/demand_forecast_train.yml
    morning_pipeline:
      source: resources/jobs/morning_pipeline.yml
    sweeper_finalize:
      source: resources/jobs/sweeper_finalize.yml
```

Note: `phantom_stock_score` was removed in Phase 3B (consolidated into `morning_pipeline`). `demand_forecast_train` was added in Phase 3A.

- [ ] **Step 4: Add 2 console_scripts to `setup.py`**

After the last Phase 3B entry in the `entry_points` dict, add:

```python
            # Phase 4A — Sweeper + EDI
            "sweeper_load_exceptions=engines.sweeper.sweeper_pipeline:_entry_load_exceptions",
            "sweeper_finalize=engines.sweeper.sweeper_pipeline:_entry_finalize",
```

- [ ] **Step 5: Run full test suite to verify no regressions**

```bash
pytest tests/unit/test_sweeper_pipeline.py tests/unit/test_freshness_orchestrator.py -v
```

Expected: all tests PASS (18 total across both files).

- [ ] **Step 6: Commit**

```bash
git add resources/jobs/morning_pipeline.yml resources/jobs/sweeper_finalize.yml databricks.yml setup.py
git commit -m "feat: wire Sweeper + EDI into DAB — sweeper_finalize cron + morning_pipeline task"
```
