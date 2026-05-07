# Phase 4A: Sweeper + EDI Design Spec

**Date:** 2026-05-07  
**Phase:** 4A (Weeks 12–13)  
**Builds on:** Phase 3B (Freshness Orchestrator, `gold.replenishment.order_recommendations`, `gold.replenishment.po_audit`)

---

## Goal

Wire the existing `SweeperStateMachine` and `EDI850Generator` stubs into the morning pipeline:
- Load PENDING_REVIEW exceptions into a queryable queue for store manager review
- Finalize all orders at 08:10 AM (manager-approved + force-approved + Sweeper auto-approved)
- Generate one EDI 850 per supplier covering every approved PO for the day

## Architecture

Two new Databricks Asset Bundle entry points, two DAB job changes, one new Delta table.

```
05:00  morning_pipeline starts
06:00  bapi_po_create completes
       └─ po_audit has PO numbers for AUTO_APPROVED rows (Phase 3B)
06:15  sweeper_load_exceptions (task in morning_pipeline, depends_on bapi_po_create)
       reads  gold.replenishment.order_recommendations  WHERE approval_status=PENDING_REVIEW
       joins  feature_store.demand.m4_probabilistic     for p50 (quantity_deviation_pct)
       runs   SweeperStateMachine.process_exception()   per row
       writes gold.replenishment.exception_queue        (overwrite)
       logs   MLflow metrics

06:15–08:10  store managers review ESCALATE rows via React Field App (Phase 4B)
             FastAPI writes manager_decision / manager_id / decision_timestamp back to
             exception_queue (UPDATE — Phase 4B concern, not this spec)

08:10  sweeper_finalize (separate cron job, independent of morning_pipeline)
       reads  gold.replenishment.exception_queue
       calls  POClient.create_purchase_order() for approved rows
       appends gold.replenishment.po_audit
       reads  ALL po_audit rows for today  (Phase 3B + Sweeper)
       joins  silver.master.unified_sku_registry  for ean_barcode, vendor_id, unit_cost
       groups by vendor_id → one EDI850Generator.generate() call per vendor
       logs   MLflow metrics
```

## New Delta Table: `gold.replenishment.exception_queue`

Written by `sweeper_load_exceptions` (overwrite). Updated by FastAPI (manager decisions — Phase 4B).

| Field | Type | Nullable | Description |
|---|---|---|---|
| `werks` | string | N | SAP plant code |
| `unified_sku_id` | string | N | Rosetta Stone SKU ID |
| `recommended_qty` | int | N | From order_recommendations |
| `transit_to_life_ratio` | double | N | From order_recommendations |
| `order_value` | double | N | From order_recommendations |
| `quantity_deviation_pct` | double | N | `abs(recommended_qty − p50) / p50`; 0.0 if p50 ≤ 0 |
| `sweeper_action` | string | N | AUTO_APPROVE / ESCALATE / BLOCK |
| `manager_decision` | string | Y | APPROVED / REJECTED / null |
| `manager_id` | string | Y | Azure AD object ID of deciding manager |
| `decision_timestamp` | timestamp | Y | When manager acted |
| `override_reason` | string | Y | Free-text; optional |
| `_loaded_at` | timestamp | N | `current_timestamp()` at load time |

Partition: none (50 stores × ~10 exceptions/store = ~500 rows/day at POC scale).

**Assumed fields in `silver.master.unified_sku_registry`:** `ean_barcode` (EAN-13 barcode string), `vendor_id` (EDI receiver ID string), `transit_days` (int). These are standard master data fields from the Rosetta Stone mapping. If absent, `sweeper_finalize` will fail the join and raise before EDI generation.

## New File: `src/engines/sweeper/sweeper_pipeline.py`

### `_entry_load_exceptions()`

```
spark = SparkSession.getActiveSession()

# Read PENDING_REVIEW recommendations
pending = spark.table("gold.replenishment.order_recommendations")
         .filter(col("approval_status") == "PENDING_REVIEW")

# Join M4 for p50 (needed for quantity_deviation_pct)
m4 = spark.table("feature_store.demand.m4_probabilistic")
     .select("werks", "unified_sku_id", "p50")

joined = pending.join(m4, on=["werks", "unified_sku_id"], how="left")

# Compute quantity_deviation_pct, build exception dicts, run Sweeper
# Write exception_queue (overwrite)
# Log MLflow: escalated_count, sweeper_auto_approved_count, sweeper_blocked_count
```

`quantity_deviation_pct` computation:
- `p50 > 0`: `abs(recommended_qty - p50) / p50`
- `p50 ≤ 0`: `0.0` (treat as no deviation — avoids division by zero)

`SweeperStateMachine` is instantiated fresh per run. Each row passed to `process_exception()` as a dict with keys: `transit_to_life_ratio`, `quantity_deviation_pct`, `minutes_to_deadline` (computed from wall clock vs `EDI_DEADLINE = time(8, 15)`).

Schema for exception_queue write uses explicit `StructType` (same pattern as Phase 3B schemas).

MLflow metrics logged:
- `pending_review_input_count` — rows read from order_recommendations
- `sweeper_auto_approved_count` — rows with sweeper_action=AUTO_APPROVE
- `sweeper_escalated_count` — rows with sweeper_action=ESCALATE
- `sweeper_blocked_count` — rows with sweeper_action=BLOCK

### `_entry_finalize()`

```
spark = SparkSession.getActiveSession()
token = dbutils.secrets.get(scope="sap-btp", key="ai-core-token")
endpoint = dbutils.secrets.get(scope="sap-btp", key="ai-core-endpoint")
conn_str = dbutils.secrets.get(scope="azure-storage", key="connection-string")

queue = spark.table("gold.replenishment.exception_queue").toPandas()

# Classify rows
BAPI_NEEDED rows:
  sweeper_action == AUTO_APPROVE
  sweeper_action == ESCALATE AND manager_decision == APPROVED
  sweeper_action == ESCALATE AND manager_decision IS NULL  → force_approved=True

BLOCKED rows:
  sweeper_action == BLOCK
  sweeper_action == ESCALATE AND manager_decision == REJECTED

# Call POClient per BAPI_NEEDED row, build audit_rows
# Append audit_rows to gold.replenishment.po_audit

# EDI consolidation
today = date.today().isoformat()
po_audit = spark.table("gold.replenishment.po_audit")
           .filter(col("_created_at").cast("date") == today)
           .filter(col("bapi_status") == "ok")

sku_registry = spark.table("silver.master.unified_sku_registry")
               .select("werks", "unified_sku_id", "ean_barcode", "vendor_id", "unit_cost", "transit_days")

consolidated = po_audit.join(sku_registry, on=["werks", "unified_sku_id"], how="left")
               .toPandas()

# Group by vendor_id → one EDI850Generator.generate() per vendor
# requested_ship_date = (date.today() + timedelta(days=transit_days)).strftime("%Y%m%d") per line

# Log MLflow metrics
```

If `exception_queue` is empty (load_exceptions task failed or was skipped): log warning, skip BAPI loop, still attempt EDI consolidation from po_audit (Phase 3B rows cover AUTO_APPROVED orders). Do not raise — partial EDI is better than no EDI.

If `po_audit` has zero `bapi_status=ok` rows after consolidation: raise `RuntimeError("No approved POs found for EDI 850 — possible pipeline failure")`. This fails the DAB task and triggers an alert.

MLflow metrics logged:
- `force_approved_count` — ESCALATE rows with no manager decision
- `manager_approved_count` — ESCALATE rows with APPROVED decision
- `manager_rejected_count` — ESCALATE rows with REJECTED decision
- `sweeper_bapi_created_count` — successful BAPI calls in this task
- `sweeper_bapi_error_count` — failed BAPI calls in this task
- `edi_files_generated` — number of EDI 850 files uploaded to Blob
- `total_po_lines` — total line items across all EDI files

## DAB Changes

### `resources/jobs/morning_pipeline.yml` — add one task

```yaml
- task_key: sweeper_load_exceptions
  depends_on:
    - task_key: bapi_po_create
  python_wheel_task:
    package_name: ahold_freshness_poc
    entry_point: sweeper_load_exceptions
  timeout_seconds: 900
```

### New `resources/jobs/sweeper_finalize.yml`

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

### `databricks.yml` — add job reference

```yaml
sweeper_finalize:
  source: resources/jobs/sweeper_finalize.yml
```

### `setup.py` — add two console_scripts

```python
"sweeper_load_exceptions=engines.sweeper.sweeper_pipeline:_entry_load_exceptions",
"sweeper_finalize=engines.sweeper.sweeper_pipeline:_entry_finalize",
```

## Unchanged Files

The following existing stubs are used as-is with no modifications:

- `src/engines/sweeper/state_machine.py` — `SweeperStateMachine`, `SweeperState`
- `src/engines/sweeper/decision_rules.py` — `DECISION_RULES`, `SweeperAction`, `ExceptionThresholds`
- `src/integration/edi/edi_850_generator.py` — `EDI850Generator`, `EDI850Line`
- `src/engines/freshness/po_client.py` — `POClient` (reused for Sweeper BAPI calls)

## Tests: `tests/unit/test_sweeper_pipeline.py`

Six tests, all lazy imports inside test functions.

| Test | Setup | Assertion |
|---|---|---|
| `test_load_exceptions_auto_approve_low_deviation` | Row with `quantity_deviation_pct=0.10`, `transit_to_life_ratio=0.3` | `sweeper_action == AUTO_APPROVE` |
| `test_load_exceptions_escalate_high_deviation` | Row with `quantity_deviation_pct=0.40`, `transit_to_life_ratio=0.3` | `sweeper_action == ESCALATE` |
| `test_load_exceptions_block_high_ttl` | Row with `transit_to_life_ratio=0.8` | `sweeper_action == BLOCK` |
| `test_finalize_calls_bapi_for_auto_approve` | exception_queue row with `sweeper_action=AUTO_APPROVE`; mock POClient returns `{"PO_NUMBER": "4500001234"}` | POClient called once; po_audit row appended with `bapi_status=ok` |
| `test_finalize_force_approves_unresolved` | exception_queue row with `sweeper_action=ESCALATE`, `manager_decision=None` | POClient called; po_audit row has `approval_status=FORCE_APPROVED` |
| `test_finalize_generates_edi_per_vendor` | po_audit has two rows with `vendor_id=V001` and `vendor_id=V002`; mock EDI850Generator | `EDI850Generator.generate()` called twice with correct `receiver_id` |

## Constraints (from CLAUDE.md)

- **Sweeper must be deterministic**: all decision logic remains in `decision_rules.py` as Pydantic models. `sweeper_pipeline.py` contains only I/O wiring — no decision logic.
- **Clean Core mandate**: BAPI calls via `POClient` only — no direct SAP table writes.
- **EDI transport**: `EDI850Generator` uploads to Azure Blob (`gold-edi-outbound` container) only. Azure Logic Apps picks up files. Never route EDI directly from Databricks to external SFTP.
- **MILP solver abstraction**: `SolverInterface` unchanged — Phase 4A does not touch solver code.
