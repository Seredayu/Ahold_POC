import json
import os
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from src.medallion.gold.exception_queue import ExceptionQueueSchema, build_exception_id

router = APIRouter()

_SELECT_COLS = (
    "werks, unified_sku_id, recommended_qty, transit_to_life_ratio, "
    "quantity_deviation_pct, sweeper_action, exception_type, manager_decision, "
    "manager_id, decision_timestamp, override_reason, override_qty, "
    "shap_values, _loaded_at"
)


class ExceptionItem(BaseModel):
    exception_id: str
    werks: str
    unified_sku_id: str
    recommended_qty: int
    transit_to_life_ratio: float
    quantity_deviation_pct: float
    sweeper_action: str
    exception_type: Optional[str] = None
    manager_decision: Optional[str] = None
    manager_id: Optional[str] = None
    decision_timestamp: Optional[str] = None
    override_reason: Optional[str] = None
    override_qty: Optional[int] = None
    shap_values: Optional[dict[str, float]] = None
    status: str  # "PENDING" | "APPROVED" | "BLOCKED"


class ApproveRequest(BaseModel):
    override_qty: Optional[int] = None
    override_reason: Optional[str] = None


def get_databricks_connection():
    import databricks.sql
    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    http_path = os.environ.get("DATABRICKS_HTTP_PATH")
    if not host or not token or not http_path:
        raise RuntimeError("DATABRICKS_HOST, DATABRICKS_TOKEN, and DATABRICKS_HTTP_PATH must be set")
    conn = databricks.sql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=token,
    )
    return conn


def _parse_exception_id(exception_id: str) -> tuple[str, str, str]:
    parts = exception_id.split("|", maxsplit=2)
    if len(parts) != 3:
        raise HTTPException(status_code=422, detail=f"Invalid exception_id format: {exception_id!r}. Expected 'werks|unified_sku_id|loaded_at'.")
    return parts[0], parts[1], parts[2]


def _decision_to_status(manager_decision: Optional[str]) -> str:
    if manager_decision == "APPROVED":
        return "APPROVED"
    if manager_decision == "REJECTED":
        return "BLOCKED"
    return "PENDING"


def _row_to_exception_item(row) -> ExceptionItem:
    (
        werks, unified_sku_id, recommended_qty, transit_to_life_ratio,
        quantity_deviation_pct, sweeper_action, exception_type, manager_decision,
        manager_id, decision_timestamp, override_reason, override_qty,
        shap_values_raw, loaded_at,
    ) = row

    shap_values = None
    if shap_values_raw is not None:
        if isinstance(shap_values_raw, str):
            shap_values = json.loads(shap_values_raw)
        else:
            shap_values = shap_values_raw

    exception_id = build_exception_id(werks, unified_sku_id, loaded_at)

    return ExceptionItem(
        exception_id=exception_id,
        werks=werks,
        unified_sku_id=unified_sku_id,
        recommended_qty=recommended_qty,
        transit_to_life_ratio=transit_to_life_ratio,
        quantity_deviation_pct=quantity_deviation_pct,
        sweeper_action=sweeper_action,
        exception_type=exception_type,
        manager_decision=manager_decision,
        manager_id=manager_id,
        decision_timestamp=decision_timestamp,
        override_reason=override_reason,
        override_qty=override_qty,
        shap_values=shap_values,
        status=_decision_to_status(manager_decision),
    )


@router.get("/", response_model=list[ExceptionItem])
def list_exceptions(store_id: Optional[str] = None, status: str = "PENDING") -> list[ExceptionItem]:
    conn = get_databricks_connection()
    cursor = conn.cursor()
    try:
        # Frontend uses "BLOCKED" but DB stores "REJECTED"
        db_status = "REJECTED" if status == "BLOCKED" else status

        base_sql = (
            f"SELECT {_SELECT_COLS} "
            "FROM gold.replenishment.exception_queue "
            "WHERE sweeper_action = 'ESCALATE' "
            "AND COALESCE(manager_decision, 'PENDING') = ? "
        )
        params: list = [db_status]

        if store_id is not None:
            base_sql += "AND werks = ? "
            params.append(store_id)

        base_sql += "ORDER BY quantity_deviation_pct DESC"

        cursor.execute(base_sql, params)
        rows = cursor.fetchall()
        return [_row_to_exception_item(row) for row in rows]
    finally:
        cursor.close()
        conn.close()


@router.get("/{exception_id}", response_model=ExceptionItem)
def get_exception(exception_id: str) -> ExceptionItem:
    werks, unified_sku_id, loaded_at = _parse_exception_id(exception_id)
    conn = get_databricks_connection()
    cursor = conn.cursor()
    try:
        sql = (
            f"SELECT {_SELECT_COLS} "
            "FROM gold.replenishment.exception_queue "
            "WHERE werks = ? AND unified_sku_id = ? AND _loaded_at = ?"
        )
        cursor.execute(sql, [werks, unified_sku_id, loaded_at])
        row = cursor.fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail=f"Exception {exception_id} not found")
        return _row_to_exception_item(row)
    finally:
        cursor.close()
        conn.close()


@router.post("/{exception_id}/approve")
def approve_exception(exception_id: str, body: ApproveRequest, request: Request) -> dict:
    werks, unified_sku_id, loaded_at = _parse_exception_id(exception_id)
    # POC: reads manager identity from client-supplied header.
    # Production: replace with Azure AD claim from X-MS-CLIENT-PRINCIPAL token.
    manager_id = request.headers.get("X-Manager-Id")

    decision_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    conn = get_databricks_connection()
    cursor = conn.cursor()
    try:
        sql = (
            "UPDATE gold.replenishment.exception_queue "
            "SET manager_decision='APPROVED', manager_id=?, decision_timestamp=?, "
            "override_reason=?, override_qty=? "
            "WHERE werks=? AND unified_sku_id=? AND _loaded_at=?"
        )
        cursor.execute(sql, [manager_id, decision_ts, body.override_reason, body.override_qty, werks, unified_sku_id, loaded_at])

        return {
            "exception_id": exception_id,
            "status": "APPROVED",
            "manager_id": manager_id,
            "decision_timestamp": decision_ts,
        }
    finally:
        cursor.close()
        conn.close()


@router.post("/{exception_id}/reject")
def reject_exception(exception_id: str, request: Request) -> dict:
    werks, unified_sku_id, loaded_at = _parse_exception_id(exception_id)
    # POC: reads manager identity from client-supplied header.
    # Production: replace with Azure AD claim from X-MS-CLIENT-PRINCIPAL token.
    manager_id = request.headers.get("X-Manager-Id")

    decision_ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    conn = get_databricks_connection()
    cursor = conn.cursor()
    try:
        sql = (
            "UPDATE gold.replenishment.exception_queue "
            "SET manager_decision='REJECTED', manager_id=?, decision_timestamp=? "
            "WHERE werks=? AND unified_sku_id=? AND _loaded_at=?"
        )
        cursor.execute(sql, [manager_id, decision_ts, werks, unified_sku_id, loaded_at])

        return {
            "exception_id": exception_id,
            "status": "BLOCKED",
            "manager_id": manager_id,
            "decision_timestamp": decision_ts,
        }
    finally:
        cursor.close()
        conn.close()
