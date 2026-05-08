"""Unit tests for the exceptions router.

No pyspark imports anywhere in this file.
Databricks connection is fully mocked.
"""
import json
import os
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

# ---------------------------------------------------------------------------
# App import — must happen after the patch context is available.  We import
# lazily inside each test so patches on get_databricks_connection work.
# ---------------------------------------------------------------------------


def _make_app():
    from src.api.main import app
    return app


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_row(
    werks="AH01",
    unified_sku_id="SKU123456",
    recommended_qty=100,
    transit_to_life_ratio=0.75,
    quantity_deviation_pct=0.25,
    sweeper_action="ESCALATE",
    exception_type="High Deviation",
    manager_decision=None,
    manager_id=None,
    decision_timestamp=None,
    override_reason=None,
    override_qty=None,
    shap_values=None,
    loaded_at="20260508T061500Z",
):
    return (
        werks,
        unified_sku_id,
        recommended_qty,
        transit_to_life_ratio,
        quantity_deviation_pct,
        sweeper_action,
        exception_type,
        manager_decision,
        manager_id,
        decision_timestamp,
        override_reason,
        override_qty,
        shap_values,
        loaded_at,
    )


def _mock_cursor(rows=None, fetchone_row=None):
    cursor = MagicMock()
    cursor.fetchall.return_value = rows or []
    cursor.fetchone.return_value = fetchone_row
    return cursor


# ---------------------------------------------------------------------------
# Tests: list_exceptions
# ---------------------------------------------------------------------------

class TestListExceptions:
    def test_returns_two_items(self):
        row1 = _make_row(werks="AH01", unified_sku_id="SKU001", loaded_at="20260508T061500Z")
        row2 = _make_row(werks="AH02", unified_sku_id="SKU002", loaded_at="20260508T061501Z")
        cursor = _mock_cursor(rows=[row1, row2])

        with patch("src.api.routers.exceptions.get_databricks_connection", return_value=cursor):
            client = TestClient(_make_app())
            resp = client.get("/exceptions/")

        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2

    def test_exception_id_format(self):
        row = _make_row(werks="AH01", unified_sku_id="SKU123456", loaded_at="20260508T061500Z")
        cursor = _mock_cursor(rows=[row])

        with patch("src.api.routers.exceptions.get_databricks_connection", return_value=cursor):
            client = TestClient(_make_app())
            resp = client.get("/exceptions/")

        item = resp.json()[0]
        assert item["exception_id"] == "AH01|SKU123456|20260508T061500Z"

    def test_status_mapping_pending(self):
        row = _make_row(manager_decision=None)
        cursor = _mock_cursor(rows=[row])

        with patch("src.api.routers.exceptions.get_databricks_connection", return_value=cursor):
            client = TestClient(_make_app())
            resp = client.get("/exceptions/")

        assert resp.json()[0]["status"] == "PENDING"

    def test_status_mapping_approved(self):
        row = _make_row(manager_decision="APPROVED")
        cursor = _mock_cursor(rows=[row])

        with patch("src.api.routers.exceptions.get_databricks_connection", return_value=cursor):
            client = TestClient(_make_app())
            resp = client.get("/exceptions/")

        assert resp.json()[0]["status"] == "APPROVED"

    def test_status_mapping_rejected_becomes_blocked(self):
        row = _make_row(manager_decision="REJECTED")
        cursor = _mock_cursor(rows=[row])

        with patch("src.api.routers.exceptions.get_databricks_connection", return_value=cursor):
            client = TestClient(_make_app())
            resp = client.get("/exceptions/")

        assert resp.json()[0]["status"] == "BLOCKED"

    def test_shap_values_parsed_from_json_string(self):
        shap_str = '{"demand_velocity": 0.15, "day_of_week": -0.05}'
        row = _make_row(shap_values=shap_str)
        cursor = _mock_cursor(rows=[row])

        with patch("src.api.routers.exceptions.get_databricks_connection", return_value=cursor):
            client = TestClient(_make_app())
            resp = client.get("/exceptions/")

        item = resp.json()[0]
        assert item["shap_values"] == {"demand_velocity": 0.15, "day_of_week": -0.05}

    def test_shap_values_passed_through_as_dict(self):
        shap_dict = {"demand_velocity": 0.15}
        row = _make_row(shap_values=shap_dict)
        cursor = _mock_cursor(rows=[row])

        with patch("src.api.routers.exceptions.get_databricks_connection", return_value=cursor):
            client = TestClient(_make_app())
            resp = client.get("/exceptions/")

        assert resp.json()[0]["shap_values"] == shap_dict

    def test_store_id_filter_passes_werks_param(self):
        cursor = _mock_cursor(rows=[])

        with patch("src.api.routers.exceptions.get_databricks_connection", return_value=cursor):
            client = TestClient(_make_app())
            resp = client.get("/exceptions/?store_id=AH01")

        assert resp.status_code == 200
        call_args = cursor.execute.call_args
        sql, params = call_args[0]
        assert "AND werks = ?" in sql
        assert "AH01" in params

    def test_no_store_id_omits_werks_clause(self):
        cursor = _mock_cursor(rows=[])

        with patch("src.api.routers.exceptions.get_databricks_connection", return_value=cursor):
            client = TestClient(_make_app())
            resp = client.get("/exceptions/")

        call_args = cursor.execute.call_args
        sql, params = call_args[0]
        assert "AND werks = ?" not in sql


# ---------------------------------------------------------------------------
# Tests: approve_exception
# ---------------------------------------------------------------------------

class TestApproveException:
    def test_returns_approved_status(self):
        cursor = _mock_cursor()

        with patch("src.api.routers.exceptions.get_databricks_connection", return_value=cursor):
            client = TestClient(_make_app())
            resp = client.post(
                "/exceptions/AH01|SKU123456|20260508T061500Z/approve",
                json={"override_qty": 90, "override_reason": "Manual check"},
                headers={"X-Manager-Id": "mgr-aad-001"},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "APPROVED"
        assert data["exception_id"] == "AH01|SKU123456|20260508T061500Z"
        assert data["manager_id"] == "mgr-aad-001"
        assert "decision_timestamp" in data

    def test_update_sql_called_with_correct_params(self):
        cursor = _mock_cursor()

        with patch("src.api.routers.exceptions.get_databricks_connection", return_value=cursor):
            client = TestClient(_make_app())
            client.post(
                "/exceptions/AH01|SKU123456|20260508T061500Z/approve",
                json={"override_qty": 90, "override_reason": "Manual check"},
                headers={"X-Manager-Id": "mgr-aad-001"},
            )

        call_args = cursor.execute.call_args
        sql, params = call_args[0]
        assert "manager_decision='APPROVED'" in sql
        assert "mgr-aad-001" in params
        assert "AH01" in params
        assert "SKU123456" in params
        assert "20260508T061500Z" in params


# ---------------------------------------------------------------------------
# Tests: reject_exception
# ---------------------------------------------------------------------------

class TestRejectException:
    def test_returns_blocked_status(self):
        cursor = _mock_cursor()

        with patch("src.api.routers.exceptions.get_databricks_connection", return_value=cursor):
            client = TestClient(_make_app())
            resp = client.post(
                "/exceptions/AH01|SKU123456|20260508T061500Z/reject",
                headers={"X-Manager-Id": "mgr-aad-002"},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "BLOCKED"
        assert data["exception_id"] == "AH01|SKU123456|20260508T061500Z"
        assert data["manager_id"] == "mgr-aad-002"

    def test_update_sql_called_with_rejected(self):
        cursor = _mock_cursor()

        with patch("src.api.routers.exceptions.get_databricks_connection", return_value=cursor):
            client = TestClient(_make_app())
            client.post(
                "/exceptions/AH01|SKU123456|20260508T061500Z/reject",
                headers={"X-Manager-Id": "mgr-aad-002"},
            )

        call_args = cursor.execute.call_args
        sql, params = call_args[0]
        assert "manager_decision='REJECTED'" in sql
        assert "mgr-aad-002" in params


# ---------------------------------------------------------------------------
# Tests: get_databricks_connection env var guard
# ---------------------------------------------------------------------------

class TestGetDatabricksConnection:
    def test_raises_when_host_missing(self):
        env = {k: v for k, v in os.environ.items()
               if k not in ("DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_HTTP_PATH")}

        with patch.dict(os.environ, env, clear=True):
            from src.api.routers.exceptions import get_databricks_connection
            with pytest.raises(RuntimeError, match="DATABRICKS_HOST, DATABRICKS_TOKEN, and DATABRICKS_HTTP_PATH must be set"):
                get_databricks_connection()

    def test_raises_when_token_missing(self):
        patched = {k: v for k, v in os.environ.items()
                   if k not in ("DATABRICKS_TOKEN", "DATABRICKS_HTTP_PATH")}
        patched["DATABRICKS_HOST"] = "https://adb-123.azuredatabricks.net"

        with patch.dict(os.environ, patched, clear=True):
            from src.api.routers.exceptions import get_databricks_connection
            with pytest.raises(RuntimeError, match="DATABRICKS_HOST, DATABRICKS_TOKEN, and DATABRICKS_HTTP_PATH must be set"):
                get_databricks_connection()

    def test_raises_when_http_path_missing(self):
        patched = {k: v for k, v in os.environ.items() if k != "DATABRICKS_HTTP_PATH"}
        patched["DATABRICKS_HOST"] = "https://adb-123.azuredatabricks.net"
        patched["DATABRICKS_TOKEN"] = "dapi-test-token"
        patched.pop("DATABRICKS_HTTP_PATH", None)

        with patch.dict(os.environ, patched, clear=True):
            from src.api.routers.exceptions import get_databricks_connection
            with pytest.raises(RuntimeError, match="DATABRICKS_HOST, DATABRICKS_TOKEN, and DATABRICKS_HTTP_PATH must be set"):
                get_databricks_connection()


# ---------------------------------------------------------------------------
# Tests: get_exception (GET /{exception_id})
# ---------------------------------------------------------------------------

class TestGetException:
    def test_get_exception_found(self):
        row = _make_row(werks="AH01", unified_sku_id="SKU123456", loaded_at="20260508T061500Z")
        cursor = _mock_cursor(fetchone_row=row)

        with patch("src.api.routers.exceptions.get_databricks_connection", return_value=cursor):
            client = TestClient(_make_app())
            resp = client.get("/exceptions/AH01|SKU123456|20260508T061500Z")

        assert resp.status_code == 200
        data = resp.json()
        assert data["exception_id"] == "AH01|SKU123456|20260508T061500Z"
        assert data["status"] == "PENDING"

    def test_get_exception_not_found(self):
        cursor = _mock_cursor(fetchone_row=None)

        with patch("src.api.routers.exceptions.get_databricks_connection", return_value=cursor):
            client = TestClient(_make_app())
            resp = client.get("/exceptions/AH01|SKU_MISSING|20260508T000000Z")

        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Tests: list_exceptions BLOCKED→REJECTED translation
# ---------------------------------------------------------------------------

class TestListExceptionsStatusTranslation:
    def test_blocked_status_passes_rejected_to_sql(self):
        cursor = _mock_cursor(rows=[])

        with patch("src.api.routers.exceptions.get_databricks_connection", return_value=cursor):
            client = TestClient(_make_app())
            resp = client.get("/exceptions/?status=BLOCKED")

        assert resp.status_code == 200
        call_args = cursor.execute.call_args
        sql, params = call_args[0]
        assert "REJECTED" in params
        assert "BLOCKED" not in params
