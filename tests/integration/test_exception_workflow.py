"""
Integration smoke test for the exception queue approval workflow.
Uses FastAPI TestClient with a monkeypatched get_databricks_connection
to simulate a full manager review cycle without a real Databricks instance.
"""
import json
import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient
from src.api.main import app

EXCEPTION_ROWS = [
    (
        "AH01", "SKU123456", 45, 0.35, 0.38,
        "ESCALATE", "High Deviation",
        None, None, None, None, None,
        json.dumps({"demand_velocity": 0.15, "freshness_index": -0.05}),
        "20260508T061500Z",
    ),
    (
        "AH01", "SKU789012", 20, 0.48, 0.26,
        "ESCALATE", "High Transit-to-Life",
        None, None, None, None, None,
        json.dumps({"transit_to_life_ratio": 0.12, "stock_out_risk": 0.08}),
        "20260508T061500Z",
    ),
]


def make_mock_cursor(rows=None, fetchone_row=None):
    cursor = MagicMock()
    cursor.fetchall.return_value = rows or []
    cursor.fetchone.return_value = fetchone_row
    return cursor


@pytest.fixture
def client():
    return TestClient(app)


class TestExceptionWorkflow:
    def test_list_pending_exceptions(self, client):
        """List returns all PENDING exceptions sorted by deviation_pct desc."""
        cursor = make_mock_cursor(rows=EXCEPTION_ROWS)
        with patch("src.api.routers.exceptions.get_databricks_connection", return_value=cursor):
            resp = client.get("/exceptions/?status=PENDING")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2
        # SKU123456 has deviation_pct 0.38 > 0.26 — should be first
        assert data[0]["sku_id"] == "SKU123456"
        assert data[0]["status"] == "PENDING"

    def test_approve_exception_updates_queue(self, client):
        """Approve POST returns APPROVED status with manager_id."""
        cursor = make_mock_cursor()
        exception_id = "AH01|SKU123456|20260508T061500Z"
        with patch("src.api.routers.exceptions.get_databricks_connection", return_value=cursor):
            resp = client.post(
                f"/exceptions/{exception_id}/approve",
                json={"override_qty": 50, "reviewer_note": "Local event"},
                headers={"X-Manager-Id": "manager-001"},
            )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "APPROVED"
        assert data["manager_id"] == "manager-001"
        # Verify UPDATE SQL was executed on the cursor
        cursor.execute.assert_called()
        call_args = cursor.execute.call_args_list
        update_calls = [c for c in call_args if "UPDATE" in str(c)]
        assert len(update_calls) >= 1

    def test_reject_exception_returns_blocked(self, client):
        """Reject POST returns BLOCKED status."""
        cursor = make_mock_cursor()
        exception_id = "AH01|SKU789012|20260508T061500Z"
        with patch("src.api.routers.exceptions.get_databricks_connection", return_value=cursor):
            resp = client.post(
                f"/exceptions/{exception_id}/reject",
                headers={"X-Manager-Id": "manager-001"},
            )
        assert resp.status_code == 200
        assert resp.json()["status"] == "BLOCKED"

    def test_get_single_exception(self, client):
        """GET /{id} returns single exception."""
        cursor = make_mock_cursor(fetchone_row=EXCEPTION_ROWS[0])
        exception_id = "AH01|SKU123456|20260508T061500Z"
        with patch("src.api.routers.exceptions.get_databricks_connection", return_value=cursor):
            resp = client.get(f"/exceptions/{exception_id}")
        assert resp.status_code == 200
        assert resp.json()["sku_id"] == "SKU123456"

    def test_get_missing_exception_returns_404(self, client):
        """GET /{id} for unknown exception returns 404."""
        cursor = make_mock_cursor(fetchone_row=None)
        exception_id = "AH01|UNKNOWN|20260508T061500Z"
        with patch("src.api.routers.exceptions.get_databricks_connection", return_value=cursor):
            resp = client.get(f"/exceptions/{exception_id}")
        assert resp.status_code == 404

    def test_health_endpoint(self, client):
        """Health check returns 200 with status ok and timestamp."""
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "timestamp" in data
