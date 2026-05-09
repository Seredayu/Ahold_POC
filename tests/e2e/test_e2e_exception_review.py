"""
E2E smoke tests against live Azure Container Apps endpoint.

Prerequisites:
  - E2E_API_BASE env var set to ACA FQDN (https://...)
  - gold.replenishment.exception_queue table exists with >= 1 ESCALATE row
  - ACA has valid DATABRICKS_HOST / DATABRICKS_TOKEN / DATABRICKS_HTTP_PATH

Run:
  E2E_API_BASE=https://ahpoc-api.hash.westeurope.azurecontainerapps.io pytest tests/e2e/ -v
"""
import pytest


class TestHealthEndpoint:
    def test_health_returns_ok(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert "timestamp" in data


class TestExceptionListEndpoint:
    def test_list_pending_returns_200(self, client):
        resp = client.get("/exceptions/?status=PENDING")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_pending_items_have_required_fields(self, client):
        resp = client.get("/exceptions/?status=PENDING")
        assert resp.status_code == 200
        items = resp.json()
        if not items:
            pytest.skip("No PENDING exceptions in queue — seed data first")
        item = items[0]
        assert "exception_id" in item
        assert "unified_sku_id" in item
        assert "recommended_qty" in item
        assert "quantity_deviation_pct" in item
        assert item["status"] == "PENDING"
        assert "|" in item["exception_id"]  # werks|sku|loaded_at format

    def test_sorted_by_deviation_pct_descending(self, client):
        resp = client.get("/exceptions/?status=PENDING")
        items = resp.json()
        if len(items) < 2:
            pytest.skip("Need >= 2 PENDING items to verify sort order")
        deviations = [item["quantity_deviation_pct"] for item in items]
        assert deviations == sorted(deviations, reverse=True)


class TestApproveRejectWorkflow:
    def test_approve_exception(self, client, manager_id):
        """Approve first available PENDING exception, verify response."""
        pending = client.get("/exceptions/?status=PENDING").json()
        if not pending:
            pytest.skip("No PENDING exceptions to approve")

        exception_id = pending[0]["exception_id"]
        resp = client.post(
            f"/exceptions/{exception_id}/approve",
            json={"override_qty": 42, "override_reason": "E2E test approval"},
            headers={"X-Manager-Id": manager_id},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "APPROVED"
        assert data["manager_id"] == manager_id
        assert data["exception_id"] == exception_id
        assert "decision_timestamp" in data

    def test_approved_exception_no_longer_pending(self, client):
        """Previously approved exception should not appear in PENDING list."""
        # Note: consumes same PENDING pool as test_approve_exception. Tests are ordered by
        # pytest class definition order, so approve runs first. If pool is empty, both skip.
        pending = client.get("/exceptions/?status=PENDING").json()
        if not pending:
            pytest.skip("No PENDING exceptions available")

        exception_id = pending[0]["exception_id"]
        client.post(
            f"/exceptions/{exception_id}/approve",
            json={"override_qty": None, "override_reason": None},
            headers={"X-Manager-Id": "e2e-verifier"},
        )

        still_pending = client.get("/exceptions/?status=PENDING").json()
        pending_ids = [item["exception_id"] for item in still_pending]
        assert exception_id not in pending_ids

    def test_reject_exception(self, client, manager_id):
        """Reject first available PENDING exception, verify BLOCKED status."""
        pending = client.get("/exceptions/?status=PENDING").json()
        if not pending:
            pytest.skip("No PENDING exceptions to reject")

        exception_id = pending[0]["exception_id"]
        resp = client.post(
            f"/exceptions/{exception_id}/reject",
            headers={"X-Manager-Id": manager_id},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "BLOCKED"
        assert data["manager_id"] == manager_id
        assert "decision_timestamp" in data
