"""Contract tests: Databricks SQL connector response shape.

These tests verify our code handles the exact column order and types
returned by gold.replenishment.exception_queue. No live Databricks needed.
"""
import json
from unittest.mock import MagicMock, patch

import pytest


def _make_mock_cursor(rows):
    cursor = MagicMock()
    cursor.fetchall.return_value = [tuple(r) for r in rows]
    cursor.fetchone.return_value = tuple(rows[0]) if rows else None
    return cursor


def _make_mock_conn(cursor):
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn


class TestExceptionQueueSchema:
    def test_row_has_14_columns(self, exception_queue_rows):
        assert len(exception_queue_rows[0]) == 14

    def test_werks_is_string(self, exception_queue_rows):
        assert isinstance(exception_queue_rows[0][0], str)

    def test_unified_sku_id_is_string(self, exception_queue_rows):
        assert isinstance(exception_queue_rows[0][1], str)

    def test_recommended_qty_is_int(self, exception_queue_rows):
        assert isinstance(exception_queue_rows[0][2], int)

    def test_transit_to_life_ratio_is_float(self, exception_queue_rows):
        assert isinstance(exception_queue_rows[0][3], float)

    def test_quantity_deviation_pct_is_float(self, exception_queue_rows):
        assert isinstance(exception_queue_rows[0][4], float)

    def test_sweeper_action_is_escalate(self, exception_queue_rows):
        assert exception_queue_rows[0][5] == "ESCALATE"

    def test_shap_values_is_valid_json(self, exception_queue_rows):
        shap_raw = exception_queue_rows[0][12]
        parsed = json.loads(shap_raw)
        assert isinstance(parsed, dict)
        assert len(parsed) > 0

    def test_manager_decision_is_null_initially(self, exception_queue_rows):
        assert exception_queue_rows[0][7] is None

    def test_loaded_at_format(self, exception_queue_rows):
        loaded_at = exception_queue_rows[0][13]
        assert len(loaded_at) == 16
        assert "T" in loaded_at


class TestExceptionQueueListEndpoint:
    def test_list_returns_pending_exceptions(self, exception_queue_rows):
        cursor = _make_mock_cursor(exception_queue_rows)
        conn = _make_mock_conn(cursor)

        with patch("src.api.routers.exceptions.get_databricks_connection", return_value=conn):
            from fastapi.testclient import TestClient
            from src.api.main import app
            client = TestClient(app)
            resp = client.get("/exceptions/?status=PENDING")
            assert resp.status_code == 200
            data = resp.json()
            assert isinstance(data, list)

    def test_exception_id_uses_pipe_delimiter(self, exception_queue_rows):
        cursor = _make_mock_cursor(exception_queue_rows)
        conn = _make_mock_conn(cursor)

        with patch("src.api.routers.exceptions.get_databricks_connection", return_value=conn):
            from fastapi.testclient import TestClient
            from src.api.main import app
            client = TestClient(app)
            resp = client.get("/exceptions/?status=PENDING")
            data = resp.json()
            if data:
                exc_id = data[0]["exception_id"]
                parts = exc_id.split("|")
                assert len(parts) == 3, f"Expected pipe-delimited ID, got: {exc_id}"
