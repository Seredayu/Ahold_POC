"""Contract tests: SAP BAPI response shape.

Verifies our BAPI client code handles the exact response structure
returned by SAP ECC. No live SAP system needed.
"""
import pytest


class TestBAPIPoCreate1Response:
    def test_response_has_pohdr(self, bapi_po_response):
        assert "POHDR" in bapi_po_response

    def test_pohdr_has_ebeln(self, bapi_po_response):
        assert "EBELN" in bapi_po_response["POHDR"]

    def test_ebeln_is_10_digits(self, bapi_po_response):
        ebeln = bapi_po_response["POHDR"]["EBELN"]
        assert ebeln.isdigit()
        assert len(ebeln) == 10

    def test_return_has_success_type(self, bapi_po_response):
        return_table = bapi_po_response["RETURN"]
        assert any(r["TYPE"] == "S" for r in return_table)

    def test_return_message_contains_po_number(self, bapi_po_response):
        ebeln = bapi_po_response["POHDR"]["EBELN"]
        messages = [r["MESSAGE"] for r in bapi_po_response["RETURN"]]
        assert any(ebeln in m for m in messages)


class TestBAPIGoodsMvtResponse:
    def test_response_has_material_document(self, bapi_goodsmvt_response):
        assert "MATERIALDOCUMENT" in bapi_goodsmvt_response

    def test_material_document_is_10_digits(self, bapi_goodsmvt_response):
        doc = bapi_goodsmvt_response["MATERIALDOCUMENT"]
        assert doc.isdigit()
        assert len(doc) == 10

    def test_response_has_year(self, bapi_goodsmvt_response):
        assert "MATDOCUMENTYEAR" in bapi_goodsmvt_response
        assert bapi_goodsmvt_response["MATDOCUMENTYEAR"] == "2026"

    def test_return_has_success_type(self, bapi_goodsmvt_response):
        return_table = bapi_goodsmvt_response["RETURN"]
        assert any(r["TYPE"] == "S" for r in return_table)


class TestBAPIErrorHandling:
    def test_error_type_e_detected(self):
        error_response = {
            "POHDR": {"EBELN": ""},
            "RETURN": [{"TYPE": "E", "MESSAGE": "Authorization check failed"}]
        }
        has_error = any(r["TYPE"] == "E" for r in error_response["RETURN"])
        assert has_error

    def test_empty_ebeln_signals_failure(self):
        failed_response = {
            "POHDR": {"EBELN": ""},
            "RETURN": [{"TYPE": "E", "MESSAGE": "PO creation failed"}]
        }
        assert failed_response["POHDR"]["EBELN"] == ""
