from dataclasses import dataclass
from typing import Any

from ..sap_btp.ai_core_client import SAPBTPClient


@dataclass
class PurchaseOrderLine:
    material: str
    plant: str
    quantity: float
    unit: str
    delivery_date: str  # YYYYMMDD
    vendor: str


@dataclass
class PurchaseOrderResult:
    po_number: str
    success: bool
    messages: list[str]


class BAPIPoCreate:
    """Wraps BAPI_PO_CREATE1 via SAP BTP AI Core. Clean Core — zero SAP modifications."""

    BAPI_NAME = "BAPI_PO_CREATE1"

    def __init__(self, btp_client: SAPBTPClient):
        self._client = btp_client

    def create(self, lines: list[PurchaseOrderLine], doc_type: str = "NB") -> PurchaseOrderResult:
        payload = self._build_payload(lines, doc_type)
        response = self._client.call_bapi(self.BAPI_NAME, payload)
        return PurchaseOrderResult(
            po_number=response.get("EXPPURCHASEORDER", ""),
            success=not any(m.get("TYPE") == "E" for m in response.get("RETURN", [])),
            messages=[m.get("MESSAGE", "") for m in response.get("RETURN", [])],
        )

    def _build_payload(self, lines: list[PurchaseOrderLine], doc_type: str) -> dict[str, Any]:
        return {
            "POHEADER": {"DOC_TYPE": doc_type, "CREAT_DATE": ""},
            "POITEM": [
                {
                    "PO_ITEM": str(i + 1).zfill(5),
                    "MATERIAL": line.material,
                    "PLANT": line.plant,
                    "QUANTITY": line.quantity,
                    "PO_UNIT": line.unit,
                    "DELIV_DATE": line.delivery_date,
                }
                for i, line in enumerate(lines)
            ],
            "POPARTNER": [{"LANGU": "EN", "BUSN_PARTNR": lines[0].vendor, "PART_ROLE": "LF"}] if lines else [],
        }
