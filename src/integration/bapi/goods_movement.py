from dataclasses import dataclass

from ..sap_btp.ai_core_client import SAPBTPClient


@dataclass
class GoodsMovementItem:
    material: str
    plant: str
    storage_location: str
    quantity: float
    movement_type: str  # e.g. "551" for stock correction


@dataclass
class GoodsMovementResult:
    material_document: str
    success: bool
    messages: list[str]


class BAPIGoodsMovement:
    """Wraps BAPI_GOODSMVT_CREATE for phantom stock corrections. Clean Core."""

    BAPI_NAME = "BAPI_GOODSMVT_CREATE"

    def __init__(self, btp_client: SAPBTPClient):
        self._client = btp_client

    def post(self, items: list[GoodsMovementItem], posting_date: str) -> GoodsMovementResult:
        payload = {
            "GOODSMVT_HEADER": {"PSTNG_DATE": posting_date, "DOC_DATE": posting_date},
            "GOODSMVT_CODE": {"GM_CODE": "01"},
            "GOODSMVT_ITEM": [
                {
                    "MATERIAL": item.material,
                    "PLANT": item.plant,
                    "STGE_LOC": item.storage_location,
                    "MOVE_TYPE": item.movement_type,
                    "ENTRY_QNT": item.quantity,
                }
                for item in items
            ],
        }
        response = self._client.call_bapi(self.BAPI_NAME, payload)
        return GoodsMovementResult(
            material_document=response.get("MATERIALDOCUMENT", ""),
            success=not any(m.get("TYPE") == "E" for m in response.get("RETURN", [])),
            messages=[m.get("MESSAGE", "") for m in response.get("RETURN", [])],
        )
