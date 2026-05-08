from integration.bapi.po_create import BAPIPoCreate, PurchaseOrderLine
from integration.sap_btp.ai_core_client import SAPBTPClient
from engines.phantom_stock.bapi_client import BAPIError


class POClient:
    """Thin adapter bridging werks/sku/qty interface to BAPIPoCreate."""

    def __init__(self, endpoint_url: str, token: str):
        self._bapi = BAPIPoCreate(SAPBTPClient(base_url=endpoint_url, token=token))

    def create_purchase_order(self, werks: str, unified_sku_id: str, quantity: int) -> dict:
        line = PurchaseOrderLine(
            material=unified_sku_id,
            plant=werks,
            quantity=float(quantity),
            unit="EA",
            delivery_date="",
            vendor="",
        )
        result = self._bapi.create(lines=[line])
        if not result.success:
            raise BAPIError(f"BAPI_PO_CREATE1 failed: {'; '.join(result.messages)}")
        return {"PO_NUMBER": result.po_number}
