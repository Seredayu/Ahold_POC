import time
import requests
from engines.phantom_stock.bapi_client import BAPIClient, BAPIError

_RETRY_DELAYS = [1, 2, 4]  # 4 total attempts: 1 initial + 3 retries


class POClient(BAPIClient):
    """
    Extends BAPIClient to create purchase orders via BAPI_PO_CREATE1.
    Inherits endpoint URL, Bearer token, and connection timeout from BAPIClient.__init__().
    Uses the same retry pattern and BAPIError exception class.
    """

    def create_purchase_order(
        self,
        werks: str,
        unified_sku_id: str,
        quantity: int,
    ) -> dict:
        """
        POST BAPI_PO_CREATE1 to SAP BTP AI Core. Returns response dict containing PO_NUMBER.
        Raises BAPIError on HTTP error, network failure, or missing PO_NUMBER in response.
        """
        payload = {
            "POHEADER": {
                "COMP_CODE": werks,
                "DOC_TYPE": "NB",
            },
            "POITEM": [
                {
                    "MATERIAL": unified_sku_id,
                    "PLANT": werks,
                    "QUANTITY": quantity,
                    "UNIT": "EA",
                }
            ],
        }
        response = None
        last_exc: Exception | None = None
        for attempt in range(1 + len(_RETRY_DELAYS)):
            if attempt > 0:
                time.sleep(_RETRY_DELAYS[attempt - 1])
            try:
                response = requests.post(
                    self._endpoint,
                    json=payload,
                    headers={"Authorization": f"Bearer {self._token}"},
                    timeout=10,
                )
            except requests.exceptions.RequestException as exc:
                last_exc = exc
                continue
            if response.ok:
                data = response.json() if response.content else {}
                if "PO_NUMBER" not in data:
                    raise BAPIError(
                        f"BAPI_PO_CREATE1 succeeded but returned no PO_NUMBER: {data}"
                    )
                return data

        if response is None:
            raise BAPIError(
                f"BAPI_PO_CREATE1 failed after {1 + len(_RETRY_DELAYS)} attempts: {last_exc}"
            ) from last_exc
        raise BAPIError(
            f"BAPI_PO_CREATE1 failed after {1 + len(_RETRY_DELAYS)} attempts: "
            f"HTTP {response.status_code} — {response.text}"
        )
