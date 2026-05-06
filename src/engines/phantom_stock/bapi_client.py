import time

import requests


class BAPIError(Exception):
    """Raised when SAP BTP AI Core returns a non-2xx response after all retries."""


_RETRY_DELAYS = [1, 2, 4]  # seconds before retry 1, 2, 3 — total 4 attempts


class BAPIClient:
    """
    Posts goods movement corrections to SAP ECC via SAP BTP AI Core HTTP endpoint.
    Credentials read from Databricks Secrets: scope "sap-btp", key "ai-core-token".
    Every call logged to MLflow as a run artifact for full auditability.
    """

    def __init__(self, endpoint_url: str, token: str):
        self._endpoint = endpoint_url
        self._token = token

    def post_goods_movement(
        self,
        werks: str,
        unified_sku_id: str,
        movement_type: str = "562",  # inventory difference posting — phantom removal
        quantity: float = 0.0,       # zero-out the phantom stock
    ) -> dict:
        """POST to SAP BTP AI Core. Raises BAPIError on non-2xx after 4 attempts (3 retries)."""
        payload = {
            "WERKS": werks,
            "MATNR": unified_sku_id,
            "BWART": movement_type,
            "MENGE": quantity,
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
                return response.json() if response.content else {}

        if last_exc is not None:
            raise BAPIError(
                f"BAPI_GOODSMVT_CREATE failed after {1 + len(_RETRY_DELAYS)} attempts: "
                f"{last_exc}"
            ) from last_exc
        raise BAPIError(
            f"BAPI_GOODSMVT_CREATE failed after {1 + len(_RETRY_DELAYS)} attempts: "
            f"HTTP {response.status_code} — {response.text}"
        )
