import os
from typing import Any

import requests


class SAPBTPClient:
    """HTTP client for SAP BTP AI Core BAPI orchestration endpoint."""

    def __init__(self, base_url: str | None = None, token: str | None = None):
        self._base_url = base_url or os.environ["SAP_BTP_BASE_URL"]
        self._token = token or os.environ["SAP_BTP_TOKEN"]

    def call_bapi(self, bapi_name: str, payload: dict[str, Any]) -> dict[str, Any]:
        url = f"{self._base_url}/bapi/{bapi_name}"
        response = requests.post(
            url,
            json=payload,
            headers={
                "Authorization": f"Bearer {self._token}",
                "Content-Type": "application/json",
            },
            timeout=30,
        )
        response.raise_for_status()
        return response.json()
