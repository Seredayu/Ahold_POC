from typing import Optional

from pydantic import BaseModel


class ExceptionQueueSchema(BaseModel):
    werks: str
    unified_sku_id: str
    recommended_qty: int
    transit_to_life_ratio: float
    quantity_deviation_pct: float
    sweeper_action: str  # AUTO_APPROVE / ESCALATE / BLOCK
    exception_type: Optional[str] = None  # "High Deviation" / "High Transit-to-Life" / etc
    manager_decision: Optional[str] = None  # APPROVED / REJECTED / None
    manager_id: Optional[str] = None  # Azure AD object ID
    decision_timestamp: Optional[str] = None  # ISO timestamp string
    override_reason: Optional[str] = None
    override_qty: Optional[int] = None
    shap_values: Optional[dict[str, float]] = None  # feature contributions
    loaded_at: str = ""


def build_exception_id(werks: str, unified_sku_id: str, loaded_at: str) -> str:
    return f"{werks}_{unified_sku_id}_{loaded_at}"
