from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()


class ExceptionItem(BaseModel):
    exception_id: str
    sku_id: str
    site_id: str
    exception_type: str
    recommended_qty: int
    deviation_pct: float
    shap_values: dict[str, float]
    status: str  # "PENDING" | "APPROVED" | "BLOCKED" | "ESCALATED"


class ApproveRequest(BaseModel):
    override_qty: int | None = None
    reviewer_note: str | None = None


@router.get("/", response_model=list[ExceptionItem])
def list_exceptions(store_id: str | None = None, status: str = "PENDING") -> list[ExceptionItem]:
    # TODO: query gold.replenishment.exception_queue
    return []


@router.post("/{exception_id}/approve")
def approve_exception(exception_id: str, body: ApproveRequest) -> dict:
    # TODO: write approval to gold.replenishment.exception_queue
    return {"exception_id": exception_id, "status": "APPROVED"}


@router.post("/{exception_id}/reject")
def reject_exception(exception_id: str) -> dict:
    return {"exception_id": exception_id, "status": "BLOCKED"}
