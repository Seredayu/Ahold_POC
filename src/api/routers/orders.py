from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()


class OrderRecommendation(BaseModel):
    sku_id: str
    site_id: str
    recommended_qty: int
    transit_to_life_ratio: float
    blocked: bool
    run_date: str


@router.get("/recommendations", response_model=list[OrderRecommendation])
def get_recommendations(site_id: str | None = None, run_date: str | None = None) -> list[OrderRecommendation]:
    # TODO: query gold.replenishment.order_recommendations
    return []
