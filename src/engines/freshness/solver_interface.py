from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass
class SolverInput:
    sku_id: str
    site_id: str
    demand_p50: float
    demand_p90: float
    current_stock: int
    shelf_life_days: int
    transit_days: int
    min_order_qty: int
    max_order_qty: int
    truck_capacity_remaining: float
    # Phase 3B extensions — defaulted to preserve backwards compatibility
    category: str = "DEFAULT"
    unit_cost: float = 0.0
    demand_p10: float = 0.0


@dataclass
class OrderRecommendation:
    sku_id: str
    site_id: str
    recommended_qty: int
    transit_to_life_ratio: float
    blocked: bool
    solver_status: str
    # Phase 3B extensions — defaulted to preserve backwards compatibility
    approval_status: str = "PENDING_REVIEW"
    approval_reason: Optional[str] = None
    confidence_ratio: float = 0.0
    order_value: float = 0.0
    ttl_policy_applied: str = "PASS"
    day_old_discount: bool = False
    promo_lift: float = 1.0
    weather_lift: float = 1.0


class SolverInterface(ABC):
    """Abstract MILP solver interface — swap CBC/OR-Tools/Gurobi without touching Engine 2."""

    @abstractmethod
    def solve(self, inputs: list[SolverInput]) -> list[OrderRecommendation]:
        """Run the MILP and return one recommendation per input."""
        ...

    @abstractmethod
    def get_solver_name(self) -> str:
        ...
