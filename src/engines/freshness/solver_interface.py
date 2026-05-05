from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class OrderRecommendation:
    sku_id: str
    site_id: str
    recommended_qty: int
    transit_to_life_ratio: float
    blocked: bool
    solver_status: str


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


class SolverInterface(ABC):
    """Abstract MILP solver interface — swap CBC/OR-Tools/Gurobi without touching Engine 2."""

    @abstractmethod
    def solve(self, inputs: list[SolverInput]) -> list[OrderRecommendation]:
        """Run the MILP and return one recommendation per input."""
        ...

    @abstractmethod
    def get_solver_name(self) -> str:
        ...
