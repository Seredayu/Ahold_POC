import pulp
from .solver_interface import OrderRecommendation, SolverInput, SolverInterface


class PuLPCBCSolver(SolverInterface):
    """PuLP/CBC MILP solver. Abstracted behind SolverInterface for OR-Tools/Gurobi swap."""

    def get_solver_name(self) -> str:
        return "PuLP/CBC"

    def solve(self, inputs: list[SolverInput]) -> list[OrderRecommendation]:
        results = []
        for inp in inputs:
            transit_to_life = inp.transit_days / max(inp.shelf_life_days, 1)

            if transit_to_life > 0.5:
                results.append(OrderRecommendation(
                    sku_id=inp.sku_id,
                    site_id=inp.site_id,
                    recommended_qty=0,
                    transit_to_life_ratio=transit_to_life,
                    blocked=True,
                    solver_status="BLOCKED_TRANSIT_TO_LIFE",
                ))
                continue

            prob = pulp.LpProblem(f"replenishment_{inp.sku_id}_{inp.site_id}", pulp.LpMinimize)
            qty = pulp.LpVariable("qty", lowBound=inp.min_order_qty, upBound=inp.max_order_qty, cat="Integer")

            # Minimise overstock cost while covering P90 demand
            prob += qty

            # Must cover P90 demand minus current stock
            net_need = max(0, inp.demand_p90 - inp.current_stock)
            prob += qty >= net_need

            # Truck capacity constraint
            prob += qty <= inp.truck_capacity_remaining

            solver = pulp.PULP_CBC_CMD(msg=False)
            prob.solve(solver)

            status = pulp.LpStatus[prob.status]
            recommended = int(pulp.value(qty)) if status == "Optimal" else 0

            results.append(OrderRecommendation(
                sku_id=inp.sku_id,
                site_id=inp.site_id,
                recommended_qty=recommended,
                transit_to_life_ratio=transit_to_life,
                blocked=False,
                solver_status=status,
            ))

        return results
