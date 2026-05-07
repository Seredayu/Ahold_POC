import pytest


# ---------------------------------------------------------------------------
# Task 2 — TTL Policy
# ---------------------------------------------------------------------------

def test_ttl_hard_block_fresh_produce():
    from engines.freshness.solver_interface import OrderRecommendation, SolverInput
    from engines.freshness.ttl_policy import apply_ttl_policy

    rec = OrderRecommendation(
        sku_id="SKU001", site_id="1000",
        recommended_qty=80, transit_to_life_ratio=0.67,
        blocked=False, solver_status="Optimal",
    )
    inp = SolverInput(
        sku_id="SKU001", site_id="1000",
        demand_p10=50.0, demand_p50=100.0, demand_p90=150.0,
        current_stock=20, shelf_life_days=6, transit_days=4,
        min_order_qty=1, max_order_qty=200,
        truck_capacity_remaining=1000.0,
        category="FRESH_PRODUCE", unit_cost=2.0,
    )
    result = apply_ttl_policy(rec, inp)

    assert result.blocked is True
    assert result.recommended_qty == 0
    assert result.ttl_policy_applied == "HARD_BLOCK"
    assert result.solver_status == "BLOCKED_TRANSIT_TO_LIFE"


def test_ttl_pass_fresh_produce():
    from engines.freshness.solver_interface import OrderRecommendation, SolverInput
    from engines.freshness.ttl_policy import apply_ttl_policy

    rec = OrderRecommendation(
        sku_id="SKU002", site_id="1000",
        recommended_qty=80, transit_to_life_ratio=0.33,
        blocked=False, solver_status="Optimal",
    )
    inp = SolverInput(
        sku_id="SKU002", site_id="1000",
        demand_p10=50.0, demand_p50=100.0, demand_p90=150.0,
        current_stock=20, shelf_life_days=6, transit_days=2,
        min_order_qty=1, max_order_qty=200,
        truck_capacity_remaining=1000.0,
        category="FRESH_PRODUCE", unit_cost=2.0,
    )
    result = apply_ttl_policy(rec, inp)

    assert result.blocked is False
    assert result.ttl_policy_applied == "PASS"
    assert result.recommended_qty == 80  # unchanged


def test_ttl_soft_cap_bakery():
    from engines.freshness.solver_interface import OrderRecommendation, SolverInput
    from engines.freshness.ttl_policy import apply_ttl_policy

    # transit_to_life_ratio = 5/8 = 0.625 > 0.50 → soft cap for BAKERY
    rec = OrderRecommendation(
        sku_id="SKU003", site_id="1000",
        recommended_qty=80, transit_to_life_ratio=0.625,
        blocked=False, solver_status="Optimal",
    )
    inp = SolverInput(
        sku_id="SKU003", site_id="1000",
        demand_p10=40.0, demand_p50=80.0, demand_p90=120.0,
        current_stock=10, shelf_life_days=8, transit_days=5,
        min_order_qty=1, max_order_qty=200,
        truck_capacity_remaining=1000.0,
        category="BAKERY", unit_cost=1.5,
    )
    result = apply_ttl_policy(rec, inp)

    assert result.day_old_discount is True
    assert result.blocked is False
    assert result.ttl_policy_applied == "SOFT_CAP"
    assert result.recommended_qty < 80  # reduced: round(80 * (1 - 0.625)) = 30
    assert result.recommended_qty == 30
