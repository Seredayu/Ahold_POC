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


def test_ttl_bakery_hard_block_when_ratio_at_or_above_one():
    from engines.freshness.solver_interface import OrderRecommendation, SolverInput
    from engines.freshness.ttl_policy import apply_ttl_policy

    # transit_to_life_ratio = 1.0 (transit_days == shelf_life_days)
    # reduced_qty = max(0, round(80 * (1.0 - 1.0))) = 0 → fall-through to HARD_BLOCK
    rec = OrderRecommendation(
        sku_id="SKU004", site_id="1000",
        recommended_qty=80, transit_to_life_ratio=1.0,
        blocked=False, solver_status="Optimal",
    )
    inp = SolverInput(
        sku_id="SKU004", site_id="1000",
        demand_p10=40.0, demand_p50=80.0, demand_p90=120.0,
        current_stock=10, shelf_life_days=5, transit_days=5,
        min_order_qty=1, max_order_qty=200,
        truck_capacity_remaining=1000.0,
        category="BAKERY", unit_cost=1.5,
    )
    result = apply_ttl_policy(rec, inp)

    assert result.blocked is True
    assert result.recommended_qty == 0
    assert result.ttl_policy_applied == "HARD_BLOCK"
    assert result.solver_status == "BLOCKED_TRANSIT_TO_LIFE"


def test_ttl_bakery_hard_block_when_ratio_exceeds_one():
    from engines.freshness.solver_interface import OrderRecommendation, SolverInput
    from engines.freshness.ttl_policy import apply_ttl_policy

    # transit_to_life_ratio = 1.5 (transit_days > shelf_life_days)
    # reduced_qty = max(0, round(80 * (1.0 - 1.5))) = max(0, round(-40)) = 0 → HARD_BLOCK
    rec = OrderRecommendation(
        sku_id="SKU005", site_id="1000",
        recommended_qty=80, transit_to_life_ratio=1.5,
        blocked=False, solver_status="Optimal",
    )
    inp = SolverInput(
        sku_id="SKU005", site_id="1000",
        demand_p10=40.0, demand_p50=80.0, demand_p90=120.0,
        current_stock=10, shelf_life_days=2, transit_days=3,
        min_order_qty=1, max_order_qty=200,
        truck_capacity_remaining=1000.0,
        category="BAKERY", unit_cost=1.5,
    )
    result = apply_ttl_policy(rec, inp)

    assert result.blocked is True
    assert result.recommended_qty == 0
    assert result.ttl_policy_applied == "HARD_BLOCK"


# ---------------------------------------------------------------------------
# Task 3 — Approval Gate
# ---------------------------------------------------------------------------

def test_approval_gate_auto_approved():
    from engines.freshness.solver_interface import OrderRecommendation, SolverInput
    from engines.freshness.approval_gate import ApprovalGate

    # uncertainty_spread = 110 - 90 = 20; confidence_ratio = 20/100 = 0.20 < 0.30 → pass
    # order_value = 10 * 30.0 = €300 < €500 → pass
    rec = OrderRecommendation(
        sku_id="SKU001", site_id="1000",
        recommended_qty=10, transit_to_life_ratio=0.3,
        blocked=False, solver_status="Optimal",
    )
    inp = SolverInput(
        sku_id="SKU001", site_id="1000",
        demand_p10=90.0, demand_p50=100.0, demand_p90=110.0,
        current_stock=50, shelf_life_days=10, transit_days=3,
        min_order_qty=1, max_order_qty=200,
        truck_capacity_remaining=1000.0,
        category="FRESH_PRODUCE", unit_cost=30.0,
    )
    result = ApprovalGate().evaluate(rec, inp)

    assert result.approval_status == "AUTO_APPROVED"
    assert result.approval_reason is None
    assert abs(result.confidence_ratio - 0.20) < 1e-9
    assert abs(result.order_value - 300.0) < 1e-9


def test_approval_gate_pending_high_uncertainty():
    from engines.freshness.solver_interface import OrderRecommendation, SolverInput
    from engines.freshness.approval_gate import ApprovalGate

    # uncertainty_spread = 140 - 60 = 80; confidence_ratio = 80/100 = 0.80 >= 0.30 -> fail
    rec = OrderRecommendation(
        sku_id="SKU002", site_id="1000",
        recommended_qty=10, transit_to_life_ratio=0.3,
        blocked=False, solver_status="Optimal",
    )
    inp = SolverInput(
        sku_id="SKU002", site_id="1000",
        demand_p10=60.0, demand_p50=100.0, demand_p90=140.0,
        current_stock=50, shelf_life_days=10, transit_days=3,
        min_order_qty=1, max_order_qty=200,
        truck_capacity_remaining=1000.0,
        category="FRESH_PRODUCE", unit_cost=5.0,
    )
    result = ApprovalGate().evaluate(rec, inp)

    assert result.approval_status == "PENDING_REVIEW"
    assert result.approval_reason is not None
    assert "HIGH_UNCERTAINTY" in result.approval_reason


def test_approval_gate_pending_high_value():
    from engines.freshness.solver_interface import OrderRecommendation, SolverInput
    from engines.freshness.approval_gate import ApprovalGate

    # confidence_ratio = 20/100 = 0.20 < 0.30 -> pass
    # order_value = 20 * 30.0 = €600 >= €500 -> fail
    rec = OrderRecommendation(
        sku_id="SKU003", site_id="1000",
        recommended_qty=20, transit_to_life_ratio=0.3,
        blocked=False, solver_status="Optimal",
    )
    inp = SolverInput(
        sku_id="SKU003", site_id="1000",
        demand_p10=90.0, demand_p50=100.0, demand_p90=110.0,
        current_stock=50, shelf_life_days=10, transit_days=3,
        min_order_qty=1, max_order_qty=200,
        truck_capacity_remaining=1000.0,
        category="FRESH_PRODUCE", unit_cost=30.0,
    )
    result = ApprovalGate().evaluate(rec, inp)

    assert result.approval_status == "PENDING_REVIEW"
    assert result.approval_reason is not None
    assert "HIGH_VALUE" in result.approval_reason


# ---------------------------------------------------------------------------
# Task 4 — PuLP Solver end-to-end
# ---------------------------------------------------------------------------

def test_pulp_solver_produces_optimal_recommendation():
    from engines.freshness.solver_interface import SolverInput
    from engines.freshness.replenishment_quantity_optimizer import PuLPCBCSolver

    # demand_p90=100, current_stock=20 -> net_need=80; transit_to_life=2/10=0.20 < 0.50 -> PASS
    inp = SolverInput(
        sku_id="SKU001", site_id="1000",
        demand_p10=60.0, demand_p50=80.0, demand_p90=100.0,
        current_stock=20, shelf_life_days=10, transit_days=2,
        min_order_qty=1, max_order_qty=200,
        truck_capacity_remaining=1000.0,
        category="FRESH_PRODUCE", unit_cost=5.0,
    )
    solver = PuLPCBCSolver()
    results = solver.solve([inp])

    assert len(results) == 1
    result = results[0]
    assert result.solver_status == "Optimal"
    assert result.recommended_qty >= 80
    assert result.ttl_policy_applied == "PASS"
    assert result.blocked is False


# ---------------------------------------------------------------------------
# Task 5 — POClient
# ---------------------------------------------------------------------------

def test_po_client_raises_bapi_error_on_http_500():
    from unittest.mock import patch, MagicMock
    from engines.freshness.po_client import POClient
    from engines.phantom_stock.bapi_client import BAPIError

    mock_response = MagicMock()
    mock_response.ok = False
    mock_response.status_code = 500
    mock_response.text = "Internal Server Error"

    with patch("engines.freshness.po_client.requests.post", return_value=mock_response), \
         patch("engines.freshness.po_client.time.sleep"):
        client = POClient("https://fake-btp-endpoint", "fake-token")
        with pytest.raises(BAPIError) as exc_info:
            client.create_purchase_order("1000", "SKU001", 50)

    assert "500" in str(exc_info.value)


def test_approval_gate_blocked_passthrough():
    from engines.freshness.solver_interface import OrderRecommendation, SolverInput
    from engines.freshness.approval_gate import ApprovalGate

    # Already-blocked recommendations must pass through with approval_status="BLOCKED",
    # never AUTO_APPROVED — this is the safety gate before _entry_bapi_po_create.
    rec = OrderRecommendation(
        sku_id="SKU006", site_id="1000",
        recommended_qty=0, transit_to_life_ratio=0.8,
        blocked=True, solver_status="BLOCKED_TRANSIT_TO_LIFE",
    )
    inp = SolverInput(
        sku_id="SKU006", site_id="1000",
        demand_p10=40.0, demand_p50=80.0, demand_p90=120.0,
        current_stock=10, shelf_life_days=5, transit_days=4,
        min_order_qty=1, max_order_qty=200,
        truck_capacity_remaining=1000.0,
        category="FRESH_PRODUCE", unit_cost=2.0,
    )
    result = ApprovalGate().evaluate(rec, inp)

    assert result.approval_status == "BLOCKED"
    assert result.blocked is True
    assert result.recommended_qty == 0
