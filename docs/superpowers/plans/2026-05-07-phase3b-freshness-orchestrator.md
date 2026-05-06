# Phase 3B — Freshness Orchestrator Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the existing MILP solver with category-configurable TTL policy and dual auto-approval gates, then wire three entry-point functions (`milp_solve`, `write_recommendations`, `bapi_po_create`) into a unified `morning_pipeline.yml` that replaces all prior ad-hoc score jobs.

**Architecture:** `SolverInput` and `OrderRecommendation` dataclasses are extended with defaults so existing code compiles unchanged; `ttl_policy.py` and `approval_gate.py` are pure-Python modules called from the solver and pipeline respectively; `POClient` extends `BAPIClient` wholesale; the unified morning pipeline enforces all ordering via `depends_on` — no wall-clock timing.

**Tech Stack:** Python dataclasses, PuLP/CBC, MLflow, Databricks Feature Engineering (FEU), PySpark, Databricks Asset Bundles (DABs), SAP BTP AI Core (BAPI_PO_CREATE1)

---

## File Map

| File | Action |
|---|---|
| `src/engines/freshness/solver_interface.py` | Modify — extend `SolverInput` and `OrderRecommendation` with new fields (all defaulted) |
| `src/engines/freshness/ttl_policy.py` | Create — `CategoryTtlConfig` dataclass + `apply_ttl_policy()` function |
| `src/engines/freshness/approval_gate.py` | Create — `ApprovalGate` class with `evaluate()` method |
| `src/engines/freshness/replenishment_quantity_optimizer.py` | Modify — replace hardcoded TTL check with `apply_ttl_policy()` call |
| `src/engines/freshness/po_client.py` | Create — `POClient(BAPIClient)` for `BAPI_PO_CREATE1` |
| `src/engines/freshness/freshness_pipeline.py` | Create — three entry points: `milp_solve`, `write_recommendations`, `bapi_po_create` |
| `tests/unit/test_freshness_orchestrator.py` | Create — 8 unit tests |
| `resources/jobs/morning_pipeline.yml` | Create — unified 13-task daily pipeline |
| `setup.py` | Modify — add 3 freshness console_scripts |
| `databricks.yml` | Modify — add `morning_pipeline`, remove the separate `phantom_stock_score` job entry |

---

### Task 1: Extend SolverInput and OrderRecommendation

**Files:**
- Modify: `src/engines/freshness/solver_interface.py`

Context: The existing dataclasses have no defaults. New fields are added **with defaults** so `PuLPCBCSolver` (which instantiates `OrderRecommendation` positionally) does not need to change until Task 4. All new fields must come after existing fields; Python dataclasses require defaulted fields to follow non-defaulted ones.

- [ ] **Step 1: Replace `solver_interface.py` with the extended version**

Full replacement of `src/engines/freshness/solver_interface.py`:

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
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
```

- [ ] **Step 2: Verify the module imports cleanly**

Run:
```bash
python -c "from engines.freshness.solver_interface import SolverInput, OrderRecommendation, SolverInterface; s = SolverInput('a','b',1.0,1.0,1,1,1,1,1,1.0); print(s.category, s.unit_cost, s.demand_p10)"
```

Expected output: `DEFAULT 0.0 0.0`

*(If running locally is not possible due to missing Databricks libraries, skip this step — the type changes are verified by downstream tests in Tasks 2–4.)*

- [ ] **Step 3: Commit**

```bash
git add src/engines/freshness/solver_interface.py
git commit -m "feat: extend SolverInput and OrderRecommendation with Phase 3B fields"
```

---

### Task 2: TTL Policy

**Files:**
- Create: `src/engines/freshness/ttl_policy.py`
- Create: `tests/unit/test_freshness_orchestrator.py` (tests 1–3)

- [ ] **Step 1: Write the 3 failing TTL tests**

Create `tests/unit/test_freshness_orchestrator.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/unit/test_freshness_orchestrator.py -v
```

Expected: 3 failures with `ModuleNotFoundError: No module named 'engines.freshness.ttl_policy'`

*(If local Python is unavailable, skip the test run and proceed to the implementation step.)*

- [ ] **Step 3: Implement `ttl_policy.py`**

Create `src/engines/freshness/ttl_policy.py`:

```python
import dataclasses
from dataclasses import dataclass
from typing import Optional

from engines.freshness.solver_interface import CategoryTtlConfig  # defined below — add to solver_interface first
```

Wait — `CategoryTtlConfig` is a new type. Define it directly in `ttl_policy.py` (not in `solver_interface.py`, which only owns the solver contract types):

```python
import dataclasses
from dataclasses import dataclass


@dataclass
class CategoryTtlConfig:
    threshold: float   # transit_to_life_ratio limit — block/cap when ratio exceeds this
    hard_block: bool   # True = zero units (HARD_BLOCK); False = scaled qty (SOFT_CAP)


_DEFAULT_POLICY_MAP: dict[str, CategoryTtlConfig] = {
    "FRESH_PRODUCE": CategoryTtlConfig(threshold=0.50, hard_block=True),
    "BAKERY": CategoryTtlConfig(threshold=0.50, hard_block=False),
    "DEFAULT": CategoryTtlConfig(threshold=0.60, hard_block=True),
}


def apply_ttl_policy(recommendation, solver_input, policy_map=None):
    """
    Apply category-specific Transit-to-Life constraint to a solver recommendation.

    Returns a new OrderRecommendation — never mutates in place.
    FRESH_PRODUCE: hard block (qty=0, blocked=True) when ratio > 0.50
    BAKERY: soft cap (qty scaled down, day_old_discount=True) when ratio > 0.50
    DEFAULT and all other categories: hard block when ratio > 0.60
    """
    if policy_map is None:
        policy_map = _DEFAULT_POLICY_MAP

    config = policy_map.get(solver_input.category, policy_map["DEFAULT"])
    ratio = recommendation.transit_to_life_ratio

    if ratio <= config.threshold:
        return dataclasses.replace(recommendation, ttl_policy_applied="PASS")

    if config.hard_block:
        return dataclasses.replace(
            recommendation,
            recommended_qty=0,
            blocked=True,
            solver_status="BLOCKED_TRANSIT_TO_LIFE",
            ttl_policy_applied="HARD_BLOCK",
        )

    # Soft cap: scale quantity down proportionally to remaining shelf-life fraction
    reduced_qty = round(recommendation.recommended_qty * (1.0 - ratio))
    return dataclasses.replace(
        recommendation,
        recommended_qty=reduced_qty,
        day_old_discount=True,
        ttl_policy_applied="SOFT_CAP",
    )
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/unit/test_freshness_orchestrator.py -v
```

Expected: 3 passing

- [ ] **Step 5: Commit**

```bash
git add src/engines/freshness/ttl_policy.py tests/unit/test_freshness_orchestrator.py
git commit -m "feat: add TTL policy (FRESH_PRODUCE hard block, BAKERY soft cap) + 3 tests"
```

---

### Task 3: Approval Gate

**Files:**
- Create: `src/engines/freshness/approval_gate.py`
- Modify: `tests/unit/test_freshness_orchestrator.py` (append tests 4–6)

- [ ] **Step 1: Append the 3 failing approval gate tests**

Append to `tests/unit/test_freshness_orchestrator.py`:

```python
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

    # uncertainty_spread = 140 - 60 = 80; confidence_ratio = 80/100 = 0.80 ≥ 0.30 → fail
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

    # confidence_ratio = 20/100 = 0.20 < 0.30 → pass
    # order_value = 20 * 30.0 = €600 ≥ €500 → fail
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
```

- [ ] **Step 2: Run tests to verify the new 3 fail**

```bash
pytest tests/unit/test_freshness_orchestrator.py -v
```

Expected: tests 1–3 pass, tests 4–6 fail with `ModuleNotFoundError: No module named 'engines.freshness.approval_gate'`

- [ ] **Step 3: Implement `approval_gate.py`**

Create `src/engines/freshness/approval_gate.py`:

```python
import dataclasses


class ApprovalGate:
    """
    Evaluates two independent gates for autonomous PO approval.
    Both must pass for AUTO_APPROVED — either failure yields PENDING_REVIEW.

    Confidence gate: uncertainty_spread / p50 < 0.30
    Value gate:      recommended_qty × unit_cost < €500
    """

    CONFIDENCE_THRESHOLD = 0.30
    VALUE_THRESHOLD = 500.0

    def evaluate(self, recommendation, solver_input):
        """
        Returns a new OrderRecommendation with approval_status, approval_reason,
        confidence_ratio, and order_value populated. Never mutates the input.
        Blocked recommendations pass through with approval_status="BLOCKED".
        """
        if recommendation.blocked:
            return dataclasses.replace(recommendation, approval_status="BLOCKED")

        uncertainty_spread = solver_input.demand_p90 - solver_input.demand_p10
        confidence_ratio = (
            uncertainty_spread / solver_input.demand_p50
            if solver_input.demand_p50 > 0
            else 0.0
        )
        order_value = recommendation.recommended_qty * solver_input.unit_cost

        reasons = []
        if confidence_ratio >= self.CONFIDENCE_THRESHOLD:
            reasons.append(f"HIGH_UNCERTAINTY: ratio={confidence_ratio:.3f}")
        if order_value >= self.VALUE_THRESHOLD:
            reasons.append(f"HIGH_VALUE: €{order_value:.2f}")

        status = "AUTO_APPROVED" if not reasons else "PENDING_REVIEW"
        reason = "; ".join(reasons) if reasons else None

        return dataclasses.replace(
            recommendation,
            approval_status=status,
            approval_reason=reason,
            confidence_ratio=confidence_ratio,
            order_value=order_value,
        )
```

- [ ] **Step 4: Run all 6 tests**

```bash
pytest tests/unit/test_freshness_orchestrator.py -v
```

Expected: 6 passing

- [ ] **Step 5: Commit**

```bash
git add src/engines/freshness/approval_gate.py tests/unit/test_freshness_orchestrator.py
git commit -m "feat: add ApprovalGate (confidence + value gates) + 3 tests"
```

---

### Task 4: Update PuLP Solver to Use TTL Policy

**Files:**
- Modify: `src/engines/freshness/replenishment_quantity_optimizer.py`
- Modify: `tests/unit/test_freshness_orchestrator.py` (append test 7)

Context: The existing `PuLPCBCSolver.solve()` has a hardcoded `if transit_to_life > 0.5:` early return. Replace this with a call to `apply_ttl_policy()` applied to the `OrderRecommendation` after the LP solve.

- [ ] **Step 1: Append the failing solver test**

Append to `tests/unit/test_freshness_orchestrator.py`:

```python
# ---------------------------------------------------------------------------
# Task 4 — PuLP Solver end-to-end
# ---------------------------------------------------------------------------

def test_pulp_solver_produces_optimal_recommendation():
    from engines.freshness.solver_interface import SolverInput
    from engines.freshness.replenishment_quantity_optimizer import PuLPCBCSolver

    # demand_p90=100, current_stock=20 → net_need=80; transit_to_life=2/10=0.20 < 0.50 → PASS
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
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest tests/unit/test_freshness_orchestrator.py::test_pulp_solver_produces_optimal_recommendation -v
```

Expected: FAIL — `ImportError` or the old solver returns `blocked=True` for ratio 0.20 (because the old hardcoded check was `> 0.5` but that would actually pass — the test may already pass on the solver_status and qty assertions but fail on `ttl_policy_applied == "PASS"` since the old code never sets that field, or it may PASS because the old code doesn't use `ttl_policy_applied`). The test will FAIL on `ttl_policy_applied == "PASS"` because the old code never sets it (field defaults to `"PASS"` — so this might actually pass accidentally).

Actually since `ttl_policy_applied` defaults to `"PASS"` in the new dataclass, and the old solver sets no TTL fields, the test may already pass. Run it to check. If it already passes, the test is still valid as a regression guard.

- [ ] **Step 3: Replace `replenishment_quantity_optimizer.py`**

Full replacement of `src/engines/freshness/replenishment_quantity_optimizer.py`:

```python
import pulp
from .solver_interface import OrderRecommendation, SolverInput, SolverInterface
from .ttl_policy import apply_ttl_policy


class PuLPCBCSolver(SolverInterface):
    """PuLP/CBC MILP solver. Abstracted behind SolverInterface for OR-Tools/Gurobi swap."""

    def get_solver_name(self) -> str:
        return "PuLP/CBC"

    def solve(self, inputs: list[SolverInput]) -> list[OrderRecommendation]:
        results = []
        for inp in inputs:
            transit_to_life = inp.transit_days / max(inp.shelf_life_days, 1)

            prob = pulp.LpProblem(f"replenishment_{inp.sku_id}_{inp.site_id}", pulp.LpMinimize)
            qty = pulp.LpVariable(
                "qty", lowBound=inp.min_order_qty, upBound=inp.max_order_qty, cat="Integer"
            )

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

            rec = OrderRecommendation(
                sku_id=inp.sku_id,
                site_id=inp.site_id,
                recommended_qty=recommended,
                transit_to_life_ratio=transit_to_life,
                blocked=False,
                solver_status=status,
            )
            results.append(apply_ttl_policy(rec, inp))

        return results
```

- [ ] **Step 4: Run all 7 tests**

```bash
pytest tests/unit/test_freshness_orchestrator.py -v
```

Expected: 7 passing

- [ ] **Step 5: Commit**

```bash
git add src/engines/freshness/replenishment_quantity_optimizer.py tests/unit/test_freshness_orchestrator.py
git commit -m "feat: wire TTL policy into PuLP solver + end-to-end solver test"
```

---

### Task 5: PO Client

**Files:**
- Create: `src/engines/freshness/po_client.py`
- Modify: `tests/unit/test_freshness_orchestrator.py` (append test 8)

Context: `POClient` extends `BAPIClient` from Phase 2B (`src/engines/phantom_stock/bapi_client.py`). It adds one method `create_purchase_order()` that POSTs to `BAPI_PO_CREATE1` via SAP BTP AI Core. The retry loop and `BAPIError` are re-used from the parent — `_RETRY_DELAYS` is re-defined locally to avoid importing a private name.

- [ ] **Step 1: Append the failing POClient test**

Append to `tests/unit/test_freshness_orchestrator.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

```bash
pytest tests/unit/test_freshness_orchestrator.py::test_po_client_raises_bapi_error_on_http_500 -v
```

Expected: FAIL with `ModuleNotFoundError: No module named 'engines.freshness.po_client'`

- [ ] **Step 3: Implement `po_client.py`**

Create `src/engines/freshness/po_client.py`:

```python
import time
import requests
from engines.phantom_stock.bapi_client import BAPIClient, BAPIError

_RETRY_DELAYS = [1, 2, 4]  # 4 total attempts: 1 initial + 3 retries


class POClient(BAPIClient):
    """
    Extends BAPIClient to create purchase orders via BAPI_PO_CREATE1.
    Inherits endpoint URL, Bearer token, and connection timeout from BAPIClient.__init__().
    Uses the same retry pattern and BAPIError exception class.
    """

    def create_purchase_order(
        self,
        werks: str,
        unified_sku_id: str,
        quantity: int,
    ) -> dict:
        """
        POST BAPI_PO_CREATE1 to SAP BTP AI Core. Returns response dict containing PO_NUMBER.
        Raises BAPIError on HTTP error, network failure, or missing PO_NUMBER in response.
        """
        payload = {
            "POHEADER": {
                "COMP_CODE": werks,
                "DOC_TYPE": "NB",
            },
            "POITEM": [
                {
                    "MATERIAL": unified_sku_id,
                    "PLANT": werks,
                    "QUANTITY": quantity,
                    "UNIT": "EA",
                }
            ],
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
                data = response.json() if response.content else {}
                if "PO_NUMBER" not in data:
                    raise BAPIError(
                        f"BAPI_PO_CREATE1 succeeded but returned no PO_NUMBER: {data}"
                    )
                return data

        if response is None:
            raise BAPIError(
                f"BAPI_PO_CREATE1 failed after {1 + len(_RETRY_DELAYS)} attempts: {last_exc}"
            ) from last_exc
        raise BAPIError(
            f"BAPI_PO_CREATE1 failed after {1 + len(_RETRY_DELAYS)} attempts: "
            f"HTTP {response.status_code} — {response.text}"
        )
```

- [ ] **Step 4: Run all 8 tests**

```bash
pytest tests/unit/test_freshness_orchestrator.py -v
```

Expected: 8 passing

- [ ] **Step 5: Commit**

```bash
git add src/engines/freshness/po_client.py tests/unit/test_freshness_orchestrator.py
git commit -m "feat: add POClient(BAPIClient) for BAPI_PO_CREATE1 + BAPIError test"
```

---

### Task 6: Freshness Pipeline Entry Points

**Files:**
- Create: `src/engines/freshness/freshness_pipeline.py`

Context: Three DABs task entry points. No unit tests — they require a live Spark session and Databricks Feature Engineering tables. The MLflow metrics logged by this file are the observability hook for operators.

Key schema note: `PuLPCBCSolver.solve()` uses `sku_id` / `site_id` field names (not `unified_sku_id` / `werks`). The intermediate `solver_output` Delta table stores these names. `_entry_write_recommendations()` renames them to `unified_sku_id` / `werks` when joining back M4 columns and writing `order_recommendations`.

- [ ] **Step 1: Create `freshness_pipeline.py`**

Create `src/engines/freshness/freshness_pipeline.py`:

```python
import mlflow
from dataclasses import asdict
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType,
    StringType, StructField, StructType,
)

_PO_CREATE_SANITY_CAP = 200

_SOLVER_OUTPUT_SCHEMA = StructType([
    StructField("sku_id", StringType(), False),
    StructField("site_id", StringType(), False),
    StructField("recommended_qty", IntegerType(), True),
    StructField("transit_to_life_ratio", DoubleType(), True),
    StructField("blocked", BooleanType(), True),
    StructField("solver_status", StringType(), True),
    StructField("approval_status", StringType(), True),
    StructField("approval_reason", StringType(), True),
    StructField("confidence_ratio", DoubleType(), True),
    StructField("order_value", DoubleType(), True),
    StructField("ttl_policy_applied", StringType(), True),
    StructField("day_old_discount", BooleanType(), True),
    StructField("promo_lift", DoubleType(), True),
    StructField("weather_lift", DoubleType(), True),
])

_PO_AUDIT_SCHEMA = StructType([
    StructField("werks", StringType(), True),
    StructField("unified_sku_id", StringType(), True),
    StructField("recommended_qty", IntegerType(), True),
    StructField("approval_status", StringType(), True),
    StructField("bapi_status", StringType(), True),
    StructField("po_number", StringType(), True),
    StructField("bapi_message", StringType(), True),
])


def _entry_freshness_milp_solve() -> None:
    from engines.freshness.solver_interface import SolverInput
    from engines.freshness.replenishment_quantity_optimizer import PuLPCBCSolver
    from engines.freshness.approval_gate import ApprovalGate

    spark = SparkSession.getActiveSession()
    solver = PuLPCBCSolver()
    gate = ApprovalGate()

    m4 = spark.table("feature_store.demand.m4_probabilistic")
    m2 = spark.table("feature_store.demand.m2_corrected").select(
        "werks", "unified_sku_id", "promo_lift", "weather_lift"
    )
    stock = spark.table("feature_store.stock.sku_site_positions").select(
        "werks", "unified_sku_id", "stock_qty"
    )
    sku_registry = spark.table("silver.master.unified_sku_registry").select(
        "werks", "unified_sku_id", "unit_cost", "transit_days",
        "min_order_qty", "max_order_qty", "category",
        "shelf_life_days", "truck_capacity_units",
    )

    joined = (
        m4
        .join(m2, on=["werks", "unified_sku_id"], how="inner")
        .join(stock, on=["werks", "unified_sku_id"], how="inner")
        .join(sku_registry, on=["werks", "unified_sku_id"], how="inner")
        .dropna(subset=[
            "p10", "p50", "p90", "stock_qty", "shelf_life_days",
            "transit_days", "unit_cost", "min_order_qty", "max_order_qty",
            "category", "truck_capacity_units",
        ])
    )

    pandas_df = joined.toPandas()

    inputs = [
        SolverInput(
            sku_id=row["unified_sku_id"],
            site_id=row["werks"],
            demand_p10=float(row["p10"]),
            demand_p50=float(row["p50"]),
            demand_p90=float(row["p90"]),
            current_stock=int(row["stock_qty"]),
            shelf_life_days=int(row["shelf_life_days"]),
            transit_days=int(row["transit_days"]),
            min_order_qty=int(row["min_order_qty"]),
            max_order_qty=int(row["max_order_qty"]),
            truck_capacity_remaining=float(row["truck_capacity_units"]),
            category=str(row["category"]),
            unit_cost=float(row["unit_cost"]),
        )
        for _, row in pandas_df.iterrows()
    ]

    recommendations = solver.solve(inputs)
    evaluated = [gate.evaluate(rec, inp) for rec, inp in zip(recommendations, inputs)]

    lift_lookup = {
        (row["unified_sku_id"], row["werks"]): (
            float(row["promo_lift"]), float(row["weather_lift"])
        )
        for _, row in pandas_df.iterrows()
    }

    output_rows = []
    for rec in evaluated:
        promo_lift, weather_lift = lift_lookup.get((rec.sku_id, rec.site_id), (1.0, 1.0))
        d = asdict(rec)
        d["promo_lift"] = promo_lift
        d["weather_lift"] = weather_lift
        output_rows.append(d)

    output_df = spark.createDataFrame(output_rows, schema=_SOLVER_OUTPUT_SCHEMA)
    output_df.write.format("delta").mode("overwrite").saveAsTable(
        "gold.replenishment.solver_output"
    )


def _entry_write_recommendations() -> None:
    spark = SparkSession.getActiveSession()

    solver_output = (
        spark.table("gold.replenishment.solver_output")
        .withColumnRenamed("sku_id", "unified_sku_id")
        .withColumnRenamed("site_id", "werks")
    )
    m4 = spark.table("feature_store.demand.m4_probabilistic").select(
        "werks", "unified_sku_id", "p10", "p50", "p90", "uncertainty_spread"
    )

    recs = (
        solver_output
        .join(m4, on=["werks", "unified_sku_id"], how="left")
        .withColumn("_computed_at", F.current_timestamp())
        .select(
            "werks", "unified_sku_id", "recommended_qty",
            "transit_to_life_ratio", "approval_status", "approval_reason",
            "confidence_ratio", "order_value", "ttl_policy_applied",
            "day_old_discount", "solver_status",
            "p10", "p50", "p90", "uncertainty_spread",
            "promo_lift", "weather_lift", "_computed_at",
        )
    )

    recs.write.format("delta").mode("overwrite").saveAsTable(
        "gold.replenishment.order_recommendations"
    )

    pandas_recs = recs.toPandas()
    mlflow.set_experiment("freshness_orchestrator")
    with mlflow.start_run():
        mlflow.log_metric(
            "auto_approved_count",
            int((pandas_recs["approval_status"] == "AUTO_APPROVED").sum()),
        )
        mlflow.log_metric(
            "pending_review_count",
            int((pandas_recs["approval_status"] == "PENDING_REVIEW").sum()),
        )
        mlflow.log_metric(
            "blocked_count",
            int((pandas_recs["approval_status"] == "BLOCKED").sum()),
        )
        total_value = float(
            pandas_recs.loc[
                pandas_recs["approval_status"] == "AUTO_APPROVED", "order_value"
            ].sum()
        )
        mlflow.log_metric("total_order_value_eur", total_value)


def _entry_bapi_po_create() -> None:
    from databricks.sdk.runtime import dbutils
    from engines.freshness.po_client import POClient
    from engines.phantom_stock.bapi_client import BAPIError

    spark = SparkSession.getActiveSession()
    token = dbutils.secrets.get(scope="sap-btp", key="ai-core-token")
    endpoint = dbutils.secrets.get(scope="sap-btp", key="ai-core-endpoint")
    client = POClient(endpoint_url=endpoint, token=token)

    auto_rows = (
        spark.table("gold.replenishment.order_recommendations")
        .filter(
            (F.col("approval_status") == "AUTO_APPROVED")
            & (F.col("recommended_qty") > 0)
        )
        .collect()
    )

    if len(auto_rows) > _PO_CREATE_SANITY_CAP:
        raise RuntimeError(
            f"AUTO_APPROVED count {len(auto_rows)} exceeds sanity cap {_PO_CREATE_SANITY_CAP}. "
            "Possible model drift — aborting BAPI PO create. "
            "Review gold.replenishment.order_recommendations."
        )

    audit_rows = []
    for row in auto_rows:
        try:
            response = client.create_purchase_order(
                werks=row["werks"],
                unified_sku_id=row["unified_sku_id"],
                quantity=row["recommended_qty"],
            )
            audit_rows.append({
                "werks": row["werks"],
                "unified_sku_id": row["unified_sku_id"],
                "recommended_qty": row["recommended_qty"],
                "approval_status": row["approval_status"],
                "bapi_status": "ok",
                "po_number": response.get("PO_NUMBER"),
                "bapi_message": None,
            })
        except BAPIError as exc:
            audit_rows.append({
                "werks": row["werks"],
                "unified_sku_id": row["unified_sku_id"],
                "recommended_qty": row["recommended_qty"],
                "approval_status": row["approval_status"],
                "bapi_status": "error",
                "po_number": None,
                "bapi_message": str(exc),
            })

    mlflow.set_experiment("freshness_orchestrator")
    with mlflow.start_run():
        mlflow.log_dict({"po_audit": audit_rows}, "po_audit.json")
        mlflow.log_metric(
            "po_created_count", sum(1 for r in audit_rows if r["bapi_status"] == "ok")
        )
        mlflow.log_metric(
            "po_error_count", sum(1 for r in audit_rows if r["bapi_status"] == "error")
        )

    audit_df = (
        spark.createDataFrame(audit_rows, schema=_PO_AUDIT_SCHEMA)
        .withColumn("_created_at", F.current_timestamp())
    )
    audit_df.write.format("delta").mode("append").saveAsTable(
        "gold.replenishment.po_audit"
    )
```

- [ ] **Step 2: Verify the module imports without Databricks**

```bash
python -c "import ast, sys; ast.parse(open('src/engines/freshness/freshness_pipeline.py').read()); print('syntax OK')"
```

Expected: `syntax OK`

- [ ] **Step 3: Run full test suite to confirm no regressions**

```bash
pytest tests/unit/test_freshness_orchestrator.py -v
```

Expected: 8 passing (pipeline functions are not unit-tested — they require a live Spark session)

- [ ] **Step 4: Commit**

```bash
git add src/engines/freshness/freshness_pipeline.py
git commit -m "feat: add freshness_pipeline entry points (milp_solve, write_recommendations, bapi_po_create)"
```

---

### Task 7: Morning Pipeline + Console Scripts + databricks.yml

**Files:**
- Create: `resources/jobs/morning_pipeline.yml`
- Modify: `setup.py`
- Modify: `databricks.yml`

Context: The `morning_pipeline.yml` replaces the standalone `phantom_stock_score.yml`. The `feature_store_refresh` step is expanded into three sequential tasks (`fs_refresh_velocity` → `fs_refresh_stock` → `fs_refresh_collapse`) since the feature store has three separate entry points. Total task count is 13. `databricks.yml` removes the `phantom_stock_score` job entry and adds `morning_pipeline`.

- [ ] **Step 1: Create `morning_pipeline.yml`**

Create `resources/jobs/morning_pipeline.yml`:

```yaml
name: morning-pipeline-${bundle.target}
schedule:
  quartz_cron_expression: "0 0 5 * * ?"
  timezone_id: "Europe/Amsterdam"
  pause_status: ${var.schedule_enabled == "true" ? "UNPAUSED" : "PAUSED"}
tasks:
  # --- Feature Store Refresh (sequential: velocity → stock → collapse signals) ---
  - task_key: fs_refresh_velocity
    python_wheel_task:
      package_name: ahold_freshness_poc
      entry_point: fs_velocity_features
    timeout_seconds: 1800
  - task_key: fs_refresh_stock
    depends_on:
      - task_key: fs_refresh_velocity
    python_wheel_task:
      package_name: ahold_freshness_poc
      entry_point: fs_stock_features
    timeout_seconds: 1800
  - task_key: fs_refresh_collapse
    depends_on:
      - task_key: fs_refresh_stock
    python_wheel_task:
      package_name: ahold_freshness_poc
      entry_point: fs_collapse_signals
    timeout_seconds: 1800

  # --- Phantom Stock branch (parallel with demand branch after feature store) ---
  - task_key: phantom_stock_score_batch
    depends_on:
      - task_key: fs_refresh_collapse
    python_wheel_task:
      package_name: ahold_freshness_poc
      entry_point: phantom_score_batch
    timeout_seconds: 1800
  - task_key: phantom_stock_write_alerts
    depends_on:
      - task_key: phantom_stock_score_batch
    python_wheel_task:
      package_name: ahold_freshness_poc
      entry_point: phantom_write_alerts
    timeout_seconds: 900
  - task_key: phantom_stock_bapi_writeback
    depends_on:
      - task_key: phantom_stock_write_alerts
    python_wheel_task:
      package_name: ahold_freshness_poc
      entry_point: phantom_bapi_writeback
    timeout_seconds: 1800

  # --- Demand branch (parallel with phantom branch after feature store) ---
  - task_key: ingest_weather
    depends_on:
      - task_key: fs_refresh_collapse
    python_wheel_task:
      package_name: ahold_freshness_poc
      entry_point: demand_ingest_weather
    timeout_seconds: 300
  - task_key: score_m1
    depends_on:
      - task_key: ingest_weather
    python_wheel_task:
      package_name: ahold_freshness_poc
      entry_point: demand_score_m1
    timeout_seconds: 1800
  - task_key: score_m2
    depends_on:
      - task_key: score_m1
    python_wheel_task:
      package_name: ahold_freshness_poc
      entry_point: demand_score_m2
    timeout_seconds: 1800
  - task_key: score_m4
    depends_on:
      - task_key: score_m2
    python_wheel_task:
      package_name: ahold_freshness_poc
      entry_point: demand_score_m4
    timeout_seconds: 1800

  # --- Freshness branch (after demand models complete) ---
  - task_key: freshness_milp_solve
    depends_on:
      - task_key: score_m4
    python_wheel_task:
      package_name: ahold_freshness_poc
      entry_point: freshness_milp_solve
    timeout_seconds: 3600
  - task_key: freshness_write_recommendations
    depends_on:
      - task_key: freshness_milp_solve
    python_wheel_task:
      package_name: ahold_freshness_poc
      entry_point: freshness_write_recommendations
    timeout_seconds: 1800
  - task_key: freshness_bapi_po_create
    depends_on:
      - task_key: freshness_write_recommendations
    python_wheel_task:
      package_name: ahold_freshness_poc
      entry_point: freshness_bapi_po_create
    timeout_seconds: 3600
```

- [ ] **Step 2: Add 3 freshness console_scripts to `setup.py`**

In `setup.py`, after the `demand_score_m4` entry and before the closing bracket of the `console_scripts` list, add:

```python
            # Phase 3B — Freshness Orchestrator
            "freshness_milp_solve=engines.freshness.freshness_pipeline:_entry_freshness_milp_solve",
            "freshness_write_recommendations=engines.freshness.freshness_pipeline:_entry_write_recommendations",
            "freshness_bapi_po_create=engines.freshness.freshness_pipeline:_entry_bapi_po_create",
```

The complete updated `console_scripts` section of `setup.py`:

```python
    entry_points={
        "console_scripts": [
            # Phase 1 — Gold aggregates
            "gold_daily_positions=medallion.gold.aggregates:_entry_daily_positions",
            "gold_sales_velocity=medallion.gold.aggregates:_entry_sales_velocity",
            "gold_open_orders=medallion.gold.aggregates:_entry_open_orders",
            # Phase 2A — Feature Store
            "fs_velocity_features=medallion.feature_store.velocity_features:_entry_velocity_features",
            "fs_stock_features=medallion.feature_store.stock_features:_entry_stock_features",
            "fs_collapse_signals=medallion.feature_store.collapse_signals:_entry_collapse_signals",
            # Phase 2B — Phantom Stock Detector
            "phantom_generate_labels=engines.phantom_stock.label_generator:_entry_generate_labels",
            "phantom_train_model=engines.phantom_stock.classifier:_entry_train_model",
            "phantom_score_batch=engines.phantom_stock.feature_pipeline:_entry_score_batch",
            "phantom_write_alerts=engines.phantom_stock.feature_pipeline:_entry_write_alerts",
            "phantom_bapi_writeback=engines.phantom_stock.feature_pipeline:_entry_bapi_writeback",
            # Phase 3A — Demand Models (M1/M2/M4)
            "demand_train_m1=engines.demand.m1_model:_entry_train_m1",
            "demand_score_m1=engines.demand.m1_model:_entry_score_m1",
            "demand_ingest_weather=engines.demand.weather_ingest:_entry_ingest_weather",
            "demand_train_m2=engines.demand.m2_model:_entry_train_m2",
            "demand_score_m2=engines.demand.m2_model:_entry_score_m2",
            "demand_train_m4=engines.demand.m4_model:_entry_train_m4",
            "demand_score_m4=engines.demand.m4_model:_entry_score_m4",
            # Phase 3B — Freshness Orchestrator
            "freshness_milp_solve=engines.freshness.freshness_pipeline:_entry_freshness_milp_solve",
            "freshness_write_recommendations=engines.freshness.freshness_pipeline:_entry_write_recommendations",
            "freshness_bapi_po_create=engines.freshness.freshness_pipeline:_entry_bapi_po_create",
        ],
    },
```

- [ ] **Step 3: Update `databricks.yml`**

Replace the `phantom_stock_score` job entry with `morning_pipeline`. The updated `jobs` section of `databricks.yml`:

```yaml
  jobs:
    lakeflow_trigger:
      source: resources/jobs/lakeflow_trigger.yml
    phantom_stock_train:
      source: resources/jobs/phantom_stock_train.yml
    demand_forecast_train:
      source: resources/jobs/demand_forecast_train.yml
    morning_pipeline:
      source: resources/jobs/morning_pipeline.yml
```

*(Remove the `phantom_stock_score` line — its tasks are now inside `morning_pipeline`.)*

- [ ] **Step 4: Run final test suite**

```bash
pytest tests/unit/test_freshness_orchestrator.py -v
```

Expected: 8 passing

- [ ] **Step 5: Commit**

```bash
git add resources/jobs/morning_pipeline.yml setup.py databricks.yml
git commit -m "feat: unified morning_pipeline.yml (13-task depends_on chain) + 3 freshness console_scripts"
```

---

## Self-Review

### Spec coverage

| Spec requirement | Task |
|---|---|
| Extend `SolverInput` with `category`, `unit_cost`, `demand_p10` | Task 1 |
| Extend `OrderRecommendation` with approval/TTL/lift fields | Task 1 |
| `CategoryTtlConfig` + `apply_ttl_policy()` in `ttl_policy.py` | Task 2 |
| FRESH_PRODUCE hard block at ratio > 0.50 | Task 2, test 1 |
| FRESH_PRODUCE pass at ratio ≤ 0.50 | Task 2, test 2 |
| BAKERY soft cap at ratio > 0.50 | Task 2, test 3 |
| `ApprovalGate.evaluate()` — both gates required | Task 3 |
| AUTO_APPROVED when both gates pass | Task 3, test 4 |
| PENDING_REVIEW on high uncertainty | Task 3, test 5 |
| PENDING_REVIEW on high value | Task 3, test 6 |
| Replace hardcoded TTL check in `PuLPCBCSolver` | Task 4 |
| Solver end-to-end test | Task 4, test 7 |
| `POClient(BAPIClient)` with `create_purchase_order()` | Task 5 |
| `BAPIError` on HTTP 500 | Task 5, test 8 |
| `_entry_freshness_milp_solve()` | Task 6 |
| `_entry_write_recommendations()` with MLflow metrics | Task 6 |
| `_entry_bapi_po_create()` with sanity cap + audit append | Task 6 |
| `gold.replenishment.solver_output` intermediate table | Task 6 |
| `gold.replenishment.order_recommendations` full overwrite | Task 6 |
| `gold.replenishment.po_audit` append | Task 6 |
| `morning_pipeline.yml` — 13 tasks, all `depends_on` | Task 7 |
| 3 freshness console_scripts in `setup.py` | Task 7 |
| Remove `phantom_stock_score` from `databricks.yml` | Task 7 |

All requirements covered. No TBDs or placeholders.

### Type consistency

- `apply_ttl_policy(recommendation, solver_input)` — used with exactly those positional args in Tasks 2 and 4 ✓
- `ApprovalGate().evaluate(rec, inp)` — same signature in Tasks 3 and 6 ✓
- `POClient(endpoint_url=..., token=...)` — matches `BAPIClient.__init__` signature from Phase 2B ✓
- `SolverInput.demand_p10` / `demand_p50` / `demand_p90` — used consistently in gate calculations ✓
- `OrderRecommendation.ttl_policy_applied` — set in Task 2, asserted in Task 4 ✓
