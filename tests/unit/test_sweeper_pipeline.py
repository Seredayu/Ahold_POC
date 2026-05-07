import pytest


# ---------------------------------------------------------------------------
# Task 1 — SweeperStateMachine action evaluation (regression baseline)
# ---------------------------------------------------------------------------

def test_load_exceptions_auto_approve_low_deviation():
    from engines.sweeper.state_machine import SweeperStateMachine
    from engines.sweeper.decision_rules import SweeperAction

    sm = SweeperStateMachine()
    # deviation=0.10 < 0.25 threshold; TTL=0.3 < 0.5; 120 min to deadline
    action = sm.process_exception({
        "transit_to_life_ratio": 0.3,
        "quantity_deviation_pct": 0.10,
        "minutes_to_deadline": 120,
    })
    assert action == SweeperAction.AUTO_APPROVE


def test_load_exceptions_escalate_high_deviation():
    from engines.sweeper.state_machine import SweeperStateMachine
    from engines.sweeper.decision_rules import SweeperAction

    sm = SweeperStateMachine()
    # TTL=0.3 passes; deviation=0.40 > 0.25 → ESCALATE; 120 min → not at deadline cutoff
    action = sm.process_exception({
        "transit_to_life_ratio": 0.3,
        "quantity_deviation_pct": 0.40,
        "minutes_to_deadline": 120,
    })
    assert action == SweeperAction.ESCALATE


def test_load_exceptions_block_high_ttl():
    from engines.sweeper.state_machine import SweeperStateMachine
    from engines.sweeper.decision_rules import SweeperAction

    sm = SweeperStateMachine()
    # TTL=0.8 > 0.5 → TransitToLifeRule fires BLOCK immediately
    action = sm.process_exception({
        "transit_to_life_ratio": 0.8,
        "quantity_deviation_pct": 0.10,
        "minutes_to_deadline": 120,
    })
    assert action == SweeperAction.BLOCK


# ---------------------------------------------------------------------------
# Task 1 — _compute_deviation helper
# ---------------------------------------------------------------------------

def test_compute_deviation_normal():
    from engines.sweeper.sweeper_pipeline import _compute_deviation
    # abs(120 - 100) / 100 = 0.20
    assert abs(_compute_deviation(120, 100.0) - 0.20) < 1e-9


def test_compute_deviation_zero_p50():
    from engines.sweeper.sweeper_pipeline import _compute_deviation
    # p50=0 → return 0.0, not ZeroDivisionError
    assert _compute_deviation(80, 0.0) == 0.0
