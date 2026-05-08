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


# ---------------------------------------------------------------------------
# Task 2 — _classify_finalize_row
# ---------------------------------------------------------------------------

def test_classify_finalize_auto_approve():
    from engines.sweeper.sweeper_pipeline import _classify_finalize_row

    needs_bapi, status = _classify_finalize_row({
        "sweeper_action": "AUTO_APPROVE",
        "manager_decision": None,
    })
    assert needs_bapi is True
    assert status == "SWEEPER_AUTO_APPROVED"


def test_classify_finalize_force_approves_unresolved():
    from engines.sweeper.sweeper_pipeline import _classify_finalize_row

    # ESCALATE with no manager decision → force-approve at deadline
    needs_bapi, status = _classify_finalize_row({
        "sweeper_action": "ESCALATE",
        "manager_decision": None,
    })
    assert needs_bapi is True
    assert status == "FORCE_APPROVED"


def test_classify_finalize_skips_rejected():
    from engines.sweeper.sweeper_pipeline import _classify_finalize_row

    needs_bapi, status = _classify_finalize_row({
        "sweeper_action": "ESCALATE",
        "manager_decision": "REJECTED",
    })
    assert needs_bapi is False
    assert status == "MANAGER_REJECTED"


def test_classify_finalize_block():
    from engines.sweeper.sweeper_pipeline import _classify_finalize_row

    needs_bapi, status = _classify_finalize_row({
        "sweeper_action": "BLOCK",
        "manager_decision": None,
    })
    assert needs_bapi is False
    assert status == "BLOCKED"


# ---------------------------------------------------------------------------
# Task 2 — _build_edi_lines_by_vendor
# ---------------------------------------------------------------------------

def test_build_edi_lines_by_vendor_groups_correctly():
    from engines.sweeper.sweeper_pipeline import _build_edi_lines_by_vendor

    rows = [
        {
            "po_number": "4500001234", "recommended_qty": 50,
            "ean_barcode": "8710624212483", "vendor_id": "V001",
            "unit_cost": 2.5, "transit_days": 2,
        },
        {
            "po_number": "4500001235", "recommended_qty": 30,
            "ean_barcode": "8710624212490", "vendor_id": "V002",
            "unit_cost": 1.8, "transit_days": 1,
        },
        {
            "po_number": "4500001236", "recommended_qty": 20,
            "ean_barcode": "8710624212507", "vendor_id": "V001",
            "unit_cost": 2.5, "transit_days": 2,
        },
    ]
    result = _build_edi_lines_by_vendor(rows)

    assert set(result.keys()) == {"V001", "V002"}
    assert len(result["V001"]) == 2      # two SKUs for same vendor
    assert len(result["V002"]) == 1
    assert result["V001"][0].quantity == 50
    assert result["V001"][0].ean_barcode == "8710624212483"
    assert result["V001"][1].line_number == 2  # line numbers increment per vendor
    assert result["V002"][0].quantity == 30
