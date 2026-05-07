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

    # Soft cap: scale quantity down proportionally to remaining shelf-life fraction.
    # Clamp to 0 — ratio > 1.0 (transit_days > shelf_life_days) would otherwise produce
    # a negative quantity. When clamped to 0, fall through to hard block so no negative
    # or zero-quantity soft-cap recommendations reach gold tables.
    reduced_qty = max(0, round(recommendation.recommended_qty * (1.0 - ratio)))
    if reduced_qty == 0:
        return dataclasses.replace(
            recommendation,
            recommended_qty=0,
            blocked=True,
            solver_status="BLOCKED_TRANSIT_TO_LIFE",
            ttl_policy_applied="HARD_BLOCK",
        )
    return dataclasses.replace(
        recommendation,
        recommended_qty=reduced_qty,
        day_old_discount=True,
        ttl_policy_applied="SOFT_CAP",
    )
