from enum import Enum
from pydantic import BaseModel, Field


class SweeperAction(str, Enum):
    AUTO_APPROVE = "AUTO_APPROVE"
    ESCALATE = "ESCALATE"
    BLOCK = "BLOCK"


class ExceptionThresholds(BaseModel):
    # Quantity deviation above which a human must review
    quantity_deviation_pct: float = Field(default=0.25, ge=0.0, le=1.0)
    # Transit-to-Life ratio above which order is blocked
    transit_to_life_max: float = Field(default=0.5, ge=0.0, le=1.0)
    # Phantom stock confidence below which detector result is ignored
    phantom_confidence_min: float = Field(default=0.85, ge=0.0, le=1.0)
    # Minutes before EDI deadline at which unresolved exceptions are force-approved
    escalation_cutoff_minutes: int = Field(default=30, ge=0)


class SweeperDecisionRule(BaseModel):
    name: str
    description: str
    thresholds: ExceptionThresholds = Field(default_factory=ExceptionThresholds)

    def evaluate(self, context: dict) -> SweeperAction:
        raise NotImplementedError


class QuantityDeviationRule(SweeperDecisionRule):
    name: str = "quantity_deviation"
    description: str = "Auto-approve if quantity deviation is within threshold"

    def evaluate(self, context: dict) -> SweeperAction:
        deviation = abs(context.get("quantity_deviation_pct", 0.0))
        if deviation <= self.thresholds.quantity_deviation_pct:
            return SweeperAction.AUTO_APPROVE
        return SweeperAction.ESCALATE


class TransitToLifeRule(SweeperDecisionRule):
    name: str = "transit_to_life"
    description: str = "Block order if product will expire before reaching shelf"

    def evaluate(self, context: dict) -> SweeperAction:
        ratio = context.get("transit_to_life_ratio", 0.0)
        if ratio > self.thresholds.transit_to_life_max:
            return SweeperAction.BLOCK
        return SweeperAction.AUTO_APPROVE


class DeadlineCutoffRule(SweeperDecisionRule):
    name: str = "deadline_cutoff"
    description: str = "Force-approve unresolved exceptions near EDI deadline"

    def evaluate(self, context: dict) -> SweeperAction:
        minutes_remaining = context.get("minutes_to_deadline", 999)
        if minutes_remaining <= self.thresholds.escalation_cutoff_minutes:
            return SweeperAction.AUTO_APPROVE
        return SweeperAction.ESCALATE


DECISION_RULES: list[SweeperDecisionRule] = [
    TransitToLifeRule(thresholds=ExceptionThresholds()),
    QuantityDeviationRule(thresholds=ExceptionThresholds()),
    DeadlineCutoffRule(thresholds=ExceptionThresholds()),
]
