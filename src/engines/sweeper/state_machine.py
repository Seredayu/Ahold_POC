from datetime import datetime, time
from enum import Enum

from .decision_rules import DECISION_RULES, SweeperAction


EDI_DEADLINE = time(8, 15)


class SweeperState(str, Enum):
    IDLE = "IDLE"
    MONITORING = "MONITORING"
    PROCESSING_EXCEPTIONS = "PROCESSING_EXCEPTIONS"
    TRIGGERING_EDI = "TRIGGERING_EDI"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"


class SweeperStateMachine:
    """Deterministic state machine managing the 05:00–08:15 AM replenishment window.

    NOT LLM-based — purchase orders require full auditability.
    All decision logic lives in decision_rules.py as Pydantic models.
    """

    def __init__(self):
        self.state = SweeperState.IDLE
        self.processed: list[dict] = []
        self.blocked: list[dict] = []
        self.escalated: list[dict] = []

    def _minutes_to_deadline(self) -> int:
        now = datetime.now().time()
        deadline = datetime.combine(datetime.today(), EDI_DEADLINE)
        current = datetime.combine(datetime.today(), now)
        delta = (deadline - current).total_seconds() / 60
        return max(0, int(delta))

    def process_exception(self, exception: dict) -> SweeperAction:
        context = {**exception, "minutes_to_deadline": self._minutes_to_deadline()}

        for rule in DECISION_RULES:
            action = rule.evaluate(context)
            if action != SweeperAction.AUTO_APPROVE:
                return action

        return SweeperAction.AUTO_APPROVE

    def run(self, exceptions: list[dict]) -> dict:
        self.state = SweeperState.MONITORING

        self.state = SweeperState.PROCESSING_EXCEPTIONS
        for exc in exceptions:
            action = self.process_exception(exc)
            if action == SweeperAction.AUTO_APPROVE:
                self.processed.append(exc)
            elif action == SweeperAction.BLOCK:
                self.blocked.append(exc)
            else:
                self.escalated.append(exc)

        self.state = SweeperState.TRIGGERING_EDI
        self.state = SweeperState.COMPLETE

        return {
            "state": self.state,
            "processed": len(self.processed),
            "blocked": len(self.blocked),
            "escalated": len(self.escalated),
            "no_touch_rate": len(self.processed) / max(len(exceptions), 1),
        }
