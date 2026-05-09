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
