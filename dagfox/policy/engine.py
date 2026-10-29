"""Policy engine: turns check results + an (optional) LLM recommendation into a band.

The engine is deterministic and has the final say. The LLM's recommendation
can only make the outcome stricter, never more permissive.
"""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field

from dagfox.agents.models import Band, Remediation, TriageResult
from dagfox.checks.entity_checks import CheckReport


class Thresholds(BaseModel):
    human_led_affected_pct: float = Field(gt=0, le=100)
    min_triage_confidence: float = Field(ge=0, le=1)
    max_supervised_changes: int = Field(gt=0)


class LateFile(BaseModel):
    band: Band
    max_wait_minutes: int = Field(gt=0)
    escalate_to: Band


class RemediationRules(BaseModel):
    allowed_sources: list[str]
    correctable_fields: list[str]


class Approvals(BaseModel):
    approvers: list[str] = Field(min_length=1)
    response_timeout_hours: int = Field(gt=0)


class PublishRules(BaseModel):
    require_hitl_for_clean_loads: bool = False


class Policy(BaseModel):
    """policy.yaml, validated. A malformed policy file fails loudly at load time."""

    model_config = ConfigDict(extra="forbid")

    version: int
    thresholds: Thresholds
    check_bands: dict[str, Band]
    default_check_band: Band
    late_file: LateFile
    allowed_actions: dict[Band, list[str]]
    remediation: RemediationRules
    approvals: Approvals
    publish: PublishRules

    @classmethod
    def load(cls, path: Path) -> Policy:
        return cls.model_validate(yaml.safe_load(Path(path).read_text()))


class PolicyDecision(BaseModel):
    band: Band
    reason: str
    rules_fired: list[str]
    policy_floor: Band
    llm_band: Band | None = None
    llm_confidence: float | None = None
    overridden: bool = False


class PolicyEngine:
    def __init__(self, policy: Policy):
        self.policy = policy

    @classmethod
    def from_file(cls, path: Path) -> PolicyEngine:
        return cls(Policy.load(path))

    def allows(self, band: Band, action: str) -> bool:
        return action in self.policy.allowed_actions.get(band, [])

    def floor(self, report: CheckReport) -> tuple[Band, list[str]]:
        """The minimum band the check results alone require."""
        p = self.policy
        band, fired = Band.AUTO, []
        for result in report.failed:
            check_band = p.check_bands.get(result.name, p.default_check_band)
            fired.append(f"check {result.name} failed -> {check_band}")
            band = Band.strictest(band, check_band)
        if report.affected_pct > p.thresholds.human_led_affected_pct:
            fired.append(
                f"{report.affected_pct:.1f}% of records affected > "
                f"{p.thresholds.human_led_affected_pct}% -> HUMAN_LED"
            )
            band = Band.HUMAN_LED
        return band, fired

    def decide(self, report: CheckReport, triage: TriageResult | None) -> PolicyDecision:
        floor, fired = self.floor(report)

        if triage is None:
            fired.append("triage output missing or failed validation -> HUMAN_LED")
            return PolicyDecision(
                band=Band.HUMAN_LED,
                reason="The triage agent did not return a valid result, so a person must review.",
                rules_fired=fired,
                policy_floor=floor,
            )

        band = Band.strictest(floor, triage.band)
        if triage.band.rank < floor.rank:
            fired.append(f"LLM recommended {triage.band}, policy floor is {floor}")
        if triage.confidence < self.policy.thresholds.min_triage_confidence:
            escalated = list(Band)[min(band.rank + 1, len(Band) - 1)]
            fired.append(
                f"triage confidence {triage.confidence:.2f} < "
                f"{self.policy.thresholds.min_triage_confidence} -> escalate {band} to {escalated}"
            )
            band = escalated

        overridden = band != triage.band
        reason = (
            f"Policy set {band} (LLM recommended {triage.band})." if overridden
            else f"Policy agrees with the LLM recommendation of {band}."
        )
        return PolicyDecision(
            band=band,
            reason=reason,
            rules_fired=fired,
            policy_floor=floor,
            llm_band=triage.band,
            llm_confidence=triage.confidence,
            overridden=overridden,
        )

    def review_remediation(self, decision: PolicyDecision, remediation: Remediation) -> PolicyDecision:
        """Escalate to HUMAN_LED if a proposed diff breaks the remediation rules."""
        rules = self.policy.remediation
        problems = []
        if len(remediation.changes) > self.policy.thresholds.max_supervised_changes:
            problems.append(
                f"{len(remediation.changes)} changes > max {self.policy.thresholds.max_supervised_changes}"
            )
        if not remediation.changes:
            problems.append("remediation proposed no changes")
        for c in remediation.changes:
            if c.action == "update_field" and c.field not in rules.correctable_fields:
                problems.append(f"{c.record_id}: field {c.field} is not correctable by policy")
            if not any(c.source.startswith(s) for s in rules.allowed_sources):
                problems.append(f"{c.record_id}: source {c.source!r} is not an allowed source")
        if not problems:
            return decision
        return decision.model_copy(
            update={
                "band": Band.HUMAN_LED,
                "reason": "The proposed fix broke policy, so a person must review: " + "; ".join(problems[:3]),
                "rules_fired": decision.rules_fired + problems,
                "overridden": True,
            }
        )

    def late_file(self, waited_minutes: float) -> PolicyDecision:
        lf = self.policy.late_file
        if waited_minutes <= lf.max_wait_minutes:
            return PolicyDecision(
                band=lf.band,
                reason=f"File not yet arrived after {waited_minutes:.0f} min; within the "
                f"{lf.max_wait_minutes} min window, so wait and retry.",
                rules_fired=[f"late_file within {lf.max_wait_minutes} min -> {lf.band}"],
                policy_floor=lf.band,
            )
        return PolicyDecision(
            band=lf.escalate_to,
            reason=f"File still missing after {waited_minutes:.0f} min; escalating.",
            rules_fired=[f"late_file over {lf.max_wait_minutes} min -> {lf.escalate_to}"],
            policy_floor=lf.escalate_to,
        )
