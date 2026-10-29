"""The policy file has the final say over the LLM's band recommendation."""

import pytest
from pydantic import ValidationError

from dagfox.agents.models import Band, ProposedChange, Remediation, TriageResult
from dagfox.checks.entity_checks import CheckReport, CheckResult
from dagfox.policy.engine import Policy


def report(failed: dict[str, int], total: int = 1000) -> CheckReport:
    """A report where each named check failed on ``n`` distinct records."""
    results, offset = [], 0
    for name, n in failed.items():
        leis = [f"LEI{offset + i:017d}" for i in range(n)]
        offset += n
        results.append(CheckResult(name=name, description=name, passed=False, affected_leis=leis))
    return CheckReport(batch_id="b1", total_rows=total, published_rows=total, results=results)


def triage(band: Band, confidence: float = 0.9) -> TriageResult:
    return TriageResult(band=band, confidence=confidence, reason="model reasoning here", primary_issue="dupes")


def test_policy_overrides_llm_when_affected_share_exceeds_threshold(policy):
    # 80 of 1000 records (8%) > 5% threshold: HUMAN_LED, whatever the LLM says.
    decision = policy.decide(report({"unsourced_change": 80}), triage(Band.SUPERVISED))
    assert decision.band == Band.HUMAN_LED
    assert decision.llm_band == Band.SUPERVISED
    assert decision.overridden
    assert any("> 5.0%" in r for r in decision.rules_fired)


def test_policy_floor_beats_a_more_permissive_llm(policy):
    decision = policy.decide(report({"duplicate_lei": 3}), triage(Band.AUTO))
    assert decision.band == Band.SUPERVISED
    assert decision.overridden


def test_llm_may_escalate_but_never_relax(policy):
    decision = policy.decide(report({"duplicate_lei": 3}), triage(Band.HUMAN_LED))
    assert decision.band == Band.HUMAN_LED
    assert not decision.overridden


def test_policy_agrees_when_llm_matches(policy):
    decision = policy.decide(report({"country_format": 10}), triage(Band.SUPERVISED))
    assert decision.band == Band.SUPERVISED
    assert not decision.overridden


def test_low_confidence_escalates_one_band(policy):
    decision = policy.decide(report({"country_format": 10}), triage(Band.SUPERVISED, confidence=0.4))
    assert decision.band == Band.HUMAN_LED


def test_check_without_safe_fix_is_human_led(policy):
    decision = policy.decide(report({"lei_format": 1}), triage(Band.SUPERVISED))
    assert decision.band == Band.HUMAN_LED


def test_unknown_check_defaults_to_human_led(policy):
    decision = policy.decide(report({"brand_new_check": 1}), triage(Band.SUPERVISED))
    assert decision.band == Band.HUMAN_LED


def test_missing_or_invalid_triage_routes_to_human_led(policy):
    decision = policy.decide(report({"country_format": 1}), None)
    assert decision.band == Band.HUMAN_LED
    assert "failed validation" in decision.rules_fired[-1]


def _change(source: str = "published.entities (last approved version)", field: str = "legal_address_country"):
    return ProposedChange(record_id="5493001KJTIIGC8Y1R12", field=field, old_value="FR", new_value="DE",
                          rationale="revert unsourced change", source=source)


def test_remediation_with_unapproved_source_escalates(policy):
    decision = policy.decide(report({"unsourced_change": 1}), triage(Band.SUPERVISED))
    reviewed = policy.review_remediation(
        decision, Remediation(changes=[_change(source="a blog post")], summary="one fix proposed"))
    assert reviewed.band == Band.HUMAN_LED


def test_remediation_touching_non_correctable_field_escalates(policy):
    decision = policy.decide(report({"unsourced_change": 1}), triage(Band.SUPERVISED))
    reviewed = policy.review_remediation(
        decision, Remediation(changes=[_change(field="legal_name")], summary="one fix proposed"))
    assert reviewed.band == Band.HUMAN_LED


def test_oversized_remediation_escalates(policy):
    decision = policy.decide(report({"unsourced_change": 1}), triage(Band.SUPERVISED))
    too_many = [_change()] * (policy.policy.thresholds.max_supervised_changes + 1)
    reviewed = policy.review_remediation(decision, Remediation(changes=too_many, summary="lots of fixes"))
    assert reviewed.band == Band.HUMAN_LED


def test_valid_remediation_keeps_supervised(policy):
    decision = policy.decide(report({"unsourced_change": 1}), triage(Band.SUPERVISED))
    reviewed = policy.review_remediation(decision, Remediation(changes=[_change()], summary="one fix proposed"))
    assert reviewed.band == Band.SUPERVISED


def test_late_file_is_auto_within_window_then_escalates(policy):
    assert policy.late_file(5).band == Band.AUTO
    assert policy.allows(Band.AUTO, "wait_and_retry")
    assert policy.late_file(policy.policy.late_file.max_wait_minutes + 1).band == Band.HUMAN_LED


def test_human_led_band_cannot_write(policy):
    assert not policy.allows(Band.HUMAN_LED, "propose_diff")
    assert not policy.allows(Band.AUTO, "propose_diff")


def test_malformed_policy_file_fails_loudly(tmp_path):
    bad = tmp_path / "policy.yaml"
    bad.write_text("version: 1\nthresholds: {human_led_affected_pct: 500}\n")
    with pytest.raises(ValidationError):
        Policy.load(bad)
