"""LLM output is only used once it validates; anything else routes to HUMAN_LED."""

import pytest
from helpers import bad_batch, failing_model, insert_batch, scripted_model
from pydantic import ValidationError

from dagfox.agents.models import Band, Explanation, ProposedChange, TriageResult
from dagfox.agents.triage import triage

VALID_LEI = "5493001KJTIIGC8Y1R12"


def test_triage_rejects_out_of_range_confidence():
    with pytest.raises(ValidationError):
        TriageResult(band="SUPERVISED", confidence=1.5, reason="valid reason text", primary_issue="x")


def test_triage_rejects_unknown_band_and_extra_fields():
    with pytest.raises(ValidationError):
        TriageResult(band="YOLO", confidence=0.5, reason="valid reason text", primary_issue="x")
    with pytest.raises(ValidationError):
        TriageResult(band="AUTO", confidence=0.5, reason="valid reason text", primary_issue="x", sql="DROP")


def test_proposed_change_requires_a_real_lei_and_source():
    with pytest.raises(ValidationError):
        ProposedChange(record_id="not-an-lei", field="legal_address_country", old_value="fr", new_value="FR",
                       rationale="normalize", source="ISO 3166-1")
    with pytest.raises(ValidationError):
        ProposedChange(record_id=VALID_LEI, field="legal_address_country", old_value="fr", new_value="FR",
                       rationale="normalize", source="")


def test_proposed_change_shape_rules():
    with pytest.raises(ValidationError, match="requires a field"):
        ProposedChange(record_id=VALID_LEI, rationale="fix it", source="ISO 3166-1")
    with pytest.raises(ValidationError, match="must differ"):
        ProposedChange(record_id=VALID_LEI, field="hq_address_country", old_value="FR", new_value="FR",
                       rationale="no-op", source="ISO 3166-1")
    with pytest.raises(ValidationError, match="drop_duplicate"):
        ProposedChange(record_id=VALID_LEI, action="drop_duplicate", field="lei", rationale="duplicate rows",
                       source="duplicate removal")


def test_explanation_must_be_three_to_five_sentences():
    with pytest.raises(ValidationError):
        Explanation(headline="Short headline", summary="Only one sentence here, which is not enough.")
    Explanation(headline="Short headline", summary="One thing broke. It hit 4 records. A fix awaits approval.")


def test_malformed_llm_output_yields_no_result():
    handle = scripted_model({"band": "PROBABLY_FINE", "confidence": "high"})
    from dagfox.checks.entity_checks import CheckReport

    run, result = triage(CheckReport(batch_id="b", total_rows=1, published_rows=1, results=[]), handle)
    assert result is None
    assert not run.ok and run.error


def test_invalid_llm_output_routes_pipeline_to_human_led(engine, published, make_pipeline, ctx):
    insert_batch(engine, "b1", bad_batch(published))
    pipeline, _ = make_pipeline({"triage": scripted_model({"band": "SUPERVISED", "confidence": 7})})
    decision = pipeline.triage(ctx, pipeline.check_batch("b1"))
    assert decision.band == Band.HUMAN_LED
    rows = pipeline.audit.rows(ctx.dag_run_id)
    assert rows[0]["event_type"] == "agent_decision"
    assert rows[0]["final_action"] == "invalid_output_routed_to_human"


def test_provider_failure_routes_pipeline_to_human_led(engine, published, make_pipeline, ctx):
    insert_batch(engine, "b1", bad_batch(published))
    pipeline, _ = make_pipeline({"triage": failing_model()})
    assert pipeline.triage(ctx, pipeline.check_batch("b1")).band == Band.HUMAN_LED


def test_invalid_remediation_escalates_instead_of_proposing(engine, published, make_pipeline, ctx, policy):
    insert_batch(engine, "b1", bad_batch(published))
    explanation = {"headline": "4 records need attention",
                   "summary": "Some records are wrong. Four are affected. A fix is being prepared."}
    pipeline, notifier = make_pipeline({
        "triage": scripted_model({"band": "SUPERVISED", "confidence": 0.9, "reason": "record-level issues",
                                  "primary_issue": "dupes"}),
        "explain": scripted_model(explanation),
        "remediate": scripted_model({"changes": [{"record_id": "nope"}], "summary": "bad"}),
    })
    report = pipeline.check_batch("b1")
    proposal = pipeline.propose_fix(ctx, report, pipeline.triage(ctx, report))
    assert proposal["band"] == Band.HUMAN_LED
    assert "subject_sha256" not in proposal  # nothing to approve
    assert notifier.sent == []  # no approval request went out
