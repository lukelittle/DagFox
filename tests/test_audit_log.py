"""One audit row per agent decision and per human decision, end to end with a mocked LLM."""

from collections import Counter

import pytest
from helpers import bad_batch, insert_batch, scripted_model

from dagfox.agents.models import Band
from dagfox.audit.log import EXCERPT_CHARS
from dagfox.pipeline import APPROVE, REJECT
from dagfox.publish.writer import published_snapshot

EXPLANATION = {
    "headline": "4 entity records need attention",
    "summary": "Four records in today's file are wrong. Two are duplicates and two have bad countries. "
               "A proposed fix is waiting for approval.",
}


def remediation_for(rows):
    return {
        "summary": "Drop two duplicates, fix one country code, revert one unsourced change.",
        "changes": [
            {"record_id": rows[0]["lei"], "action": "drop_duplicate", "rationale": "identical rows",
             "source": "duplicate removal (identical rows)"},
            {"record_id": rows[1]["lei"], "action": "drop_duplicate", "rationale": "identical rows",
             "source": "duplicate removal (identical rows)"},
            {"record_id": rows[2]["lei"], "field": "legal_address_country",
             "old_value": rows[2]["legal_address_country"].lower(),
             "new_value": rows[2]["legal_address_country"], "rationale": "wrong case",
             "source": "ISO 3166-1 alpha-2 normalization"},
            {"record_id": rows[3]["lei"], "field": "legal_address_country",
             "old_value": "FR" if rows[3]["legal_address_country"] != "FR" else "DE",
             "new_value": rows[3]["legal_address_country"], "rationale": "changed without a source update",
             "source": "published.entities (last approved version)"},
        ],
    }


@pytest.fixture
def supervised_run(engine, published, make_pipeline, ctx):
    """Runs triage -> propose_fix for a SUPERVISED batch; returns (pipeline, proposal, notifier)."""
    insert_batch(engine, "b1", bad_batch(published))
    pipeline, notifier = make_pipeline({
        "triage": scripted_model({"band": "SUPERVISED", "confidence": 0.9,
                                  "reason": "record-level issues with clear fixes", "primary_issue": "dupes"}),
        "explain": scripted_model(EXPLANATION),
        "remediate": scripted_model(remediation_for(published)),
    })
    report = pipeline.check_batch("b1")
    proposal = pipeline.propose_fix(ctx, report, pipeline.triage(ctx, report))
    assert proposal["band"] == Band.SUPERVISED
    return pipeline, proposal, notifier


def counts(pipeline, ctx) -> Counter:
    return Counter(r["event_type"] for r in pipeline.audit.rows(ctx.dag_run_id))


def test_one_row_per_agent_decision(supervised_run, ctx):
    pipeline, _, _ = supervised_run
    rows = [r for r in pipeline.audit.rows(ctx.dag_run_id) if r["event_type"] == "agent_decision"]
    assert [r["actor"] for r in rows] == ["triage", "explain", "remediate"]
    assert all(r["model_name"] == "mock-llm" and r["model_provider"] == "mock" for r in rows)
    assert counts(pipeline, ctx) == {"agent_decision": 3, "policy_decision": 1, "proposed_fix": 1}


def test_agent_inputs_are_hashed_and_truncated(supervised_run, ctx):
    pipeline, _, _ = supervised_run
    for row in pipeline.audit.rows(ctx.dag_run_id):
        if row["event_type"] == "agent_decision":
            assert len(row["input_sha256"]) == 64
            assert len(row["input_excerpt"]) <= EXCERPT_CHARS


def test_approve_records_one_human_decision_then_publishes(supervised_run, ctx, engine, published):
    pipeline, proposal, notifier = supervised_run
    assert notifier.sent[-1].link.endswith("/tasks/approve_fix/required_actions?map_index=-1")

    approval_id = pipeline.record_human_decision(ctx, proposal, APPROVE, "data_steward", "looks right")
    result = pipeline.publish(ctx, "b1", proposal["changes"], approval_id)

    c = counts(pipeline, ctx)
    assert c["human_decision"] == 1 and c["agent_decision"] == 3 and c["publish"] == 1
    human = next(r for r in pipeline.audit.rows(ctx.dag_run_id) if r["event_type"] == "human_decision")
    assert (human["approver"], human["human_decision"]) == ("data_steward", APPROVE)
    assert result.updated == 0  # the fix restored the published values exactly
    assert published_snapshot(engine) == {r["lei"]: r for r in published}


def test_reject_records_one_human_decision_and_no_write(supervised_run, ctx, engine):
    pipeline, proposal, _ = supervised_run
    before = published_snapshot(engine)
    approval_id = pipeline.record_human_decision(ctx, proposal, REJECT, "data_steward")
    with pytest.raises(PermissionError):
        pipeline.publish(ctx, "b1", proposal["changes"], approval_id)
    c = counts(pipeline, ctx)
    assert c["human_decision"] == 1 and c["publish"] == 0
    assert published_snapshot(engine) == before


def test_hash_chain_detects_tampering(supervised_run, engine):
    pipeline, _, _ = supervised_run
    assert pipeline.audit.verify()[0]
    # Simulate a superuser editing history with triggers disabled.
    raw = engine.raw_connection()
    raw.execute("DROP TRIGGER dagfox.audit_log_no_update")
    raw.execute("UPDATE dagfox.audit_log SET approver = 'mallory' WHERE seq = 2")
    raw.commit()
    ok, _, bad_seq = pipeline.audit.verify()
    assert not ok and bad_seq == 2


def test_human_led_escalation_writes_summary_only(engine, published, make_pipeline, ctx):
    rows = [dict(r) for r in published]
    for r in rows[:10]:  # 10% of records changed with no source update
        r["legal_form_code"] = "8888"
    insert_batch(engine, "b2", rows)
    pipeline, notifier = make_pipeline({
        "triage": scripted_model({"band": "SUPERVISED", "confidence": 0.9, "reason": "looks fixable to me",
                                  "primary_issue": "legal form"}),
        "explain": scripted_model(EXPLANATION),
    })
    report = pipeline.check_batch("b2")
    decision = pipeline.triage(ctx, report)
    assert decision.band == Band.HUMAN_LED and decision.overridden
    pipeline.escalate(ctx, report, decision)
    c = counts(pipeline, ctx)
    assert c == {"agent_decision": 2, "policy_decision": 1, "escalation": 1}
    assert notifier.sent[-1].band == Band.HUMAN_LED and notifier.sent[-1].link is None
