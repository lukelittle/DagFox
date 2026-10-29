"""Nothing writes to published.* without a matching, approved approval record."""

import pytest
from helpers import bad_batch, insert_batch
from sqlalchemy import text

from dagfox.agents.models import ProposedChange
from dagfox.checks.entity_checks import run_checks
from dagfox.publish.diff import DiffConflict, subject_hash
from dagfox.publish.writer import PublishedWriter, UnapprovedWriteError, published_snapshot, record_approval


def _fix_for(rows) -> list[ProposedChange]:
    return [
        ProposedChange(record_id=rows[0]["lei"], action="drop_duplicate", rationale="identical rows",
                       source="duplicate removal (identical rows)"),
        ProposedChange(record_id=rows[1]["lei"], action="drop_duplicate", rationale="identical rows",
                       source="duplicate removal (identical rows)"),
        ProposedChange(record_id=rows[2]["lei"], field="legal_address_country",
                       old_value=rows[2]["legal_address_country"],
                       new_value=rows[2]["legal_address_country"].upper(),
                       rationale="wrong case", source="ISO 3166-1 alpha-2 normalization"),
    ]


@pytest.fixture
def staged(engine, published):
    rows = bad_batch(published)
    rows[3] = dict(published[3])  # keep only fixable problems for these tests
    insert_batch(engine, "b1", rows)
    return rows


def _published_count(engine) -> int:
    with engine.connect() as conn:
        return conn.execute(text("SELECT count(*) FROM published.entities")).scalar()


def test_write_without_approval_raises(engine, staged):
    before = published_snapshot(engine)
    with pytest.raises(UnapprovedWriteError):
        PublishedWriter(engine).publish_batch("b1", _fix_for(staged), approval_id=None)
    assert published_snapshot(engine) == before


def test_write_with_unknown_approval_raises(engine, staged):
    with pytest.raises(UnapprovedWriteError, match="does not exist"):
        PublishedWriter(engine).publish_batch("b1", _fix_for(staged), approval_id="made-up")


def test_rejected_approval_cannot_be_used_to_write(engine, staged):
    changes = _fix_for(staged)
    approval = record_approval(engine, kind="hitl", subject_sha256=subject_hash(staged, changes),
                               decision="rejected", approver="data_steward")
    with pytest.raises(UnapprovedWriteError, match="rejected"):
        PublishedWriter(engine).publish_batch("b1", changes, approval)


def test_approval_only_covers_the_exact_diff_that_was_approved(engine, staged):
    changes = _fix_for(staged)
    approval = record_approval(engine, kind="hitl", subject_sha256=subject_hash(staged, changes[:1]),
                               decision="approved", approver="data_steward")
    with pytest.raises(UnapprovedWriteError, match="different batch or diff"):
        PublishedWriter(engine).publish_batch("b1", changes, approval)


def test_approval_is_invalidated_if_the_staged_batch_changes(engine, staged):
    changes = _fix_for(staged)
    approval = record_approval(engine, kind="hitl", subject_sha256=subject_hash(staged, changes),
                               decision="approved", approver="data_steward")
    with engine.begin() as conn:
        conn.execute(text("UPDATE staging.entities SET legal_name = 'Sneaky Ltd' WHERE lei = :l"),
                     {"l": staged[10]["lei"]})
    with pytest.raises(UnapprovedWriteError):
        PublishedWriter(engine).publish_batch("b1", changes, approval)


def test_approved_diff_is_published(engine, staged, published):
    changes = _fix_for(staged)
    approval = record_approval(engine, kind="hitl", subject_sha256=subject_hash(staged, changes),
                               decision="approved", approver="data_steward")
    result = PublishedWriter(engine).publish_batch("b1", changes, approval)
    assert result.updated == 0 and result.inserted == 0  # the fix restores exactly the published values
    assert _published_count(engine) == len(published)
    assert run_checks("b1", list(published_snapshot(engine).values()), published_snapshot(engine)).passed


def test_diff_that_does_not_match_the_data_is_rejected_whole(engine, staged):
    wrong = [ProposedChange(record_id=staged[2]["lei"], field="legal_address_country", old_value="ZZ",
                            new_value="DE", rationale="hallucinated", source="ISO 3166-1")]
    approval = record_approval(engine, kind="hitl", subject_sha256=subject_hash(staged, wrong),
                               decision="approved", approver="data_steward")
    with pytest.raises(DiffConflict):
        PublishedWriter(engine).publish_batch("b1", wrong, approval)


def test_clean_load_policy_approval_refuses_a_failing_batch(engine, staged, make_pipeline, ctx):
    pipeline, _ = make_pipeline({})
    with pytest.raises(ValueError, match="passed every check"):
        pipeline.authorize_clean_load(ctx, pipeline.check_batch("b1"))


def test_audit_log_is_append_only(engine, make_pipeline, ctx):
    pipeline, _ = make_pipeline({})
    pipeline.audit.record("auto_action", "policy", dag_run_id=ctx.dag_run_id)
    with pytest.raises(Exception, match="append-only"), engine.begin() as conn:
        conn.execute(text("UPDATE dagfox.audit_log SET actor = 'someone_else'"))
    with pytest.raises(Exception, match="append-only"), engine.begin() as conn:
        conn.execute(text("DELETE FROM dagfox.audit_log"))
