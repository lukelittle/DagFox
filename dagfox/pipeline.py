"""Pipeline steps for entities_daily, kept free of Airflow so they are unit-testable.

The DAG wires these together. Every agent call, policy decision, human decision
and action taken here produces exactly one audit row.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

from sqlalchemy import Engine, text

from dagfox.agents.explain import explain, fallback_explanation
from dagfox.agents.models import AgentRun, Band, Explanation, ProposedChange
from dagfox.agents.remediate import remediate
from dagfox.agents.triage import triage
from dagfox.audit.log import AuditLog
from dagfox.checks.entity_checks import CheckReport, run_checks
from dagfox.db import ENTITY_FIELDS
from dagfox.llm.adapter import ModelHandle
from dagfox.notify import Notification, Notifier
from dagfox.policy.engine import PolicyDecision, PolicyEngine
from dagfox.publish.diff import DiffConflict, apply_diff, canonical_json, subject_hash
from dagfox.publish.writer import PublishedWriter, PublishResult, fetch_batch, published_snapshot, record_approval
from dagfox.sources.landing import LandingZone

APPROVE, REJECT = "Approve", "Reject"
MAX_DIFF_ROWS_SHOWN = 60


@dataclass(frozen=True)
class RunContext:
    dag_id: str
    dag_run_id: str
    task_id: str | None = None
    base_url: str = "http://localhost:8080"

    def approval_url(self, task_id: str = "approve_fix") -> str:
        return (
            f"{self.base_url}/dags/{self.dag_id}/runs/{quote(self.dag_run_id, safe='')}"
            f"/tasks/{task_id}/required_actions?map_index=-1"
        )


class FixProposal(dict):
    """XCom-friendly result of propose_fix (a plain dict with known keys)."""


class EntityPipeline:
    def __init__(
        self,
        engine: Engine,
        policy: PolicyEngine,
        model_factory: Callable[[str], ModelHandle],
        notifier: Notifier,
    ):
        self.engine = engine
        self.policy = policy
        self.model_for = model_factory
        self.notifier = notifier
        self.audit = AuditLog(engine)
        self.writer = PublishedWriter(engine)

    # ------------------------------------------------------------------ load + check
    def load_staging(self, batch_id: str, path: Path) -> int:
        """(Re)load one batch into staging.entities. Idempotent per batch_id."""
        rows = LandingZone.read(path)
        cols = ", ".join(ENTITY_FIELDS)
        placeholders = ", ".join(f":{f}" for f in ENTITY_FIELDS)
        with self.engine.begin() as conn:
            conn.execute(text("DELETE FROM staging.entities WHERE batch_id = :b"), {"b": batch_id})
            if rows:
                conn.execute(
                    text(f"INSERT INTO staging.entities ({cols}, batch_id) VALUES ({placeholders}, :batch_id)"),
                    [{f: r.get(f) for f in ENTITY_FIELDS} | {"batch_id": batch_id} for r in rows],
                )
        return len(rows)

    def check_batch(self, batch_id: str) -> CheckReport:
        return run_checks(batch_id, fetch_batch(self.engine, batch_id), published_snapshot(self.engine))

    # ------------------------------------------------------------------ AUTO: late file
    def late_file_poke(self, ctx: RunContext, arrived: bool, waited_minutes: float) -> None:
        if arrived:
            if self._count_actions(ctx, "wait_and_retry"):
                self.audit.record("auto_action", "policy", **self._ids(ctx), band=Band.AUTO,
                                  output={"reason": f"File arrived after {waited_minutes:.1f} min."},
                                  final_action="resumed_after_late_file")
            return
        decision = self.policy.late_file(waited_minutes)
        self.audit.record("auto_action", "policy", **self._ids(ctx), band=decision.band,
                          output=decision.model_dump(mode="json"), final_action="wait_and_retry")

    def late_file_timed_out(self, ctx: RunContext, waited_minutes: float) -> None:
        decision = self.policy.late_file(waited_minutes)
        self.audit.record("escalation", "policy", **self._ids(ctx), band=decision.band,
                          output=decision.model_dump(mode="json"), final_action="escalated_missing_file")
        self.notifier.send(Notification(
            title="Entity file has not arrived",
            text=decision.reason + " Airflow's retry window is exhausted; please contact the vendor.",
            band=decision.band, run_id=ctx.dag_run_id,
        ))

    # ------------------------------------------------------------------ triage
    def triage(self, ctx: RunContext, report: CheckReport) -> PolicyDecision:
        run, result = triage(report, self.model_for("triage"))
        self._record_agent(ctx, run, band=result.band if result else None,
                           confidence=result.confidence if result else None)
        decision = self.policy.decide(report, result)
        self.audit.record("policy_decision", "policy", **self._ids(ctx), band=decision.band,
                          output=decision.model_dump(mode="json"), confidence=decision.llm_confidence,
                          final_action="override" if decision.overridden else "accept")
        return decision

    # ------------------------------------------------------------------ SUPERVISED: dry-run diff
    def propose_fix(self, ctx: RunContext, report: CheckReport, decision: PolicyDecision) -> FixProposal:
        """Explain + remediate + dry run. If the diff fails validation or policy review, the
        result has band HUMAN_LED and no changes; the caller then escalates."""
        explanation = self._explain(ctx, report, decision)
        changes: list[ProposedChange] = []

        if not report.passed:
            run, remediation = remediate(report, self.policy.policy, self.model_for("remediate"))
            self._record_agent(ctx, run, band=decision.band)
            if remediation is None:
                return self._needs_human(report, explanation, decision.model_copy(update={
                    "band": Band.HUMAN_LED,
                    "reason": "The remediate agent did not return a valid diff.",
                    "rules_fired": decision.rules_fired + ["remediation output invalid -> HUMAN_LED"],
                }))
            decision = self.policy.review_remediation(decision, remediation)
            changes = remediation.changes
            if decision.band == Band.SUPERVISED:
                problem = self._diff_problem(report.batch_id, changes)
                if problem:
                    decision = decision.model_copy(update={
                        "band": Band.HUMAN_LED, "reason": problem,
                        "rules_fired": decision.rules_fired + [problem]})
            if decision.band != Band.SUPERVISED:
                return self._needs_human(report, explanation, decision)

        rows = fetch_batch(self.engine, report.batch_id)
        preview = self.writer.preview(report.batch_id, changes)
        proposal = FixProposal(
            fix_id=str(uuid.uuid4()),
            batch_id=report.batch_id,
            band=decision.band,
            changes=[c.model_dump(mode="json") for c in changes],
            subject_sha256=subject_hash(rows, changes),
            headline=explanation.headline,
            summary=explanation.summary,
            decision=decision.model_dump(mode="json"),
            preview=preview,
        )
        proposal["approval_body"] = self._approval_body(proposal)
        self._store_fix(ctx, proposal)
        self.audit.record("proposed_fix", "remediate" if changes else "policy", **self._ids(ctx),
                          band=decision.band, output={"fix_id": proposal["fix_id"],
                                                      "subject_sha256": proposal["subject_sha256"],
                                                      "changes": len(changes), "preview": preview},
                          final_action="dry_run_diff_awaiting_approval")
        self.notifier.send(Notification(
            title=explanation.headline, text=explanation.summary, band=decision.band,
            run_id=ctx.dag_run_id, link=ctx.approval_url(),
            facts={"Proposed changes": str(len(changes)), "Publish preview": _preview_text(preview)},
        ))
        return proposal

    def proposal_for_clean_load(self, ctx: RunContext, report: CheckReport) -> FixProposal:
        """Used when policy requires a human sign-off even for a clean batch."""
        decision = PolicyDecision(band=Band.SUPERVISED, reason="Policy requires approval for every publish.",
                                  rules_fired=["publish.require_hitl_for_clean_loads"],
                                  policy_floor=Band.SUPERVISED)
        return self.propose_fix(ctx, report, decision)

    # ------------------------------------------------------------------ HUMAN_LED
    @staticmethod
    def _needs_human(report: CheckReport, explanation: Explanation, decision: PolicyDecision) -> FixProposal:
        return FixProposal(band=Band.HUMAN_LED, batch_id=report.batch_id,
                           explanation=explanation.model_dump(), decision=decision.model_dump(mode="json"))

    def escalate(
        self, ctx: RunContext, report: CheckReport, decision: PolicyDecision,
        explanation: Explanation | None = None,
    ) -> FixProposal:
        """Gather context, summarise, notify. Never proposes or performs a write."""
        explanation = explanation or self._explain(ctx, report, decision)
        self.audit.record("escalation", "policy", **self._ids(ctx), band=Band.HUMAN_LED,
                          output={"summary": explanation.summary, "reason": decision.reason,
                                  "rules_fired": decision.rules_fired},
                          final_action="escalated_no_write")
        self.notifier.send(Notification(
            title=explanation.headline, text=explanation.summary, band=Band.HUMAN_LED,
            run_id=ctx.dag_run_id,
            facts={"Why a person is needed": decision.reason, "Batch": report.batch_id},
        ))
        return self._needs_human(report, explanation, decision)

    # ------------------------------------------------------------------ human decision + publish
    def record_human_decision(
        self, ctx: RunContext, proposal: dict, chosen: str, approver: str, comment: str | None = None,
    ) -> str:
        decision = {APPROVE: "approved", REJECT: "rejected"}.get(chosen, "timed_out")
        approval_id = record_approval(
            self.engine, kind="hitl", subject_sha256=proposal["subject_sha256"], decision=decision,
            approver=approver, dag_id=ctx.dag_id, dag_run_id=ctx.dag_run_id, comment=comment,
        )
        self.audit.record("human_decision", approver, **self._ids(ctx), band=proposal["band"],
                          output={"approval_id": approval_id, "fix_id": proposal["fix_id"],
                                  "subject_sha256": proposal["subject_sha256"], "comment": comment},
                          human_decision=chosen, approver=approver,
                          final_action="publish_approved_fix" if decision == "approved" else "no_write")
        return approval_id

    def authorize_clean_load(self, ctx: RunContext, report: CheckReport) -> str:
        if not report.passed:
            raise ValueError("only a batch that passed every check can be authorised by policy")
        if self.policy.policy.publish.require_hitl_for_clean_loads:
            raise PermissionError("policy requires human approval for every publish")
        rows = fetch_batch(self.engine, report.batch_id)
        return record_approval(
            self.engine, kind="clean_load_policy", subject_sha256=subject_hash(rows, []),
            decision="approved", approver="policy:all_checks_passed",
            dag_id=ctx.dag_id, dag_run_id=ctx.dag_run_id,
            comment=f"{len(report.results)} checks passed",
        )

    def publish(self, ctx: RunContext, batch_id: str, changes: list[dict], approval_id: str) -> PublishResult:
        parsed = [ProposedChange.model_validate(c) for c in changes]
        result = self.writer.publish_batch(batch_id, parsed, approval_id)
        self.audit.record("publish", "system", **self._ids(ctx),
                          output={"approval_id": approval_id, "inserted": result.inserted,
                                  "updated": result.updated, "unchanged": result.unchanged,
                                  "changes_applied": len(parsed)},
                          final_action="published")
        if parsed:
            self.notifier.send(Notification(
                title="Approved fix published",
                text=f"{len(parsed)} corrections applied; {result.updated} records updated and "
                     f"{result.inserted} inserted in published.entities.",
                run_id=ctx.dag_run_id,
            ))
        return result

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def _ids(ctx: RunContext) -> dict:
        return {"dag_id": ctx.dag_id, "dag_run_id": ctx.dag_run_id, "task_id": ctx.task_id}

    def _record_agent(self, ctx: RunContext, run: AgentRun, band=None, confidence=None) -> None:
        """One audit row per agent call, valid or not. Inputs are stored as hash + excerpt."""
        self.audit.record(
            "agent_decision", run.agent, **self._ids(ctx), band=band,
            input_hash=(run.input_sha256, run.input_excerpt),
            model_provider=run.model_provider, model_name=run.model_name, model_version=run.model_version,
            output=run.output if run.ok else {"error": run.error}, confidence=confidence,
            final_action="valid_output" if run.ok else "invalid_output_routed_to_human",
        )

    def _explain(self, ctx: RunContext, report: CheckReport, decision: PolicyDecision) -> Explanation:
        if report.passed:
            return Explanation(
                headline="Clean batch awaiting sign-off",
                summary=f"All {len(report.results)} data quality checks passed on {report.total_rows} records. "
                        "Nothing needs correcting. Policy requires a person to approve every publish. "
                        "Approve to publish this batch as-is.",
            )
        run, explanation = explain(report, decision, self.model_for("explain"))
        self._record_agent(ctx, run, band=decision.band)
        return explanation or fallback_explanation(report, decision)

    def _diff_problem(self, batch_id: str, changes: list[ProposedChange]) -> str | None:
        """Dry-run the diff against the real batch; it must apply cleanly and pass every check."""
        rows = fetch_batch(self.engine, batch_id)
        try:
            fixed = apply_diff(rows, changes)
        except DiffConflict as exc:
            return f"Proposed diff does not match the data: {exc}"
        after = run_checks(batch_id, fixed, published_snapshot(self.engine))
        if not after.passed:
            names = ", ".join(r.name for r in after.failed)
            return f"Proposed diff would still leave failing checks ({names})."
        return None

    def _store_fix(self, ctx: RunContext, proposal: FixProposal) -> None:
        is_pg = self.engine.dialect.name == "postgresql"
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO dagfox.proposed_fixes (fix_id, dag_id, dag_run_id, batch_id, diff, "
                    "diff_sha256, model_name) VALUES (:fix_id, :dag_id, :dag_run_id, :batch_id, "
                    f"{'CAST(:diff AS JSONB)' if is_pg else ':diff'}, :sha, :model)"
                ),
                dict(fix_id=proposal["fix_id"], dag_id=ctx.dag_id, dag_run_id=ctx.dag_run_id,
                     batch_id=proposal["batch_id"], diff=canonical_json(proposal["changes"]),
                     sha=proposal["subject_sha256"], model=self.model_for("remediate").name),
            )

    def _count_actions(self, ctx: RunContext, final_action: str) -> int:
        with self.engine.connect() as conn:
            return conn.execute(
                text("SELECT count(*) FROM dagfox.audit_log WHERE dag_run_id = :r AND final_action = :a"),
                {"r": ctx.dag_run_id, "a": final_action},
            ).scalar()

    @staticmethod
    def _approval_body(p: FixProposal) -> str:
        d = p["decision"]
        llm = f"{d.get('llm_band')} ({d.get('llm_confidence')})" if d.get("llm_band") else "n/a"
        lines = [
            f"### {p['headline']}",
            "",
            p["summary"],
            "",
            f"**Band:** {p['band']} &nbsp; **LLM recommended:** {llm} &nbsp; "
            f"**Policy override:** {'yes' if d.get('overridden') else 'no'}",
            "",
            f"**If approved:** {_preview_text(p['preview'])}",
            "",
        ]
        if p["changes"]:
            lines += ["| LEI | Action | Field | Old | New | Source |", "|---|---|---|---|---|---|"]
            for c in p["changes"][:MAX_DIFF_ROWS_SHOWN]:
                old, new = (("—", "—") if c["action"] == "drop_duplicate"
                            else (f"`{c['old_value']!r}`", f"`{c['new_value']!r}`"))  # repr shows stray spaces
                lines.append(
                    f"| `{c['record_id']}` | {c['action']} | {c['field'] or ''} | {old} | {new} | {c['source']} |"
                )
            if len(p["changes"]) > MAX_DIFF_ROWS_SHOWN:
                lines.append(f"\n…and {len(p['changes']) - MAX_DIFF_ROWS_SHOWN} more (full diff in "
                             f"`dagfox.proposed_fixes`, fix_id `{p['fix_id']}`).")
        lines += [
            "",
            f"Fix `{p['fix_id']}` · your approval covers SHA-256 `{p['subject_sha256'][:16]}…` "
            "of exactly this batch and diff.",
            "",
            "**Approve** publishes the corrected batch. **Reject** leaves published data untouched.",
        ]
        return "\n".join(lines)


def _preview_text(preview: dict) -> str:
    return (f"{preview['updated']} published records updated, {preview['inserted']} inserted, "
            f"{preview['unchanged']} unchanged")
