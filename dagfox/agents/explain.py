"""Explain agent: a 3-5 sentence plain-English summary for a data operations analyst."""

from __future__ import annotations

from dagfox.agents.base import run_agent
from dagfox.agents.models import AgentRun, Explanation
from dagfox.agents.triage import triage_context
from dagfox.checks.entity_checks import CheckReport
from dagfox.llm.adapter import ModelHandle
from dagfox.policy.engine import PolicyDecision

INSTRUCTIONS = """\
You are the EXPLAIN agent. Your reader is a data operations analyst, not an engineer.
Write a short headline and a summary of 3 to 5 sentences in plain English:
what is wrong, how many records, why it matters, and what happens next given the
band (SUPERVISED: a proposed fix awaits approval in Airflow; HUMAN_LED: a person
must investigate, nothing will be changed automatically). No jargon, no code, no SQL.
"""

TASK = "Explain today's data issues to the operations analyst."


def explain(
    report: CheckReport, decision: PolicyDecision, handle: ModelHandle
) -> tuple[AgentRun, Explanation | None]:
    context = triage_context(report) | {
        "band": decision.band,
        "band_reason": decision.reason,
        "policy_rules_fired": decision.rules_fired,
    }
    return run_agent("explain", Explanation, INSTRUCTIONS, TASK, context, handle)


def fallback_explanation(report: CheckReport, decision: PolicyDecision) -> Explanation:
    """Deterministic text used when the explain agent fails, so the analyst still gets a message."""
    checks = ", ".join(f"{r.name} ({r.affected_count})" for r in report.failed) or "none"
    return Explanation(
        headline=f"{decision.band}: {len(report.affected_leis)} entity records need attention",
        summary=(
            f"Data quality checks failed on batch {report.batch_id}. "
            f"Failed checks: {checks}. "
            f"{len(report.affected_leis)} of {report.total_rows} records ({report.affected_pct:.1f}%) are affected. "
            f"Policy decision: {decision.reason}"
        ),
    )
