"""Triage agent: reads check results + sample rows and recommends a band."""

from __future__ import annotations

from dagfox.agents.base import run_agent
from dagfox.agents.models import AgentRun, TriageResult
from dagfox.checks.entity_checks import CheckReport
from dagfox.llm.adapter import ModelHandle

INSTRUCTIONS = """\
You are the TRIAGE agent. Classify today's data quality issues into one band:
- AUTO: transient, no data is wrong (e.g. a late file). The system can wait and retry.
- SUPERVISED: specific records are wrong and each has a clear, sourced correction
  (e.g. formatting errors, exact duplicates, a value that differs from the last
  approved published value with no source update). A human approves a diff.
- HUMAN_LED: ambiguous or high impact (many records affected, no clear source of
  truth, or identifiers themselves invalid). A person must investigate.
Give a confidence between 0 and 1 and a one-to-two sentence reason.
Your band is a recommendation; a deterministic policy makes the final decision.
"""

TASK = "Triage the failed data quality checks for this batch."
SAMPLE_EVIDENCE = 10


def triage_context(report: CheckReport) -> dict:
    return {
        "batch_id": report.batch_id,
        "total_rows": report.total_rows,
        "published_rows": report.published_rows,
        "affected_records": len(report.affected_leis),
        "affected_pct": round(report.affected_pct, 2),
        "failed_checks": [
            {
                "name": r.name,
                "description": r.description,
                "affected_count": r.affected_count,
                "sample_evidence": [e.model_dump() for e in r.evidence[:SAMPLE_EVIDENCE]],
            }
            for r in report.failed
        ],
    }


def triage(report: CheckReport, handle: ModelHandle) -> tuple[AgentRun, TriageResult | None]:
    return run_agent("triage", TriageResult, INSTRUCTIONS, TASK, triage_context(report), handle)
