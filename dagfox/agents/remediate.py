"""Remediate agent: proposes a row-level dry-run diff. It never writes anything."""

from __future__ import annotations

from dagfox.agents.base import run_agent
from dagfox.agents.models import AgentRun, Remediation
from dagfox.checks.entity_checks import CheckReport
from dagfox.llm.adapter import ModelHandle
from dagfox.policy.engine import Policy

INSTRUCTIONS = """\
You are the REMEDIATE agent. Propose the smallest set of row-level changes that
fixes the failed checks. Rules:
- Only change records listed in the evidence. old_value must equal the staging_value exactly.
- Every change needs a rationale and a source. Use these sources:
  * value changed with no source update -> revert to published_value,
    source "published.entities (last approved version)"
  * country code with bad formatting (case, spaces) -> the ISO 3166-1 alpha-2 code,
    source "ISO 3166-1 alpha-2 normalization"
  * LEI duplicated with identical rows -> action "drop_duplicate" (no field/new_value),
    source "duplicate removal (identical rows)"
- Only touch fields listed in correctable_fields.
- If a fix is not clear-cut, leave that record out and say so in the summary.
Your diff is a dry run. A human reviews and approves it before anything is written.
"""

TASK = "Propose a dry-run diff that fixes the failed checks."


def remediate(report: CheckReport, policy: Policy, handle: ModelHandle) -> tuple[AgentRun, Remediation | None]:
    context = {
        "batch_id": report.batch_id,
        "allowed_sources": policy.remediation.allowed_sources,
        "correctable_fields": policy.remediation.correctable_fields,
        "failed_checks": [
            {"name": r.name, "evidence": [e.model_dump() for e in r.evidence]} for r in report.failed
        ],
    }
    return run_agent("remediate", Remediation, INSTRUCTIONS, TASK, context, handle)
