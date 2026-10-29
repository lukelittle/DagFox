"""Offline stand-in for an LLM: deterministic rules behind pydantic-ai's FunctionModel.

Lets the demo run with no API key while exercising the real agent path
(prompting, structured output, validation, audit). The triage rule is
deliberately naive - it looks at the *kind* of failure, not its volume - so
the demo shows the policy engine overriding it on a high-impact batch.
"""

from __future__ import annotations

from pydantic_ai.messages import ModelMessage, ModelRequest, ModelResponse, ToolCallPart, UserPromptPart
from pydantic_ai.models.function import AgentInfo

from dagfox.agents.base import parse_context
from dagfox.checks.reference import normalize_country


def _context(messages: list[ModelMessage]) -> dict:
    for message in reversed(messages):
        if isinstance(message, ModelRequest):
            for part in message.parts:
                if isinstance(part, UserPromptPart) and isinstance(part.content, str):
                    return parse_context(part.content)
    raise ValueError("no user prompt found")


def _respond(info: AgentInfo, args: dict) -> ModelResponse:
    return ModelResponse(parts=[ToolCallPart(tool_name=info.output_tools[0].name, args=args)])


def triage_rules(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
    ctx = _context(messages)
    names = {c["name"] for c in ctx["failed_checks"]}
    if names & {"lei_format", "row_count"}:
        band, confidence = "HUMAN_LED", 0.85
        reason = "Identifiers are invalid or records are missing, which has no safe automatic fix."
    elif names:
        band, confidence = "SUPERVISED", 0.80
        reason = (
            f"The failures ({', '.join(sorted(names))}) are record-level and each has a clear "
            "correction, so a reviewed fix is appropriate."
        )
    else:
        band, confidence, reason = "AUTO", 0.95, "No data defects were found in this batch."
    return _respond(info, {"band": band, "confidence": confidence, "reason": reason,
                           "primary_issue": ", ".join(sorted(names)) or "none"})


def explain_rules(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
    ctx = _context(messages)
    parts = [f"{c['affected_count']} {c['name'].replace('_', ' ')}" for c in ctx["failed_checks"]]
    next_step = (
        "A proposed fix is waiting for approval in Airflow, and nothing has been published yet."
        if ctx["band"] == "SUPERVISED"
        else "Nothing will be changed automatically, so a person needs to investigate before this batch is published."
    )
    summary = (
        f"Today's entity file has {ctx['affected_records']} problem records out of {ctx['total_rows']} "
        f"({ctx['affected_pct']} percent). "
        f"The issues found are: {'; '.join(parts)}. "
        f"If published as-is, downstream users would see incorrect reference data for these entities. "
        f"{next_step}"
    )
    return _respond(info, {"headline": f"{ctx['band']}: {ctx['affected_records']} entity records need attention",
                           "summary": summary})


def remediate_rules(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
    ctx = _context(messages)
    changes, seen_dupes = [], set()
    for check in ctx["failed_checks"]:
        for ev in check["evidence"]:
            lei, field = ev["lei"], ev.get("field")
            if check["name"] == "duplicate_lei" and lei not in seen_dupes:
                seen_dupes.add(lei)
                changes.append({"record_id": lei, "action": "drop_duplicate",
                                "rationale": f"LEI {ev['note']}; rows are identical, keep one.",
                                "source": "duplicate removal (identical rows)"})
            elif check["name"] == "country_format" and (fixed := normalize_country(ev["staging_value"])):
                changes.append({"record_id": lei, "field": field, "old_value": ev["staging_value"],
                                "new_value": fixed,
                                "rationale": "Country code has wrong case or stray spaces.",
                                "source": "ISO 3166-1 alpha-2 normalization"})
            elif check["name"] == "unsourced_change" and ev.get("published_value") is not None:
                changes.append({"record_id": lei, "field": field, "old_value": ev["staging_value"],
                                "new_value": ev["published_value"],
                                "rationale": "Value changed but the source's last update date did not; "
                                             "keep the last approved value.",
                                "source": "published.entities (last approved version)"})
    return _respond(info, {"changes": changes,
                           "summary": f"Proposed {len(changes)} record-level corrections from the evidence."})


RULES = {"triage": triage_rules, "explain": explain_rules, "remediate": remediate_rules}
