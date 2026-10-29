"""Shared agent runner: one pydantic-ai call, validated output, an AgentRun record either way."""

from __future__ import annotations

import json
from typing import TypeVar

from pydantic import BaseModel
from pydantic_ai import Agent

from dagfox.agents.models import AgentRun
from dagfox.audit.log import summarize_input
from dagfox.llm.adapter import ModelHandle
from dagfox.publish.diff import canonical_json

T = TypeVar("T", bound=BaseModel)

CONTEXT_MARKER = "CONTEXT_JSON:"
COMMON_INSTRUCTIONS = """\
You work for the data operations team of a regulated financial institution that
maintains a legal-entity reference database (GLEIF LEI records).
Ground every statement in the CONTEXT_JSON you are given. Never invent records,
values or sources. If the context is insufficient, say so.
"""


def build_prompt(task: str, context: dict) -> str:
    return f"{task}\n\n{CONTEXT_MARKER}\n{canonical_json(context)}"


def parse_context(prompt: str) -> dict:
    """Inverse of build_prompt (used by the offline rules)."""
    return json.loads(prompt.split(CONTEXT_MARKER, 1)[1])


def run_agent(
    name: str,
    output_type: type[T],
    instructions: str,
    task: str,
    context: dict,
    handle: ModelHandle,
) -> tuple[AgentRun, T | None]:
    """Run one agent. Any failure (provider error, schema violation after retries)
    returns ``None`` output; the caller routes that to HUMAN_LED."""
    prompt = build_prompt(task, context)
    input_sha256, input_excerpt = summarize_input(prompt)
    record = dict(
        agent=name,
        model_provider=handle.provider,
        model_name=handle.name,
        input_sha256=input_sha256,
        input_excerpt=input_excerpt,
    )
    try:
        agent = Agent(
            handle.model,
            output_type=output_type,
            instructions=COMMON_INSTRUCTIONS + instructions,
            retries=2,
        )
        result = agent.run_sync(prompt)
        output = output_type.model_validate(result.output.model_dump())  # belt and braces
        version = getattr(result.response, "model_name", None)
        return AgentRun(ok=True, output=output.model_dump(mode="json"), model_version=version, **record), output
    except Exception as exc:  # noqa: BLE001 - every failure mode means "a human looks at this"
        return AgentRun(ok=False, output=None, error=f"{type(exc).__name__}: {exc}"[:500], **record), None
