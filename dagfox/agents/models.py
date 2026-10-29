"""Pydantic schemas for everything the agents produce.

LLM output is only ever used after it validates against one of these models.
"""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

LEI_REGEX = r"^[A-Z0-9]{18}[0-9]{2}$"


class Band(StrEnum):
    AUTO = "AUTO"
    SUPERVISED = "SUPERVISED"
    HUMAN_LED = "HUMAN_LED"

    @property
    def rank(self) -> int:
        return list(Band).index(self)

    @staticmethod
    def strictest(*bands: Band) -> Band:
        return max(bands, key=lambda b: b.rank)


class TriageResult(BaseModel):
    """Triage agent output: a band *recommendation*. The policy engine decides."""

    model_config = ConfigDict(extra="forbid")

    band: Band
    confidence: float = Field(ge=0.0, le=1.0)
    reason: str = Field(min_length=10, max_length=1000)
    primary_issue: str = Field(min_length=3, max_length=120)


class Explanation(BaseModel):
    """Explain agent output: a short plain-English summary for an ops analyst."""

    model_config = ConfigDict(extra="forbid")

    headline: str = Field(min_length=5, max_length=120)
    summary: str = Field(min_length=40, max_length=1500)

    @field_validator("summary")
    @classmethod
    def three_to_five_sentences(cls, v: str) -> str:
        sentences = [s for s in re.split(r"(?<=[.!?])\s+", v.strip()) if s]
        if not 3 <= len(sentences) <= 5:
            raise ValueError(f"summary must be 3-5 sentences, got {len(sentences)}")
        return v


class ProposedChange(BaseModel):
    """One row-level change in a dry-run diff."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    record_id: str = Field(pattern=LEI_REGEX, description="LEI of the record to change")
    action: Literal["update_field", "drop_duplicate"] = "update_field"
    field: str | None = None
    old_value: str | None = None
    new_value: str | None = None
    rationale: str = Field(min_length=5, max_length=500)
    source: str = Field(
        min_length=3,
        max_length=200,
        description="Where the corrected value comes from, e.g. 'published.entities (last approved version)'",
    )

    @model_validator(mode="after")
    def check_shape(self) -> ProposedChange:
        if self.action == "update_field":
            if not self.field:
                raise ValueError("update_field requires a field")
            if self.old_value == self.new_value:
                raise ValueError("new_value must differ from old_value")
        elif self.field or self.new_value is not None:
            raise ValueError("drop_duplicate must not set field/new_value")
        return self


class Remediation(BaseModel):
    """Remediate agent output: a dry-run diff. Never applied without approval."""

    model_config = ConfigDict(extra="forbid")

    changes: list[ProposedChange] = Field(max_length=1000)
    summary: str = Field(min_length=10, max_length=1000)


class AgentRun(BaseModel):
    """Envelope recorded in the audit log for each agent call."""

    agent: Literal["triage", "explain", "remediate"]
    ok: bool
    output: dict | None
    error: str | None = None
    model_provider: str
    model_name: str
    model_version: str | None = None
    input_sha256: str
    input_excerpt: str
