"""Data quality checks for a staged batch of entity records.

Checks are plain functions over lists of dicts: deterministic, fast for a few
thousand rows, and trivially unit-testable. Each returns a CheckResult with
row-level evidence that the agents (and humans) can read.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Sequence

from pydantic import BaseModel, Field

from dagfox.checks.reference import ELF_CODE_PATTERN, country_is_valid, lei_is_valid

# Fields whose change must be backed by a newer last_update_date from the source.
TRACKED_FIELDS = [
    "legal_name",
    "legal_form_code",
    "legal_address_country",
    "hq_address_country",
    "entity_status",
]
MAX_EVIDENCE = 200


class Evidence(BaseModel):
    lei: str | None
    field: str | None = None
    staging_value: str | None = None
    published_value: str | None = None
    note: str


class CheckResult(BaseModel):
    name: str
    description: str
    passed: bool
    affected_leis: list[str] = Field(default_factory=list)
    evidence: list[Evidence] = Field(default_factory=list)

    @property
    def affected_count(self) -> int:
        return len(self.affected_leis)


class CheckReport(BaseModel):
    batch_id: str
    total_rows: int
    published_rows: int
    results: list[CheckResult]

    @property
    def failed(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed]

    @property
    def passed(self) -> bool:
        return not self.failed

    @property
    def affected_leis(self) -> set[str]:
        return {lei for r in self.failed for lei in r.affected_leis}

    @property
    def affected_pct(self) -> float:
        return 100.0 * len(self.affected_leis) / max(self.total_rows, 1)

    def summary_lines(self) -> list[str]:
        return [
            f"{r.name}: {'PASS' if r.passed else f'FAIL ({r.affected_count} records)'}"
            for r in self.results
        ]


Published = dict[str, dict]


def _result(name: str, description: str, evidence: list[Evidence]) -> CheckResult:
    leis = sorted({e.lei for e in evidence if e.lei})
    return CheckResult(
        name=name,
        description=description,
        passed=not evidence,
        affected_leis=leis,
        evidence=evidence[:MAX_EVIDENCE],
    )


def check_required_fields(rows: Sequence[dict], published: Published) -> CheckResult:
    ev = [
        Evidence(lei=r.get("lei"), field=f, note=f"{f} is empty")
        for r in rows
        for f in ("lei", "legal_name", "legal_address_country")
        if not r.get(f)
    ]
    return _result("required_fields", "LEI, legal name and legal country are present", ev)


def check_lei_format(rows: Sequence[dict], published: Published) -> CheckResult:
    ev = [
        Evidence(lei=r["lei"], field="lei", staging_value=r["lei"], note="fails ISO 17442 format/checksum")
        for r in rows
        if r.get("lei") and not lei_is_valid(r["lei"])
    ]
    return _result("lei_format", "LEIs are valid ISO 17442 codes", ev)


def check_duplicate_lei(rows: Sequence[dict], published: Published) -> CheckResult:
    counts = Counter(r.get("lei") for r in rows if r.get("lei"))
    ev = [
        Evidence(lei=lei, note=f"appears {n} times in the batch")
        for lei, n in sorted(counts.items())
        if n > 1
    ]
    return _result("duplicate_lei", "Each LEI appears once in the batch", ev)


def check_country_format(rows: Sequence[dict], published: Published) -> CheckResult:
    ev = [
        Evidence(lei=r.get("lei"), field=f, staging_value=r.get(f),
                 published_value=published.get(r.get("lei"), {}).get(f),
                 note="not an ISO 3166-1 alpha-2 code")
        for r in rows
        for f in ("legal_address_country", "hq_address_country")
        if r.get(f) is not None and not country_is_valid(r.get(f))
    ]
    return _result("country_format", "Country fields are ISO 3166-1 alpha-2 codes", ev)


def check_legal_form_format(rows: Sequence[dict], published: Published) -> CheckResult:
    ev = [
        Evidence(lei=r.get("lei"), field="legal_form_code", staging_value=r.get("legal_form_code"),
                 note="not a 4-character ISO 20275 ELF code")
        for r in rows
        if r.get("legal_form_code") and not ELF_CODE_PATTERN.match(r["legal_form_code"])
    ]
    return _result("legal_form_format", "Legal form is a 4-character ELF code", ev)


def check_unsourced_changes(rows: Sequence[dict], published: Published) -> CheckResult:
    """A published value changed but the source's last_update_date did not move forward."""
    ev = []
    for r in rows:
        prev = published.get(r.get("lei"))
        if not prev or (r.get("last_update_date") or "") > (prev.get("last_update_date") or ""):
            continue
        for f in TRACKED_FIELDS:
            new, old = r.get(f), prev.get(f)
            # Pure formatting differences are reported by the format checks instead.
            if new != old and (new or "").strip().upper() != (old or "").strip().upper():
                ev.append(Evidence(lei=r["lei"], field=f, staging_value=new, published_value=old,
                                   note="changed without a newer last_update_date from the source"))
    return _result(
        "unsourced_change",
        "Changes to published records are backed by a newer source update date",
        ev,
    )


def check_row_count(rows: Sequence[dict], published: Published) -> CheckResult:
    unique = len({r.get("lei") for r in rows})
    missing = [lei for lei in published if lei not in {r.get("lei") for r in rows}]
    if published and unique < 0.9 * len(published):
        ev = [Evidence(lei=lei, note="published record missing from the batch") for lei in missing]
    else:
        ev = []
    return _result("row_count", "Batch has at least 90% of the published records", ev)


CHECKS: list[Callable[[Sequence[dict], Published], CheckResult]] = [
    check_required_fields,
    check_lei_format,
    check_duplicate_lei,
    check_country_format,
    check_legal_form_format,
    check_unsourced_changes,
    check_row_count,
]


def run_checks(batch_id: str, rows: Sequence[dict], published: Published) -> CheckReport:
    return CheckReport(
        batch_id=batch_id,
        total_rows=len(rows),
        published_rows=len(published),
        results=[check(rows, published) for check in CHECKS],
    )
