"""Pure functions for applying a proposed diff and hashing what gets approved."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Sequence

from dagfox.agents.models import ProposedChange
from dagfox.db import ENTITY_FIELDS

CORRECTABLE_FIELDS = frozenset(ENTITY_FIELDS) - {"lei"}


class DiffConflict(ValueError):
    """A proposed change does not match the data it claims to change."""


def canonical_json(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)


def sha256(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def batch_hash(rows: Iterable[dict]) -> str:
    """Order-independent hash of a batch of entity rows."""
    lines = sorted(canonical_json({f: r.get(f) for f in ENTITY_FIELDS}) for r in rows)
    return sha256("\n".join(lines))


def diff_hash(changes: Sequence[ProposedChange]) -> str:
    return sha256(canonical_json([c.model_dump(mode="json") for c in changes]))


def subject_hash(rows: Iterable[dict], changes: Sequence[ProposedChange]) -> str:
    """What an approver signs: this exact batch with this exact diff."""
    return sha256(batch_hash(rows) + ":" + diff_hash(changes))


def apply_diff(rows: Sequence[dict], changes: Sequence[ProposedChange]) -> list[dict]:
    """Return a corrected copy of ``rows``. Raises DiffConflict on any mismatch.

    Every change is checked against the data: the record must exist and the
    old value must match exactly. A diff that does not line up with reality
    is rejected as a whole, never partially applied.
    """
    out = [dict(r) for r in rows]
    for change in changes:
        matches = [r for r in out if r.get("lei") == change.record_id]
        if not matches:
            raise DiffConflict(f"{change.record_id}: record not in batch")
        if change.action == "drop_duplicate":
            if len(matches) < 2:
                raise DiffConflict(f"{change.record_id}: not duplicated")
            keep = matches[0]
            out = [r for r in out if r.get("lei") != change.record_id or r is keep]
            continue
        if change.field not in CORRECTABLE_FIELDS:
            raise DiffConflict(f"{change.record_id}: field {change.field!r} is not correctable")
        for row in matches:
            if row.get(change.field) != change.old_value:
                raise DiffConflict(
                    f"{change.record_id}.{change.field}: expected {change.old_value!r}, "
                    f"found {row.get(change.field)!r}"
                )
            row[change.field] = change.new_value
    return out
