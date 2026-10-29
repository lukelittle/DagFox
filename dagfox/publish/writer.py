"""The only code path that writes to ``published.*``.

The guard is enforced here, in code, and again by a Postgres trigger
(see entities_schema.sql). Prompts have no part in it.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass

from sqlalchemy import Connection, Engine, text

from dagfox.agents.models import ProposedChange
from dagfox.db import ENTITY_FIELDS, fetch_entities
from dagfox.publish.diff import apply_diff, subject_hash


class UnapprovedWriteError(PermissionError):
    """Raised when a write to published.* is attempted without a matching approval."""


@dataclass(frozen=True)
class PublishResult:
    approval_id: str
    inserted: int
    updated: int
    unchanged: int


def record_approval(
    engine: Engine,
    *,
    kind: str,
    subject_sha256: str,
    decision: str,
    approver: str,
    dag_id: str | None = None,
    dag_run_id: str | None = None,
    comment: str | None = None,
) -> str:
    approval_id = str(uuid.uuid4())
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO dagfox.approvals (approval_id, dag_id, dag_run_id, kind, "
                "subject_sha256, decision, approver, comment) VALUES (:approval_id, :dag_id, "
                ":dag_run_id, :kind, :subject_sha256, :decision, :approver, :comment)"
            ),
            dict(
                approval_id=approval_id,
                dag_id=dag_id,
                dag_run_id=dag_run_id,
                kind=kind,
                subject_sha256=subject_sha256,
                decision=decision,
                approver=approver,
                comment=comment,
            ),
        )
    return approval_id


def fetch_batch(engine: Engine, batch_id: str) -> list[dict]:
    cols = ", ".join(ENTITY_FIELDS)
    with engine.connect() as conn:
        rows = conn.execute(
            text(f"SELECT {cols} FROM staging.entities WHERE batch_id = :b"), {"b": batch_id}
        ).mappings()
        return [dict(r) for r in rows]


class PublishedWriter:
    def __init__(self, engine: Engine):
        self.engine = engine

    def publish_batch(
        self,
        batch_id: str,
        changes: Sequence[ProposedChange],
        approval_id: str | None,
    ) -> PublishResult:
        """Publish staging batch ``batch_id`` with ``changes`` applied.

        The writer reads the batch itself and recomputes the subject hash, so
        an approval only ever authorises the exact batch + diff it was given for.
        """
        if not approval_id:
            raise UnapprovedWriteError("write to published.entities requires an approval_id")

        rows = fetch_batch(self.engine, batch_id)
        expected = subject_hash(rows, changes)

        with self.engine.begin() as conn:
            approval = conn.execute(
                text(
                    "SELECT decision, subject_sha256 FROM dagfox.approvals "
                    "WHERE approval_id = :a"
                ),
                {"a": approval_id},
            ).mappings().first()
            if approval is None:
                raise UnapprovedWriteError(f"approval {approval_id} does not exist")
            if approval["decision"] != "approved":
                raise UnapprovedWriteError(f"approval {approval_id} is {approval['decision']}")
            if approval["subject_sha256"] != expected:
                raise UnapprovedWriteError(
                    f"approval {approval_id} covers a different batch or diff than the one being published"
                )

            final_rows = apply_diff(rows, changes)
            leis = [r["lei"] for r in final_rows]
            if len(leis) != len(set(leis)):
                raise ValueError("batch still contains duplicate LEIs after applying the diff")

            self._bind_approval(conn, approval_id)
            return self._upsert(conn, final_rows, approval_id)

    def preview(self, batch_id: str, changes: Sequence[ProposedChange]) -> dict:
        """Dry run: what publish_batch would do, without writing anything."""
        final_rows = apply_diff(fetch_batch(self.engine, batch_id), changes)
        current = published_snapshot(self.engine)
        inserted = sum(1 for r in final_rows if r["lei"] not in current)
        updated = sum(
            1 for r in final_rows
            if r["lei"] in current and any(current[r["lei"]][f] != r.get(f) for f in ENTITY_FIELDS)
        )
        return {"inserted": inserted, "updated": updated, "unchanged": len(final_rows) - inserted - updated}

    @staticmethod
    def _bind_approval(conn: Connection, approval_id: str) -> None:
        if conn.dialect.name == "postgresql":
            # Transaction-scoped; read by the published.require_approval() trigger.
            conn.execute(text("SELECT set_config('dagfox.approval_id', :a, true)"), {"a": approval_id})

    def _upsert(self, conn: Connection, rows: list[dict], approval_id: str) -> PublishResult:
        cols = ", ".join(ENTITY_FIELDS)
        current = {
            r["lei"]: dict(r)
            for r in conn.execute(text(f"SELECT {cols} FROM published.entities")).mappings()
        }
        inserts, updates = [], []
        for row in rows:
            params = {f: row.get(f) for f in ENTITY_FIELDS} | {"approval_id": approval_id}
            existing = current.get(row["lei"])
            if existing is None:
                inserts.append(params)
            elif any(existing[f] != row.get(f) for f in ENTITY_FIELDS):
                updates.append(params)

        if inserts:
            placeholders = ", ".join(f":{f}" for f in ENTITY_FIELDS)
            conn.execute(
                text(
                    f"INSERT INTO published.entities ({cols}, approval_id) "
                    f"VALUES ({placeholders}, :approval_id)"
                ),
                inserts,
            )
        if updates:
            assignments = ", ".join(f"{f} = :{f}" for f in ENTITY_FIELDS if f != "lei")
            conn.execute(
                text(
                    f"UPDATE published.entities SET {assignments}, approval_id = :approval_id, "
                    f"published_at = CURRENT_TIMESTAMP WHERE lei = :lei"
                ),
                updates,
            )
        return PublishResult(
            approval_id=approval_id,
            inserted=len(inserts),
            updated=len(updates),
            unchanged=len(rows) - len(inserts) - len(updates),
        )


def published_snapshot(engine: Engine) -> dict[str, dict]:
    return {r["lei"]: r for r in fetch_entities(engine, "published.entities")}
