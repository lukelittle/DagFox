"""Append-only, hash-chained audit log.

One row per agent decision, policy decision, human decision and action.
Agent inputs are never stored in full: only a SHA-256 and a short excerpt.

    python -m dagfox.audit.log verify    # check the hash chain
"""

from __future__ import annotations

import json
import sys
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import Engine, text

from dagfox.publish.diff import canonical_json, sha256

EXCERPT_CHARS = 500
HASHED_FIELDS = [
    "event_id", "event_time", "dag_id", "dag_run_id", "task_id", "event_type", "actor", "band",
    "input_sha256", "input_excerpt", "model_provider", "model_name", "model_version", "output",
    "confidence", "human_decision", "approver", "final_action",
]


def _time_str(value: Any) -> str:
    if isinstance(value, datetime):
        return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return str(value)


def _row_hash(prev_hash: str | None, row: dict) -> str:
    payload = {f: row.get(f) for f in HASHED_FIELDS}
    payload["event_time"] = _time_str(payload["event_time"])
    if payload["confidence"] is not None:
        payload["confidence"] = f"{Decimal(str(payload['confidence'])):.3f}"
    if isinstance(payload["output"], str):  # SQLite returns JSON as text
        payload["output"] = json.loads(payload["output"])
    return sha256((prev_hash or "") + canonical_json(payload))


def summarize_input(agent_input: Any) -> tuple[str, str]:
    """(sha256, excerpt) of an agent input. The full input is not stored."""
    raw = agent_input if isinstance(agent_input, str) else canonical_json(agent_input)
    return sha256(raw), raw[:EXCERPT_CHARS]


class AuditLog:
    def __init__(self, engine: Engine):
        self.engine = engine

    def record(
        self,
        event_type: str,
        actor: str,
        *,
        dag_id: str | None = None,
        dag_run_id: str | None = None,
        task_id: str | None = None,
        band: str | None = None,
        agent_input: Any = None,
        input_hash: tuple[str, str] | None = None,
        model_provider: str | None = None,
        model_name: str | None = None,
        model_version: str | None = None,
        output: dict | None = None,
        confidence: float | None = None,
        human_decision: str | None = None,
        approver: str | None = None,
        final_action: str | None = None,
    ) -> str:
        if input_hash is None and agent_input is not None:
            input_hash = summarize_input(agent_input)
        input_sha256, input_excerpt = input_hash or (None, None)
        row = dict(
            event_id=str(uuid.uuid4()),
            event_time=_time_str(datetime.now(UTC)),
            dag_id=dag_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            event_type=event_type,
            actor=actor,
            band=str(band) if band else None,
            input_sha256=input_sha256,
            input_excerpt=input_excerpt,
            model_provider=model_provider,
            model_name=model_name,
            model_version=model_version,
            output=output,
            confidence=round(confidence, 3) if confidence is not None else None,
            human_decision=human_decision,
            approver=approver,
            final_action=final_action,
        )
        with self.engine.begin() as conn:
            is_pg = conn.dialect.name == "postgresql"
            if is_pg:  # serialise writers so the hash chain stays linear
                conn.execute(text("SELECT pg_advisory_xact_lock(hashtext('dagfox.audit_log'))"))
            prev = conn.execute(
                text("SELECT row_hash FROM dagfox.audit_log ORDER BY seq DESC LIMIT 1")
            ).scalar()
            row["prev_hash"] = prev
            row["row_hash"] = _row_hash(prev, row)
            cols = list(row)
            values = [
                ("CAST(:output AS JSONB)" if is_pg else ":output") if c == "output" else f":{c}" for c in cols
            ]
            conn.execute(
                text(f"INSERT INTO dagfox.audit_log ({', '.join(cols)}) VALUES ({', '.join(values)})"),
                row | {"output": canonical_json(output) if output is not None else None},
            )
        return row["event_id"]

    def rows(self, dag_run_id: str | None = None) -> list[dict]:
        sql = "SELECT * FROM dagfox.audit_log"
        params = {}
        if dag_run_id:
            sql += " WHERE dag_run_id = :r"
            params["r"] = dag_run_id
        with self.engine.connect() as conn:
            return [dict(r) for r in conn.execute(text(sql + " ORDER BY seq"), params).mappings()]

    def verify(self) -> tuple[bool, int, int | None]:
        """Recompute the chain. Returns (ok, rows checked, first bad seq)."""
        prev = None
        rows = self.rows()
        for row in rows:
            if row["prev_hash"] != prev or _row_hash(prev, row) != row["row_hash"]:
                return False, len(rows), row["seq"]
            prev = row["row_hash"]
        return True, len(rows), None


def main(argv: list[str]) -> int:
    from dagfox.config import resolve_db_url
    from dagfox.db import get_engine

    if argv[:1] != ["verify"]:
        print("usage: python -m dagfox.audit.log verify")
        return 2
    ok, n, bad = AuditLog(get_engine(resolve_db_url())).verify()
    print(f"audit log: {n} rows, chain {'OK' if ok else f'BROKEN at seq {bad}'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
