"""Pipeline steps for entities_daily, kept free of Airflow so they are unit-testable.

The DAG file wires these together; nothing here knows about task instances.
"""

from __future__ import annotations

from pathlib import Path

from sqlalchemy import Engine, text

from dagfox.checks.entity_checks import CheckReport, run_checks
from dagfox.db import ENTITY_FIELDS
from dagfox.publish.writer import fetch_batch, published_snapshot
from dagfox.sources.landing import LandingZone


def load_staging(engine: Engine, batch_id: str, path: Path) -> int:
    """(Re)load one batch into staging.entities. Idempotent per batch_id."""
    rows = LandingZone.read(path)
    cols = ", ".join(ENTITY_FIELDS)
    placeholders = ", ".join(f":{f}" for f in ENTITY_FIELDS)
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM staging.entities WHERE batch_id = :b"), {"b": batch_id})
        if rows:
            conn.execute(
                text(f"INSERT INTO staging.entities ({cols}, batch_id) VALUES ({placeholders}, :batch_id)"),
                [{f: r.get(f) for f in ENTITY_FIELDS} | {"batch_id": batch_id} for r in rows],
            )
    return len(rows)


def check_batch(engine: Engine, batch_id: str) -> CheckReport:
    return run_checks(batch_id, fetch_batch(engine, batch_id), published_snapshot(engine))
