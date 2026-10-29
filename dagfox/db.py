"""Thin SQLAlchemy Core helpers shared by the loader, checks, audit log and publisher."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from sqlalchemy import Engine, create_engine, text

from dagfox.config import PACKAGE_DIR

ENTITY_FIELDS = [
    "lei",
    "legal_name",
    "legal_form_code",
    "legal_address_city",
    "legal_address_country",
    "hq_address_country",
    "entity_status",
    "registration_status",
    "initial_registration_date",
    "last_update_date",
    "managing_lou",
]

# Schema files in the order they must be applied (approvals must exist before
# the published-table guard trigger that references it).
SCHEMA_FILES = [
    PACKAGE_DIR / "audit" / "schema.sql",
    PACKAGE_DIR / "entities_schema.sql",
]


@lru_cache(maxsize=4)
def get_engine(url: str) -> Engine:
    return create_engine(url, pool_pre_ping=True, future=True)


def apply_schema(engine: Engine, files: list[Path] = SCHEMA_FILES) -> None:
    """Apply the Postgres DDL. Every statement is idempotent."""
    # Raw DBAPI cursor with no parameters, so plpgsql's '%' placeholders pass through untouched.
    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            for path in files:
                cur.execute(path.read_text())
        raw.commit()
    finally:
        raw.close()


def fetch_entities(engine: Engine, table: str) -> list[dict]:
    if table not in {"staging.entities", "published.entities"}:
        raise ValueError(f"unknown entity table {table!r}")
    cols = ", ".join(ENTITY_FIELDS)
    with engine.connect() as conn:
        rows = conn.execute(text(f"SELECT {cols} FROM {table} ORDER BY lei")).mappings().all()
    return [dict(r) for r in rows]
