"""Test fixtures: in-memory SQLite with attached schemas, scripted LLMs, no network.

SQLite's ATTACH gives us real `published.entities` / `dagfox.audit_log` names,
so the production SQL runs unchanged. The Postgres-only guard trigger is
covered by tests/test_postgres_guard.py when DAGFOX_TEST_PG_URL is set.
"""

from __future__ import annotations

import os

import pytest
from helpers import RecordingNotifier, insert_batch, make_entities
from sqlalchemy import create_engine, event
from sqlalchemy.pool import StaticPool

from dagfox.config import DEFAULT_POLICY_PATH
from dagfox.db import ENTITY_FIELDS
from dagfox.llm.adapter import ModelHandle
from dagfox.pipeline import EntityPipeline, RunContext
from dagfox.policy.engine import PolicyEngine
from dagfox.publish.diff import subject_hash
from dagfox.publish.writer import PublishedWriter, record_approval

os.environ.setdefault("PYDANTIC_AI_NO_BANNER", "1")

ENTITY_COLS = ", ".join(f"{f} TEXT" for f in ENTITY_FIELDS)
SQLITE_DDL = f"""
CREATE TABLE dagfox.audit_log (
    seq INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE, event_time TEXT NOT NULL,
    dag_id TEXT, dag_run_id TEXT, task_id TEXT, event_type TEXT NOT NULL, actor TEXT NOT NULL, band TEXT,
    input_sha256 TEXT, input_excerpt TEXT, model_provider TEXT, model_name TEXT, model_version TEXT,
    output TEXT, confidence NUMERIC, human_decision TEXT, approver TEXT, final_action TEXT,
    prev_hash TEXT, row_hash TEXT NOT NULL);
CREATE TRIGGER dagfox.audit_log_no_update BEFORE UPDATE ON audit_log
    BEGIN SELECT RAISE(ABORT, 'append-only table'); END;
CREATE TRIGGER dagfox.audit_log_no_delete BEFORE DELETE ON audit_log
    BEGIN SELECT RAISE(ABORT, 'append-only table'); END;
CREATE TABLE dagfox.approvals (
    approval_id TEXT PRIMARY KEY, created_at TEXT DEFAULT CURRENT_TIMESTAMP, dag_id TEXT, dag_run_id TEXT,
    kind TEXT NOT NULL, subject_sha256 TEXT NOT NULL, decision TEXT NOT NULL, approver TEXT NOT NULL,
    comment TEXT);
CREATE TABLE dagfox.proposed_fixes (
    fix_id TEXT PRIMARY KEY, created_at TEXT DEFAULT CURRENT_TIMESTAMP, dag_id TEXT, dag_run_id TEXT,
    batch_id TEXT, diff TEXT, diff_sha256 TEXT, model_name TEXT);
CREATE TABLE staging.entities ({ENTITY_COLS}, batch_id TEXT NOT NULL,
    loaded_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE published.entities ({ENTITY_COLS}, published_at TEXT DEFAULT CURRENT_TIMESTAMP,
    approval_id TEXT NOT NULL, PRIMARY KEY (lei));
"""


@pytest.fixture
def engine():
    eng = create_engine("sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False})

    @event.listens_for(eng, "connect")
    def attach(dbapi_conn, _):
        for schema in ("dagfox", "staging", "published"):
            dbapi_conn.execute(f"ATTACH DATABASE ':memory:' AS {schema}")

    raw = eng.raw_connection()
    raw.executescript(SQLITE_DDL)
    raw.close()
    return eng


@pytest.fixture
def published(engine) -> list[dict]:
    """100 entities published through the guarded writer (as the seed loader does)."""
    rows = make_entities(100)
    insert_batch(engine, "bootstrap", rows)
    approval = record_approval(engine, kind="bootstrap", subject_sha256=subject_hash(rows, []),
                               decision="approved", approver="system:seed")
    PublishedWriter(engine).publish_batch("bootstrap", [], approval)
    return rows


@pytest.fixture
def policy() -> PolicyEngine:
    return PolicyEngine.from_file(DEFAULT_POLICY_PATH)


@pytest.fixture
def ctx() -> RunContext:
    return RunContext(dag_id="entities_daily", dag_run_id="manual__test__1", task_id="test")


@pytest.fixture
def make_pipeline(engine, policy):
    def factory(models: dict[str, ModelHandle]) -> tuple[EntityPipeline, RecordingNotifier]:
        notifier = RecordingNotifier()
        return EntityPipeline(engine, policy, lambda agent: models[agent], notifier), notifier
    return factory
