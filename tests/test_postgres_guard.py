"""Integration: the Postgres trigger blocks writes to published.* from *any* client.

Skipped unless DAGFOX_TEST_PG_URL points at a seeded DagFox database, e.g.
    DAGFOX_TEST_PG_URL=postgresql+psycopg2://dagfox:dagfox@localhost:5433/dagfox pytest
"""

import os

import pytest
from sqlalchemy import create_engine, text

PG_URL = os.environ.get("DAGFOX_TEST_PG_URL")
pytestmark = pytest.mark.skipif(not PG_URL, reason="DAGFOX_TEST_PG_URL not set")


@pytest.fixture
def pg():
    return create_engine(PG_URL)


def test_raw_sql_update_without_approval_is_blocked(pg):
    with pytest.raises(Exception, match="DagFox guard"), pg.begin() as conn:
        conn.execute(text("UPDATE published.entities SET legal_name = 'x' "
                          "WHERE lei = (SELECT min(lei) FROM published.entities)"))


def test_raw_sql_with_rejected_approval_is_blocked(pg):
    with pytest.raises(Exception, match="not approved"), pg.begin() as conn:
        conn.execute(text("SELECT set_config('dagfox.approval_id', 'no-such-approval', true)"))
        conn.execute(text("DELETE FROM published.entities WHERE lei = (SELECT min(lei) FROM published.entities)"))


def test_approvals_and_audit_log_are_append_only(pg):
    for table in ("dagfox.approvals", "dagfox.audit_log"):
        with pytest.raises(Exception, match="append-only"), pg.begin() as conn:
            conn.execute(text(f"DELETE FROM {table}"))
