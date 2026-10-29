-- DagFox audit + approval tables (PostgreSQL).
--
-- All three tables are append-only: UPDATE, DELETE and TRUNCATE raise.
-- audit_log rows are additionally hash-chained (row_hash covers prev_hash),
-- so `python -m dagfox.audit.log verify` detects edits made by a superuser
-- who disables the triggers.

CREATE SCHEMA IF NOT EXISTS dagfox;

CREATE OR REPLACE FUNCTION dagfox.forbid_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'DagFox: % on %.% is not allowed (append-only table)',
        TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME;
END;
$$;

-- One row per agent decision, policy decision, human decision and action.
CREATE TABLE IF NOT EXISTS dagfox.audit_log (
    seq             BIGSERIAL PRIMARY KEY,
    event_id        TEXT        NOT NULL UNIQUE,
    event_time      TIMESTAMPTZ NOT NULL DEFAULT now(),
    dag_id          TEXT,
    dag_run_id      TEXT,
    task_id         TEXT,
    event_type      TEXT        NOT NULL CHECK (event_type IN (
                        'agent_decision', 'policy_decision', 'human_decision',
                        'auto_action', 'proposed_fix', 'publish', 'notification', 'escalation')),
    actor           TEXT        NOT NULL,   -- agent name, 'policy', 'system' or the approver
    band            TEXT CHECK (band IN ('AUTO', 'SUPERVISED', 'HUMAN_LED')),
    input_sha256    TEXT,                   -- hash of the full agent input
    input_excerpt   TEXT,                   -- first 500 chars only
    model_provider  TEXT,
    model_name      TEXT,
    model_version   TEXT,
    output          JSONB,
    confidence      NUMERIC(4, 3),
    human_decision  TEXT,
    approver        TEXT,
    final_action    TEXT,
    prev_hash       TEXT,
    row_hash        TEXT        NOT NULL
);

CREATE INDEX IF NOT EXISTS audit_log_run_idx ON dagfox.audit_log (dag_run_id);

-- Every write to published.* must reference an approved row here.
CREATE TABLE IF NOT EXISTS dagfox.approvals (
    approval_id     TEXT        PRIMARY KEY,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    dag_id          TEXT,
    dag_run_id      TEXT,
    kind            TEXT        NOT NULL CHECK (kind IN ('hitl', 'clean_load_policy', 'bootstrap')),
    subject_sha256  TEXT        NOT NULL,   -- hash of the exact diff / batch that was approved
    decision        TEXT        NOT NULL CHECK (decision IN ('approved', 'rejected', 'timed_out')),
    approver        TEXT        NOT NULL,
    comment         TEXT
);

-- Dry-run diffs proposed by the remediate agent, stored before anyone sees them.
CREATE TABLE IF NOT EXISTS dagfox.proposed_fixes (
    fix_id          TEXT        PRIMARY KEY,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    dag_id          TEXT,
    dag_run_id      TEXT        NOT NULL,
    batch_id        TEXT        NOT NULL,
    diff            JSONB       NOT NULL,
    diff_sha256     TEXT        NOT NULL,
    model_name      TEXT
);

CREATE OR REPLACE TRIGGER audit_log_append_only
    BEFORE UPDATE OR DELETE ON dagfox.audit_log
    FOR EACH ROW EXECUTE FUNCTION dagfox.forbid_mutation();
CREATE OR REPLACE TRIGGER audit_log_no_truncate
    BEFORE TRUNCATE ON dagfox.audit_log
    FOR EACH STATEMENT EXECUTE FUNCTION dagfox.forbid_mutation();
CREATE OR REPLACE TRIGGER approvals_append_only
    BEFORE UPDATE OR DELETE ON dagfox.approvals
    FOR EACH ROW EXECUTE FUNCTION dagfox.forbid_mutation();
CREATE OR REPLACE TRIGGER approvals_no_truncate
    BEFORE TRUNCATE ON dagfox.approvals
    FOR EACH STATEMENT EXECUTE FUNCTION dagfox.forbid_mutation();
CREATE OR REPLACE TRIGGER proposed_fixes_append_only
    BEFORE UPDATE OR DELETE ON dagfox.proposed_fixes
    FOR EACH ROW EXECUTE FUNCTION dagfox.forbid_mutation();

-- "Who approved what": the view the demo queries.
CREATE OR REPLACE VIEW dagfox.v_decisions AS
SELECT a.event_time,
       a.dag_run_id,
       a.event_type,
       a.actor,
       a.band,
       a.model_name,
       a.confidence,
       a.human_decision,
       a.approver,
       a.final_action,
       left(coalesce(a.output ->> 'summary', a.output ->> 'reason', a.output::text), 120) AS detail
FROM dagfox.audit_log a
ORDER BY a.seq;
