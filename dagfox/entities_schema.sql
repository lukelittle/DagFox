-- Entity reference tables (PostgreSQL). Columns follow GLEIF Level 1 LEI-CDF.
-- Dates are stored as ISO-8601 UTC text ('YYYY-MM-DDTHH:MM:SSZ'), which sorts correctly.

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS published;

-- Staging is a raw landing copy: no primary key, so duplicates can be detected.
CREATE TABLE IF NOT EXISTS staging.entities (
    lei                       TEXT,
    legal_name                TEXT,
    legal_form_code           TEXT,
    legal_address_city        TEXT,
    legal_address_country     TEXT,
    hq_address_country        TEXT,
    entity_status             TEXT,
    registration_status       TEXT,
    initial_registration_date TEXT,
    last_update_date          TEXT,
    managing_lou              TEXT,
    batch_id                  TEXT NOT NULL,
    loaded_at                 TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS published.entities (
    lei                       TEXT PRIMARY KEY,
    legal_name                TEXT NOT NULL,
    legal_form_code           TEXT,
    legal_address_city        TEXT,
    legal_address_country     TEXT NOT NULL,
    hq_address_country        TEXT,
    entity_status             TEXT,
    registration_status       TEXT,
    initial_registration_date TEXT,
    last_update_date          TEXT,
    managing_lou              TEXT,
    published_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    approval_id               TEXT NOT NULL REFERENCES dagfox.approvals (approval_id)
);

-- Prior versions of every published row, written by trigger.
CREATE TABLE IF NOT EXISTS published.entities_history (
    changed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    operation     TEXT NOT NULL,
    old_row       JSONB NOT NULL,
    approval_id   TEXT NOT NULL
);

-- Database-level guardrail. The Python PublishedWriter is the only code path
-- that writes published.*, but this trigger makes the rule hold for *any*
-- client: the transaction must `SET LOCAL dagfox.approval_id` to an approved
-- row in dagfox.approvals, and inserted/updated rows must carry that id.
CREATE OR REPLACE FUNCTION published.require_approval() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    approval TEXT := nullif(current_setting('dagfox.approval_id', true), '');
BEGIN
    IF approval IS NULL THEN
        RAISE EXCEPTION 'DagFox guard: % on published.entities without an approval', TG_OP;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM dagfox.approvals
                   WHERE approval_id = approval AND decision = 'approved') THEN
        RAISE EXCEPTION 'DagFox guard: approval % is missing or not approved', approval;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    IF NEW.approval_id IS DISTINCT FROM approval THEN
        RAISE EXCEPTION 'DagFox guard: row approval_id % does not match session approval %',
            NEW.approval_id, approval;
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION published.record_history() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO published.entities_history (operation, old_row, approval_id)
    VALUES (TG_OP, to_jsonb(OLD), current_setting('dagfox.approval_id', true));
    RETURN NULL;
END;
$$;

CREATE OR REPLACE TRIGGER entities_require_approval
    BEFORE INSERT OR UPDATE OR DELETE ON published.entities
    FOR EACH ROW EXECUTE FUNCTION published.require_approval();
CREATE OR REPLACE TRIGGER entities_no_truncate
    BEFORE TRUNCATE ON published.entities
    FOR EACH STATEMENT EXECUTE FUNCTION dagfox.forbid_mutation();
CREATE OR REPLACE TRIGGER entities_history
    AFTER UPDATE OR DELETE ON published.entities
    FOR EACH ROW EXECUTE FUNCTION published.record_history();
