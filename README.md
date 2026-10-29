# DagFox

**DataOps agents for Apache Airflow.**

Airflow tells you something failed. DagFox tells the right person why and what to do, and nothing touches published data without sign-off. It sits inside your Airflow pipelines and watches the *data*, not the infrastructure. When a run lands bad records (duplicates, malformed codes, values that changed overnight with no source behind them), DagFox's agents classify the problem, explain it in plain English to a data operations analyst, and propose a row-level fix as a dry-run diff. The diff waits for a human to approve it in the Airflow UI. Every agent call, policy decision and human decision lands in an append-only, hash-chained audit log. It is built for regulated reference data, where auditability matters more than cleverness.

> Status: MVP. One pipeline (`entities_daily`), three bands, runs on a laptop with one command.

## Where DagFox fits next to AWS DevOps Agent

| | AWS DevOps Agent | DagFox |
|---|---|---|
| Question it answers | *Did the pipeline run?* | *The pipeline ran. Is the data right, and who fixes it?* |
| Typical signals | Broken DAGs, task exceptions, retries, infra and deployment issues | Duplicate identifiers, malformed codes, unsourced changes to published records, volume anomalies |
| Primary user | Platform / DevOps engineer | Data operations analyst, data steward |
| Output | Root cause for a failed run | Plain-English explanation, a proposed row-level diff, a recorded human decision |

They are complementary. DagFox deliberately ends a data-quality run as a *successful* Airflow run with a recorded decision, so it doesn't look like an infrastructure failure. Pipeline-health tooling stays focused on pipeline health.

## The three bands

| Band | Example | What DagFox does | Writes to `published.*`? |
|---|---|---|---|
| **AUTO** | Vendor file is late | Waits and retries; every retry is logged | Only the normal publish of a clean batch, under a recorded policy approval |
| **SUPERVISED** | Duplicate LEIs, `"us "` instead of `US`, countries changed overnight with no source update (under 5% of records) | Explains, proposes a dry-run diff, **pauses for an Airflow HITL approval** | Only after a named approver approves that exact diff |
| **HUMAN_LED** | More than 5% of records changed with no source, invalid identifiers, missing records | Gathers context and writes a summary. **No proposed write.** | No |

The LLM *recommends* a band. [`policy.yaml`](dagfox/policy/policy.yaml) has the final say: rules can only make a band stricter. If the LLM's output fails validation (or the provider is down), the run goes to HUMAN_LED.

## Architecture

```mermaid
flowchart LR
    subgraph Airflow["Airflow 3.3 · entities_daily"]
        W[wait_for_file<br/>sensor] --> L[load_staging] --> C[check_and_triage]
        C --> R{route}
        R -->|all checks pass| PC[publish_clean]
        R -->|SUPERVISED| PF[propose_fix] --> A[[approve_fix<br/>HITL ApprovalOperator]] --> PX[publish_fix]
        R -->|HUMAN_LED| E[escalate]
        PF -->|diff fails review| E2[escalate_after_review]
    end

    subgraph Agents["dagfox.agents (pydantic-ai)"]
        T[triage] ~~~ X[explain] ~~~ M[remediate]
    end
    LLM[(llm/adapter.py<br/>Bedrock · Azure OpenAI · Anthropic · offline)]
    POL[/policy.yaml + engine/]

    C -. checks + sample rows .-> T
    T -. band recommendation .-> POL
    PF -. evidence .-> X & M
    Agents --- LLM

    subgraph PG["Postgres · dagfox db"]
        S[(staging.entities)]
        P[(published.entities<br/>guard trigger)]
        AU[(dagfox.audit_log<br/>append-only, hash-chained)]
        AP[(dagfox.approvals)]
    end

    L --> S
    PC & PX -->|PublishedWriter<br/>verifies approval + hash| P
    A -->|decision + approver| AP
    C & PF & A & E & PX --> AU
    PF & E --> N[Slack / Teams webhook]
    A -. analyst approves in Airflow UI .- U((data_steward))
```

## Quickstart

Requirements: Docker with Compose v2 and about 4 GB of RAM for Docker.

```bash
make up        # builds the image, starts Airflow + Postgres, loads the GLEIF sample
```

Open <http://localhost:8080>. There are two users, both with password `dagfox` (set in `.env`):

- `ops_analyst` triggers and watches runs.
- `data_steward` is the **only** user who can answer DagFox approval requests (`approvals.approvers` in `policy.yaml`).

No API key is needed. The default `DAGFOX_LLM_PROVIDER=offline` is a deterministic, rule-based stand-in for the LLM, so the demo works on a plane. See [Configure a real LLM](#configure-a-real-llm).

## Trigger each fault

```bash
make run FAULT=none          # clean file               -> published, no human needed
make run FAULT=late_file     # file arrives ~60s late   -> AUTO: waits, retries, logs, publishes
make run FAULT=bad_records   # ~2% of records broken    -> SUPERVISED: diff awaits approval
make run FAULT=mass_change   # ~8% changed, no source   -> HUMAN_LED: summary only, nothing written
```

You can also trigger from the UI: **Trigger** → set `fault`.

To approve a SUPERVISED fix, log in as `data_steward` and open the run's `approve_fix` task. On the **Required Action** tab you'll see the plain-English summary, the band and whether policy overrode the LLM, a dry-run preview (how many published rows change), and the full row-level diff. Choose **Approve** or **Reject**, with an optional comment.

## Query the audit log

```bash
make audit           # who decided what, in order
make audit-verify    # recompute the audit log's hash chain
```

```sql
-- psql -h localhost -p 5433 -U dagfox dagfox
SELECT event_time, dag_run_id, event_type, actor, band, model_name, confidence,
       human_decision, approver, final_action
FROM dagfox.audit_log ORDER BY seq;

SELECT created_at, kind, decision, approver, comment, subject_sha256
FROM dagfox.approvals ORDER BY created_at;          -- every authorisation to write published.*

SELECT * FROM published.entities_history ORDER BY changed_at DESC LIMIT 20;  -- prior versions
```

Each `audit_log` row records: timestamp, DAG run, band, the agent input (as a SHA-256 hash plus a 500-character excerpt), model provider, name and served version, the validated output, confidence, the human decision, the approver and the final action.

## Guardrails, and where they are enforced

All of these are enforced in code. None of them rely on prompts.

1. **Every agent output is validated.** Agents return Pydantic models ([`agents/models.py`](dagfox/agents/models.py)). A provider error or a schema violation, even after pydantic-ai's retries, routes the run to HUMAN_LED.
2. **The policy decides the band.** [`policy/engine.py`](dagfox/policy/engine.py) computes a floor from the check results and thresholds. The LLM can raise the band but never lower it.
3. **Every fix is a dry run first.** A proposed diff must apply cleanly to the real staged batch (record exists, `old_value` matches exactly), must leave every check passing, and must cite an allowed source and touch only correctable fields. Otherwise the run escalates. The diff is stored in `dagfox.proposed_fixes` before anyone sees it.
4. **Approvals are bound to content.** An approval records the approver and the SHA-256 of *the exact staged batch plus diff* they were shown. [`PublishedWriter`](dagfox/publish/writer.py), the only code path that writes `published.*`, recomputes that hash and refuses on any mismatch, rejection or missing approval.
5. **The database enforces it too.** A Postgres trigger on `published.entities` rejects any insert, update or delete, from any client, unless the transaction is bound to an approved row in `dagfox.approvals`. `audit_log`, `approvals` and `proposed_fixes` reject UPDATE, DELETE and TRUNCATE.
6. **Approvers are separated.** The HITL task is assigned to the policy's approvers (`assigned_users`); Airflow refuses responses from anyone else.
7. **No secrets in code.** The database comes from the Airflow Connection `dagfox_db`; webhooks come from env vars or Airflow Variables; LLM credentials come from each SDK's standard environment variables.

A batch that passes every check is published under a recorded `clean_load_policy` approval (approver `policy:all_checks_passed`). Set `publish.require_hitl_for_clean_loads: true` to require a human sign-off for every publish.

## Configure a real LLM

All provider-specific code lives in [`dagfox/llm/adapter.py`](dagfox/llm/adapter.py). To switch providers, set these in `.env` and run `make up`:

| Provider | `.env` |
|---|---|
| Anthropic API | `DAGFOX_LLM_PROVIDER=anthropic`, `ANTHROPIC_API_KEY=…`, optional `DAGFOX_LLM_MODEL` (default `claude-opus-5-5`) |
| Amazon Bedrock | `DAGFOX_LLM_PROVIDER=bedrock`, `AWS_REGION=…` plus AWS credentials, `DAGFOX_LLM_MODEL=<model or inference-profile id>` |
| Azure OpenAI | `DAGFOX_LLM_PROVIDER=azure_openai`, `AZURE_OPENAI_ENDPOINT`, `AZURE_OPENAI_API_KEY`, `OPENAI_API_VERSION`, `DAGFOX_LLM_MODEL=<deployment>` |

## Notifications

Set `DAGFOX_NOTIFY_CHANNEL=slack` or `teams` and the matching `DAGFOX_SLACK_WEBHOOK_URL` / `DAGFOX_TEAMS_WEBHOOK_URL` (an env var or an Airflow Variable). Teams uses a Workflows "post to a channel when a webhook request is received" URL and receives an Adaptive Card. Notifications carry the summary and a **Review in Airflow** link. The approval itself always happens in Airflow. A failed webhook is logged and never blocks the pipeline. The default channel, `log`, writes notifications to the task log.

## Tests

```bash
pip install -e ".[dev]" && make test     # or: make test-docker (inside the running Airflow image)
```

The tests run on in-memory SQLite with the LLM mocked, so they make no network calls. They cover the policy engine overriding the LLM, invalid LLM output routing to HUMAN_LED, unapproved, mismatched or rejected writes to `published.*` raising errors, one audit row per agent and per human decision, and hash-chain tamper detection. Three more tests check the Postgres guard trigger directly; they run when `DAGFOX_TEST_PG_URL` is set:

```bash
DAGFOX_TEST_PG_URL=postgresql+psycopg2://dagfox:dagfox@localhost:5433/dagfox pytest tests/test_postgres_guard.py
```

## Repository layout

```
dagfox/
  agents/        triage.py, explain.py, remediate.py, models.py (Pydantic schemas), base.py (runner)
  llm/           adapter.py (the only provider-aware module), offline.py (no-key stand-in)
  policy/        policy.yaml, engine.py
  audit/         log.py (hash-chained writer + verify), schema.sql
  notify/        slack.py, teams.py
  checks/        entity_checks.py, reference.py (ISO 17442 / ISO 3166)
  publish/       writer.py (guarded writer), diff.py (apply + hash)
  sources/       landing.py, vendor_sim.py (demo-only fault injection)
  pipeline.py    Airflow-free pipeline steps (what the tests drive)
  airflow_support.py  approval operator + task glue
dags/            entities_daily.py
data/            load_gleif.py (sample loader)
tests/
```

## Data

The demo uses **GLEIF Level 1 LEI data** (legal entity reference data, [CC0 1.0](https://www.gleif.org/en/meta/lei-data-terms-of-use/)) as a stand-in for a company database. On first `make up`, `data/load_gleif.py` downloads about 3,000 records from `api.gleif.org` and caches them in `data/cache/`. If the download fails (offline, or a proxy blocks it), the loader logs a warning and generates **synthetic** GLEIF-shaped records with obviously fake LEIs (prefix `DFX0`). No proprietary vendor data is used or imitated.

The "vendor file" for each run comes from `sources/vendor_sim.py`, which is demo-only. It keeps its own state across runs, adds normal daily churn (some registrations lapse, each with a newer `last_update_date`), and injects the requested fault.

## Path to MWAA (not in the MVP)

The library is Airflow-agnostic apart from `airflow_support.py` and the DAG file, so the move is mostly configuration:

- Ship `dagfox/` as a wheel via `plugins.zip` or `requirements.txt`, and copy `dags/`.
- Store `dagfox_db` (RDS/Aurora Postgres) and the webhook URLs in Secrets Manager via MWAA's secrets backend.
- Use `bedrock` as the LLM provider, with the MWAA execution role.
- Replace `LandingZone` with an S3 prefix, and drop `vendor_sim`.
- Run `audit/schema.sql` and `entities_schema.sql` through your migration tool.

Note: MWAA must offer Airflow ≥ 3.1 for HITL operators (3.3 for the trigger-free `awaiting_input` state).

## Roadmap

- **Checks:** push down to SQL / dbt tests / Great Expectations for large tables; volume and distribution drift checks.
- **Evidence:** let the remediate agent look up the GLEIF golden copy (read-only tool) to source corrections, instead of only reverting to published values.
- **Approvals:** per-field or per-row partial approval; four-eyes (two approvers) for high-impact diffs; approval expiry.
- **Audit:** periodic hash-chain anchoring (for example, S3 Object Lock); an export for model-risk review; an eval set of labelled incidents to score triage accuracy per model.
- **Notifications:** threaded updates when a decision is made; PagerDuty for HUMAN_LED.
- **Packaging:** a reusable `DagFoxCheckOperator`, so any DAG can opt in; an MWAA deployment template.

## Verified against

- Apache Airflow **3.3.2** with `apache-airflow-providers-standard` 1.19 (`ApprovalOperator`, `assigned_users`, `response_timeout`, `awaiting_input` state, `/api/v2/.../hitlDetails`), Task SDK (`airflow.sdk`), Python 3.12.
- pydantic-ai **2.63** (`Agent(output_type=…)`, `FunctionModel`, `AnthropicModel`, `BedrockConverseModel`, `OpenAIChatModel` + `AzureProvider`).

## License

Apache 2.0. See [LICENSE](LICENSE).
