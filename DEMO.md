# DagFox: two-minute demo

**Before you present:** run `make reset && make up` (takes about 2 minutes the first time). Have two windows open:
a terminal in the repo, and a browser at <http://localhost:8080> logged in as **`data_steward`** (password `dagfox`).
Open DAG `entities_daily` in Grid view.

Run 2's file arrives about 60 seconds late by design. Start it while you talk over run 1.

---

### 0:00 · Set the scene (15s)

> "This is a daily load of legal-entity reference data: about 3,000 GLEIF LEI records into a published table that downstream systems trust. Airflow already tells us when a *pipeline* breaks, and AWS DevOps Agent can diagnose that. DagFox covers the other question: the pipeline ran, but is the *data* right, and who fixes it?"

### 0:15 · Run 1: clean (15s)

```bash
make run FAULT=none
```

> "Seven checks run: identifier format and checksum, duplicates, country codes, legal-form codes, changes without a source update, and volume. Everything passes, so the batch publishes. No AI call and no human needed. The policy still records an approval for the write."

### 0:30 · Run 2: late file, AUTO (20s)

```bash
make run FAULT=late_file
```

> "Today the vendor is late. That's the AUTO band. DagFox waits and retries on its own, and it logs every retry. No one gets paged."

Point at `wait_for_file` going up for reschedule, then to green, about 60 seconds later. Keep talking over it.

### 0:50 · Run 3: bad records, SUPERVISED (45s)

```bash
make run FAULT=bad_records
```

When `approve_fix` turns **Awaiting Input**, click it and open the **Required Action** tab.

> "Fifty-seven records are wrong: twelve duplicate LEIs, fifteen country codes like `' US'` and `'it'`, and thirty records whose country changed overnight with no update date from the source. The triage agent classified this as SUPERVISED, and the policy file agreed. The policy has the final say, and it can only make things stricter."
>
> "Here's the plain-English summary for the analyst. Below it is the proposed fix: a row-level diff with the old value, the new value and the source for every change. It's a dry run, and nothing has been written. My approval covers the hash of exactly this batch and this diff. If anything changes after I approve, the publish refuses."
>
> "Only `data_steward` can answer this. The ops analyst can see it but can't approve it."

Type a comment ("Spot-checked against GLEIF") and click **Approve**. `publish_fix` turns green.

> "Now the corrected batch is published, including today's legitimate updates that the bad records were holding back."

### 1:35 · Audit (20s)

```bash
make audit
```

> "Every step is a row: each agent call with its model and confidence, the policy decision, the proposed fix, and the human decision with *who* approved it. The audit table is append-only and hash-chained."

```bash
make audit-verify
```

### 1:55 · Close (5s)

> "Airflow tells you something failed. DagFox tells the right person why and what to do, and nothing touches published data without sign-off."

---

### If you have another 30 seconds: HUMAN_LED

```bash
make run FAULT=mass_change
```

> "Eight percent of records changed legal form overnight with no source. The (offline) triage model says SUPERVISED. The policy overrides it to HUMAN_LED because more than 5% of records are affected. DagFox writes a summary and notifies people, and it proposes no write at all."

`make audit` shows `policy_decision … HUMAN_LED … override` followed by `escalation … escalated_no_write`.

### If someone asks "what stops it writing anyway?"

```bash
docker compose exec postgres psql -U dagfox -d dagfox \
  -c "UPDATE published.entities SET legal_name = 'x' WHERE lei = (SELECT min(lei) FROM published.entities);"
# ERROR:  DagFox guard: UPDATE on published.entities without an approval
```
