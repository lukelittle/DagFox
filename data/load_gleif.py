"""Load a GLEIF Level 1 LEI sample into staging.entities and published.entities.

GLEIF LEI data is published under CC0 1.0 (https://www.gleif.org/en/meta/lei-data-terms-of-use).

    python data/load_gleif.py fetch --n 3000      # download a sample from api.gleif.org
    python data/load_gleif.py synthetic --n 3000  # offline fallback, clearly fake LEIs
    python data/load_gleif.py seed                # create schemas, load published + staging
    python data/load_gleif.py ensure              # fetch (or fall back) if no cache, then seed

The last ``NEW_REGISTRATIONS`` records of the sample are held back from the
initial published load, so the first daily file contains genuinely new entities.
"""

from __future__ import annotations

import argparse
import csv
import logging
import random
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx
from sqlalchemy import text

from dagfox.checks.reference import lei_check_digits
from dagfox.config import Settings, resolve_db_url
from dagfox.db import ENTITY_FIELDS, apply_schema, get_engine
from dagfox.publish.diff import subject_hash
from dagfox.publish.writer import PublishedWriter, record_approval

log = logging.getLogger("dagfox.loader")

GLEIF_API = "https://api.gleif.org/api/v1/lei-records"
PAGE_SIZE = 200
NEW_REGISTRATIONS = 20
BOOTSTRAP_BATCH = "bootstrap"


def to_iso_utc(value: str | None) -> str | None:
    if not value:
        return None
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def flatten(record: dict) -> dict:
    """Map one GLEIF API lei-record onto our column set."""
    attrs = record.get("attributes", {})
    entity = attrs.get("entity", {})
    reg = attrs.get("registration", {})
    legal_addr = entity.get("legalAddress") or {}
    hq_addr = entity.get("headquartersAddress") or {}
    return {
        "lei": attrs.get("lei") or record.get("id"),
        "legal_name": (entity.get("legalName") or {}).get("name"),
        "legal_form_code": (entity.get("legalForm") or {}).get("id"),
        "legal_address_city": legal_addr.get("city"),
        "legal_address_country": legal_addr.get("country"),
        "hq_address_country": hq_addr.get("country"),
        "entity_status": entity.get("status"),
        "registration_status": reg.get("status"),
        "initial_registration_date": to_iso_utc(reg.get("initialRegistrationDate")),
        "last_update_date": to_iso_utc(reg.get("lastUpdateDate")),
        "managing_lou": reg.get("managingLou"),
    }


def fetch(n: int) -> list[dict]:
    rows: list[dict] = []
    with httpx.Client(timeout=30, headers={"Accept": "application/vnd.api+json"}) as client:
        page = 1
        while len(rows) < n:
            resp = client.get(GLEIF_API, params={"page[size]": PAGE_SIZE, "page[number]": page})
            resp.raise_for_status()
            data = resp.json().get("data", [])
            if not data:
                break
            rows.extend(flatten(r) for r in data)
            log.info("fetched page %d (%d records)", page, len(rows))
            page += 1
            time.sleep(0.2)  # be polite to the public API
    return rows[:n]


def synthetic(n: int, seed: int = 17442) -> list[dict]:
    """GLEIF-shaped records with obviously fake LEIs (prefix 'DFX0'). Offline fallback only."""
    rng = random.Random(seed)
    countries = ["US", "GB", "DE", "FR", "NL", "LU", "IE", "JP", "CA", "CH", "SG", "KY", "AU", "ES", "IT"]
    forms = ["8888", "H1UM", "XTIQ", "T0YJ", "2HBR", "6QQB", "ZQO8", "V44D"]
    suffixes = ["Holdings Ltd", "Capital LLC", "Fund LP", "Bank AG", "Partners SA", "Trust", "GmbH"]
    rows = []
    for i in range(n):
        base = "DFX000" + "".join(rng.choice("ABCDEFGHJKLMNPQRSTUVWXYZ0123456789") for _ in range(12))
        country = rng.choice(countries)
        registered = datetime(2012 + rng.randrange(12), rng.randrange(1, 13), rng.randrange(1, 28), tzinfo=UTC)
        updated = datetime(2025, rng.randrange(1, 13), rng.randrange(1, 28), tzinfo=UTC)
        rows.append(
            {
                "lei": base + lei_check_digits(base),
                "legal_name": f"Synthetic Entity {i:05d} {rng.choice(suffixes)}",
                "legal_form_code": rng.choice(forms),
                "legal_address_city": f"City {rng.randrange(200):03d}",
                "legal_address_country": country,
                "hq_address_country": country if rng.random() > 0.15 else rng.choice(countries),
                "entity_status": "ACTIVE" if rng.random() > 0.05 else "INACTIVE",
                "registration_status": rng.choice(["ISSUED"] * 8 + ["LAPSED", "RETIRED"]),
                "initial_registration_date": registered.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "last_update_date": updated.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "managing_lou": "DFX0SYNTHETICLOU000",
            }
        )
    return rows


def write_csv(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=ENTITY_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    log.info("wrote %d records to %s", len(rows), path)


def read_csv(path: Path) -> list[dict]:
    with path.open(newline="") as f:
        return [{k: (v if v != "" else None) for k, v in row.items()} for row in csv.DictReader(f)]


def seed(sample_path: Path) -> None:
    """Create schemas and publish the initial snapshot through the guarded writer."""
    engine = get_engine(resolve_db_url())
    apply_schema(engine)

    with engine.connect() as conn:
        if conn.execute(text("SELECT count(*) FROM published.entities")).scalar():
            log.info("published.entities already populated; skipping seed")
            return

    rows = read_csv(sample_path)[:-NEW_REGISTRATIONS]
    cols = ", ".join(ENTITY_FIELDS)
    placeholders = ", ".join(f":{f}" for f in ENTITY_FIELDS)
    with engine.begin() as conn:
        conn.execute(
            text(f"INSERT INTO staging.entities ({cols}, batch_id) VALUES ({placeholders}, :batch_id)"),
            [r | {"batch_id": BOOTSTRAP_BATCH} for r in rows],
        )

    # Even the initial load goes through an approval record, so the audit trail is complete.
    approval_id = record_approval(
        engine,
        kind="bootstrap",
        subject_sha256=subject_hash(rows, []),
        decision="approved",
        approver="system:seed",
        comment=f"Initial load of GLEIF sample {sample_path.name}",
    )
    result = PublishedWriter(engine).publish_batch(BOOTSTRAP_BATCH, [], approval_id)
    log.info("seeded published.entities: %s", result)


def ensure(sample_path: Path, n: int) -> None:
    if not sample_path.exists():
        try:
            write_csv(fetch(n), sample_path)
        except Exception as exc:  # network blocked, API down, ...
            log.warning("GLEIF fetch failed (%s); falling back to SYNTHETIC sample data", exc)
            write_csv(synthetic(n), sample_path)
    seed(sample_path)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["fetch", "synthetic", "seed", "ensure"])
    parser.add_argument("--n", type=int, default=3000)
    parser.add_argument("--sample", type=Path, default=Settings.from_env().sample_path)
    args = parser.parse_args(argv)

    if args.command == "fetch":
        write_csv(fetch(args.n), args.sample)
    elif args.command == "synthetic":
        write_csv(synthetic(args.n), args.sample)
    elif args.command == "seed":
        seed(args.sample)
    else:
        ensure(args.sample, args.n)
    return 0


if __name__ == "__main__":
    sys.exit(main())
