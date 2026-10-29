"""DEMO ONLY: simulates the upstream vendor's daily entity file, with injectable faults.

Faults are deterministic (fixed seed) so every demo run looks the same:

    none         clean file: yesterday's records plus a few new registrations
    late_file    clean file that arrives late                         -> AUTO
    bad_records  ~2% of records broken: duplicate LEIs, malformed
                 country codes, countries changed with no source      -> SUPERVISED
    mass_change  ~8% of records get a new legal form, no source       -> HUMAN_LED
"""

from __future__ import annotations

import random
import time

from dagfox.sources.landing import LandingZone

FAULTS = ("none", "late_file", "bad_records", "mass_change")
NEW_REGISTRATIONS = 20  # matches data/load_gleif.py: held back from the initial publish
SEED = 20260101


def build_daily_rows(sample: list[dict], fault: str) -> list[dict]:
    if fault not in FAULTS:
        raise ValueError(f"unknown fault {fault!r}; choose from {FAULTS}")
    rows = [dict(r) for r in sample]
    rng = random.Random(SEED)
    existing = rows[:-NEW_REGISTRATIONS]  # records already published
    by_country: dict[str, int] = {}
    for r in existing:
        by_country[r["legal_address_country"]] = by_country.get(r["legal_address_country"], 0) + 1
    common_countries = sorted(by_country, key=by_country.get, reverse=True)[:5]

    if fault == "bad_records":
        picked = rng.sample(existing, 12 + 15 + 30)
        dupes, malformed, flipped = picked[:12], picked[12:27], picked[27:]
        rows.extend(dict(r) for r in dupes)
        for i, r in enumerate(malformed):
            code = r["legal_address_country"]
            r["legal_address_country"] = [code.lower(), f" {code}", f"{code} "][i % 3]
        for r in flipped:
            r["legal_address_country"] = next(c for c in common_countries if c != r["legal_address_country"])

    elif fault == "mass_change":
        for r in rng.sample(existing, int(len(existing) * 0.08)):
            r["legal_form_code"] = "8888" if r["legal_form_code"] != "8888" else "9999"

    return rows


def drop_daily_file(
    landing: LandingZone, name: str, sample: list[dict], fault: str, late_by_seconds: int
) -> dict:
    rows = build_daily_rows(sample, fault)
    deliver_at = time.time() + late_by_seconds if fault == "late_file" else None
    path = landing.deliver(name, rows, deliver_at=deliver_at)
    return {"path": str(path), "rows": len(rows), "fault": fault, "deliver_at": deliver_at}
