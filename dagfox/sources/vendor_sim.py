"""DEMO ONLY: simulates the upstream vendor's daily entity file, with injectable faults.

Faults are deterministic (fixed seed) so every demo run looks the same:

Every file also carries normal daily churn: a few new registrations and
SOURCED_UPDATES records whose registration lapsed (with a newer last_update_date).

    none         clean file
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
SOURCED_UPDATES = 25
SEED = 20260101


def daily_churn(state: list[dict], run_key: str, as_of: str | None = None) -> tuple[list[dict], set[str]]:
    """Legitimate, sourced changes: some registrations lapse, with a newer last_update_date.
    Returns the new rows and the LEIs that changed."""
    rows = [dict(r) for r in state]
    as_of = as_of or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    issued = [r for r in rows[:-NEW_REGISTRATIONS] if r["registration_status"] == "ISSUED"]
    changed = random.Random(f"updates:{run_key}").sample(issued, min(SOURCED_UPDATES, len(issued)))
    for r in changed:
        r["registration_status"], r["last_update_date"] = "LAPSED", as_of
    return rows, {r["lei"] for r in changed}


def inject_fault(clean: list[dict], fault: str, protected: set[str] = frozenset()) -> list[dict]:
    """Apply a demo fault. Row selection uses a fixed seed, so every demo run looks the same."""
    if fault not in FAULTS:
        raise ValueError(f"unknown fault {fault!r}; choose from {FAULTS}")
    rows = [dict(r) for r in clean]
    rng = random.Random(SEED)
    # Already-published records not legitimately updated today (a newer source date would mask the fault).
    existing = [r for r in rows[:-NEW_REGISTRATIONS] if r["lei"] not in protected]
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
    """The vendor keeps its own state across days (initialised from the GLEIF sample);
    faults only corrupt the file sent, never the vendor's state."""
    state_path = landing.root / ".vendor_state.csv"
    state = LandingZone.read(state_path) if state_path.exists() else sample
    clean, updated = daily_churn(state, run_key=name)
    landing.deliver(state_path.name, clean)
    rows = inject_fault(clean, fault, protected=updated)
    deliver_at = time.time() + late_by_seconds if fault == "late_file" else None
    path = landing.deliver(name, rows, deliver_at=deliver_at)
    return {"path": str(path), "rows": len(rows), "fault": fault, "deliver_at": deliver_at}
