"""Deterministic data quality checks and the demo fault injector."""

from helpers import bad_batch, make_entities

from dagfox.checks.entity_checks import run_checks
from dagfox.checks.reference import lei_is_valid, normalize_country
from dagfox.sources.vendor_sim import daily_churn, inject_fault


def test_lei_checksum():
    assert lei_is_valid("5493001KJTIIGC8Y1R12")
    assert not lei_is_valid("5493001KJTIIGC8Y1R13")
    assert not lei_is_valid("short")


def test_country_normalization_is_conservative():
    assert normalize_country(" us ") == "US"
    assert normalize_country("Germany") is None


def test_clean_batch_passes():
    rows = make_entities(50)
    assert run_checks("b", rows, {r["lei"]: r for r in rows}).passed


def test_bad_batch_fails_the_expected_checks():
    published = make_entities(100)
    report = run_checks("b", bad_batch(published), {r["lei"]: r for r in published})
    failed = {r.name: r.affected_count for r in report.failed}
    assert failed == {"duplicate_lei": 2, "country_format": 1, "unsourced_change": 1}
    assert report.affected_pct < 5


def test_sourced_change_is_not_flagged():
    published = make_entities(10)
    rows = [dict(r) for r in published]
    rows[0] |= {"registration_status": "LAPSED", "last_update_date": "2026-01-01T00:00:00Z"}
    assert run_checks("b", rows, {r["lei"]: r for r in published}).passed


def test_vendor_sim_faults_map_to_bands_by_volume():
    sample = make_entities(3020)  # fault counts are absolute, sized for the ~3,000-record demo sample
    published = {r["lei"]: r for r in sample[:-20]}
    clean, updated = daily_churn(sample, run_key="r1")
    assert run_checks("none", inject_fault(clean, "none", updated), published).passed

    bad = run_checks("bad", inject_fault(clean, "bad_records", updated), published)
    assert 1 < bad.affected_pct < 5

    mass = run_checks("mass", inject_fault(clean, "mass_change", updated), published)
    assert mass.affected_pct > 5
