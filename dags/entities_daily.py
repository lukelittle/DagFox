"""entities_daily: land the vendor's entity file, check it, publish it."""

from __future__ import annotations

import re
from datetime import datetime, timedelta

from airflow.sdk import Param, PokeReturnValue, dag, get_current_context, task

from dagfox.config import Settings, resolve_db_url
from dagfox.db import get_engine
from dagfox.sources.landing import LandingZone
from dagfox.sources.vendor_sim import FAULTS


def _batch_id() -> str:
    run_id = get_current_context()["run_id"]
    return re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)


def _file_name(batch_id: str) -> str:
    return f"entities_{batch_id}.csv"


@dag(
    dag_id="entities_daily",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dagfox", "entities"],
    params={"fault": Param("none", enum=list(FAULTS), description="Demo fault to inject")},
)
def entities_daily():
    @task
    def simulate_vendor_drop(params=None) -> dict:
        """DEMO ONLY: write today's vendor file (with the requested fault) to the landing zone."""
        from dagfox.sources.vendor_sim import drop_daily_file

        settings = Settings.from_env()
        return drop_daily_file(
            LandingZone(settings.landing_dir),
            _file_name(_batch_id()),
            LandingZone.read(settings.sample_path),
            params["fault"],
            settings.late_file_delay_seconds,
        )

    @task.sensor(poke_interval=15, timeout=timedelta(minutes=30), mode="reschedule")
    def wait_for_file() -> PokeReturnValue:
        path = LandingZone(Settings.from_env().landing_dir).poll(_file_name(_batch_id()))
        return PokeReturnValue(is_done=path is not None, xcom_value=str(path) if path else None)

    @task
    def load_staging(path: str) -> str:
        from pathlib import Path

        from dagfox.pipeline import load_staging as load

        batch_id = _batch_id()
        load(get_engine(resolve_db_url()), batch_id, Path(path))
        return batch_id

    @task
    def run_checks(batch_id: str) -> dict:
        from dagfox.pipeline import check_batch

        report = check_batch(get_engine(resolve_db_url()), batch_id)
        for line in report.summary_lines():
            print(line)
        if not report.passed:
            raise RuntimeError(f"{len(report.failed)} checks failed on {batch_id}")
        return report.model_dump(mode="json")

    drop = simulate_vendor_drop()
    path = wait_for_file()
    drop >> path
    run_checks(load_staging(path))


entities_daily()
