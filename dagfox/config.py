"""Runtime settings.

Everything comes from the environment (or an Airflow Connection for the
database), never from code. Keeping this module free of Airflow imports means
the same library runs in docker compose, in unit tests and, later, on MWAA.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parent
DEFAULT_POLICY_PATH = PACKAGE_DIR / "policy" / "policy.yaml"


@dataclass(frozen=True)
class Settings:
    policy_path: Path
    llm_provider: str
    llm_model: str | None
    notify_channel: str
    landing_dir: Path
    sample_path: Path
    late_file_delay_seconds: int
    airflow_base_url: str

    @classmethod
    def from_env(cls) -> Settings:
        env = os.environ
        return cls(
            policy_path=Path(env.get("DAGFOX_POLICY_PATH", DEFAULT_POLICY_PATH)),
            llm_provider=env.get("DAGFOX_LLM_PROVIDER", "offline").lower(),
            llm_model=env.get("DAGFOX_LLM_MODEL") or None,
            notify_channel=env.get("DAGFOX_NOTIFY_CHANNEL", "log").lower(),
            landing_dir=Path(env.get("DAGFOX_LANDING_DIR", "/opt/dagfox/landing")),
            sample_path=Path(env.get("DAGFOX_SAMPLE_PATH", "/opt/dagfox/data/cache/gleif_sample.csv")),
            late_file_delay_seconds=int(env.get("DAGFOX_LATE_FILE_DELAY_SECONDS", "60")),
            airflow_base_url=env.get("DAGFOX_AIRFLOW_BASE_URL", "http://localhost:8080"),
        )


def resolve_db_url() -> str:
    """Database URL for the DagFox data/audit database.

    Order: ``DAGFOX_DB_URL`` env var, then the Airflow Connection ``dagfox_db``
    (which on MWAA would be backed by Secrets Manager).
    """
    if url := os.environ.get("DAGFOX_DB_URL"):
        return url
    from airflow.sdk import Connection  # imported lazily: optional at runtime

    uri = Connection.get("dagfox_db").get_uri()
    # Airflow renders postgres connections as postgres://; SQLAlchemy wants postgresql://
    return uri.replace("postgres://", "postgresql://", 1)


def secret(name: str) -> str | None:
    """Look up a secret by env var, falling back to an Airflow Variable of the same name."""
    if value := os.environ.get(name):
        return value
    try:
        from airflow.sdk import Variable

        return Variable.get(name, default=None)
    except Exception:  # Airflow not available (tests, CLI)
        return None
