"""Notifications to Slack or Microsoft Teams incoming webhooks (config chooses).

    DAGFOX_NOTIFY_CHANNEL = log | slack | teams
    DAGFOX_SLACK_WEBHOOK_URL / DAGFOX_TEAMS_WEBHOOK_URL  (env var or Airflow Variable)

Notifications are informational. Approvals always happen in the Airflow UI.
A failed notification is logged, never fatal: it must not block the pipeline.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Protocol

log = logging.getLogger("dagfox.notify")

BAND_EMOJI = {"AUTO": "🟢", "SUPERVISED": "🟡", "HUMAN_LED": "🔴"}


@dataclass
class Notification:
    title: str
    text: str
    band: str | None = None
    run_id: str | None = None
    link: str | None = None
    facts: dict[str, str] = field(default_factory=dict)

    @property
    def prefix(self) -> str:
        return f"{BAND_EMOJI.get(str(self.band), 'ℹ️')} DagFox" + (f" [{self.band}]" if self.band else "")


class Notifier(Protocol):
    def send(self, note: Notification) -> None: ...


class LogNotifier:
    def send(self, note: Notification) -> None:
        log.info("%s %s: %s %s", note.prefix, note.title, note.text, note.link or "")


class WebhookNotifier:
    """Base for webhook notifiers: POST a JSON payload, log (don't raise) on failure."""

    def __init__(self, url: str, timeout: float = 10.0):
        self.url = url
        self.timeout = timeout

    def payload(self, note: Notification) -> dict:
        raise NotImplementedError

    def send(self, note: Notification) -> None:
        import httpx

        try:
            httpx.post(self.url, json=self.payload(note), timeout=self.timeout).raise_for_status()
        except Exception as exc:  # noqa: BLE001
            log.warning("notification failed (%s): %s", type(self).__name__, exc)


def build_notifier(channel: str) -> Notifier:
    from dagfox.config import secret

    if channel == "slack" and (url := secret("DAGFOX_SLACK_WEBHOOK_URL")):
        from dagfox.notify.slack import SlackNotifier

        return SlackNotifier(url)
    if channel == "teams" and (url := secret("DAGFOX_TEAMS_WEBHOOK_URL")):
        from dagfox.notify.teams import TeamsNotifier

        return TeamsNotifier(url)
    if channel not in {"log", "slack", "teams"}:
        log.warning("unknown DAGFOX_NOTIFY_CHANNEL %r; logging instead", channel)
    elif channel != "log":
        log.warning("no webhook URL configured for %s; logging instead", channel)
    return LogNotifier()
