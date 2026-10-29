"""Webhook payload shapes (no network: payloads are built, never posted)."""

from dagfox.notify import LogNotifier, Notification, build_notifier
from dagfox.notify.slack import SlackNotifier
from dagfox.notify.teams import TeamsNotifier

NOTE = Notification(title="57 records need attention", text="Summary.", band="SUPERVISED",
                    run_id="r1", link="http://localhost:8080/x", facts={"Proposed changes": "57"})


def test_slack_payload_has_review_button():
    payload = SlackNotifier("https://hooks.slack.invalid/x").payload(NOTE)
    assert "SUPERVISED" in payload["text"]
    assert any(b["type"] == "actions" for b in payload["blocks"])


def test_teams_payload_is_an_adaptive_card():
    payload = TeamsNotifier("https://teams.invalid/x").payload(NOTE)
    card = payload["attachments"][0]["content"]
    assert card["type"] == "AdaptiveCard"
    assert card["actions"][0]["url"] == NOTE.link


def test_missing_webhook_falls_back_to_log(monkeypatch):
    monkeypatch.delenv("DAGFOX_SLACK_WEBHOOK_URL", raising=False)
    assert isinstance(build_notifier("slack"), LogNotifier)
