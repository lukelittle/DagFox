"""Slack incoming webhook (Block Kit)."""

from __future__ import annotations

from dagfox.notify import Notification, WebhookNotifier


class SlackNotifier(WebhookNotifier):
    def payload(self, note: Notification) -> dict:
        blocks: list[dict] = [
            {"type": "header", "text": {"type": "plain_text", "text": f"{note.prefix}: {note.title}"[:150]}},
            {"type": "section", "text": {"type": "mrkdwn", "text": note.text}},
        ]
        if note.facts:
            blocks.append({"type": "section", "fields": [
                {"type": "mrkdwn", "text": f"*{k}*\n{v}"} for k, v in note.facts.items()
            ]})
        if note.link:
            blocks.append({"type": "actions", "elements": [
                {"type": "button", "text": {"type": "plain_text", "text": "Review in Airflow"}, "url": note.link}
            ]})
        if note.run_id:
            blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": f"run `{note.run_id}`"}]})
        return {"text": f"{note.prefix}: {note.title}", "blocks": blocks}
