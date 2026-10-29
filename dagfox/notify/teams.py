"""Microsoft Teams incoming webhook (Workflows "post to a channel when a webhook request
is received"), sending an Adaptive Card."""

from __future__ import annotations

from dagfox.notify import Notification, WebhookNotifier


class TeamsNotifier(WebhookNotifier):
    def payload(self, note: Notification) -> dict:
        body: list[dict] = [
            {"type": "TextBlock", "size": "Medium", "weight": "Bolder", "wrap": True,
             "text": f"{note.prefix}: {note.title}"},
            {"type": "TextBlock", "wrap": True, "text": note.text},
        ]
        if note.facts:
            body.append({"type": "FactSet", "facts": [{"title": k, "value": v} for k, v in note.facts.items()]})
        if note.run_id:
            body.append({"type": "TextBlock", "isSubtle": True, "size": "Small", "text": f"run {note.run_id}"})
        card = {
            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
            "type": "AdaptiveCard",
            "version": "1.4",
            "body": body,
        }
        if note.link:
            card["actions"] = [{"type": "Action.OpenUrl", "title": "Review in Airflow", "url": note.link}]
        return {
            "type": "message",
            "attachments": [{"contentType": "application/vnd.microsoft.card.adaptive", "content": card}],
        }
