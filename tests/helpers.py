"""Plain helpers shared by tests: entity rows, batches, mocked LLMs, a recording notifier."""

from __future__ import annotations

import random
from collections.abc import Callable

from pydantic_ai.messages import ModelResponse, ToolCallPart
from pydantic_ai.models.function import FunctionModel
from sqlalchemy import text

from dagfox.checks.reference import lei_check_digits
from dagfox.db import ENTITY_FIELDS
from dagfox.llm.adapter import ModelHandle
from dagfox.notify import Notification


def make_entities(n: int, seed: int = 7) -> list[dict]:
    rng = random.Random(seed)
    rows = []
    for i in range(n):
        base = "TEST00" + "".join(rng.choice("ABCDEFGHJKLMNPQRSTUVWXYZ0123456789") for _ in range(12))
        country = rng.choice(["US", "GB", "DE", "FR", "NL"])
        rows.append({
            "lei": base + lei_check_digits(base), "legal_name": f"Test Entity {i} Ltd",
            "legal_form_code": "H1UM", "legal_address_city": "Testville",
            "legal_address_country": country, "hq_address_country": country,
            "entity_status": "ACTIVE", "registration_status": "ISSUED",
            "initial_registration_date": "2015-01-01T00:00:00Z", "last_update_date": "2025-01-01T00:00:00Z",
            "managing_lou": "TESTLOU",
        })
    return rows


def insert_batch(engine, batch_id: str, rows: list[dict]) -> None:
    cols = ", ".join(ENTITY_FIELDS)
    vals = ", ".join(f":{f}" for f in ENTITY_FIELDS)
    with engine.begin() as conn:
        conn.execute(text(f"INSERT INTO staging.entities ({cols}, batch_id) VALUES ({vals}, :batch_id)"),
                     [r | {"batch_id": batch_id} for r in rows])


def bad_batch(published_rows: list[dict]) -> list[dict]:
    """2 duplicates, 1 malformed country, 1 unsourced country change: 4 of 100 records (SUPERVISED)."""
    rows = [dict(r) for r in published_rows]
    rows.append(dict(rows[0]))
    rows.append(dict(rows[1]))
    rows[2]["legal_address_country"] = rows[2]["legal_address_country"].lower()
    rows[3]["legal_address_country"] = "FR" if rows[3]["legal_address_country"] != "FR" else "DE"
    return rows


def scripted_model(output: dict | Callable[[], dict], name: str = "mock-llm") -> ModelHandle:
    """A mocked LLM: always 'answers' with ``output`` as its structured result."""
    def fn(messages, info):
        args = output() if callable(output) else output
        return ModelResponse(parts=[ToolCallPart(tool_name=info.output_tools[0].name, args=args)])
    return ModelHandle(FunctionModel(fn, model_name=name), "mock", name)


def failing_model() -> ModelHandle:
    def fn(messages, info):
        raise RuntimeError("provider unavailable")
    return ModelHandle(FunctionModel(fn, model_name="broken-llm"), "mock", "broken-llm")


class RecordingNotifier:
    def __init__(self):
        self.sent: list[Notification] = []

    def send(self, note: Notification) -> None:
        self.sent.append(note)
