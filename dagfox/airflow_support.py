"""Airflow-specific glue: building the pipeline inside a task and the approval operator.

Everything else in the dagfox package is Airflow-agnostic.
"""

from __future__ import annotations

from typing import Any

from airflow.providers.standard.operators.hitl import ApprovalOperator
from airflow.sdk import Param

from dagfox.config import Settings, resolve_db_url
from dagfox.db import get_engine
from dagfox.llm.adapter import build_model
from dagfox.notify import build_notifier
from dagfox.pipeline import EntityPipeline, RunContext
from dagfox.policy.engine import PolicyEngine


def build_pipeline() -> EntityPipeline:
    settings = Settings.from_env()
    return EntityPipeline(
        engine=get_engine(resolve_db_url()),
        policy=PolicyEngine.from_file(settings.policy_path),
        model_factory=lambda agent: build_model(settings.llm_provider, settings.llm_model, agent),
        notifier=build_notifier(settings.notify_channel),
    )


def run_context(context: dict) -> RunContext:
    return RunContext(
        dag_id=context["dag"].dag_id,
        dag_run_id=context["run_id"],
        task_id=context["task"].task_id,
        base_url=Settings.from_env().airflow_base_url,
    )


class FixApprovalOperator(ApprovalOperator):
    """Airflow HITL approval that records the human decision in DagFox's approval table and audit log.

    The approval row binds the approver to the SHA-256 of the exact batch + diff
    they saw; the publish step re-verifies that hash before writing.
    """

    def __init__(self, *, proposal_task_id: str, **kwargs):
        kwargs.setdefault("params", {
            "comment": Param("", type="string", title="Comment (optional)", source="task",
                             description="Recorded in the DagFox audit log with your decision."),
        })
        super().__init__(**kwargs)
        self.proposal_task_id = proposal_task_id

    def _proposal(self, context) -> dict:
        return context["ti"].xcom_pull(task_ids=self.proposal_task_id, key="proposal")

    def execute_complete(self, context, event: dict[str, Any]) -> Any:
        if event.get("error_type") == "timeout":  # no response in time: record it, then fail as usual
            build_pipeline().record_human_decision(
                run_context(context), self._proposal(context), "timed_out", "system:timeout",
                "No response within the approval window",
            )
        result = super().execute_complete(context=context, event=event)  # skips downstream on Reject
        user = result.get("responded_by_user") or {}
        approver = user.get("name") or user.get("id") or "unknown"
        comment = (result.get("params_input") or {}).get("comment") or None
        approval_id = build_pipeline().record_human_decision(
            run_context(context), self._proposal(context), result["chosen_options"][0], approver, comment,
        )
        self.log.info("Recorded %s by %s as approval %s", result["chosen_options"][0], approver, approval_id)
        return {"chosen": result["chosen_options"][0], "approver": approver, "approval_id": approval_id}
